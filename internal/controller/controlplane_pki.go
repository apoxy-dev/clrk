package controller

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/apoxy-dev/clrk/internal/egpki"
)

// Control-plane PKI Secrets + label. All live in the controller-manager's
// runtime namespace (POD_NAMESPACE). The root issues the :9443 server cert and
// every per-EG client cert; the CA bundle is the mount-only trust anchor the
// data-plane Envoys reference to verify the server cert.
const (
	ControlPlaneRootSecretName   = "clrk-control-plane-root"
	ControlPlaneServerSecretName = "clrk-control-plane-server"
	ControlPlaneCASecretName     = "clrk-control-plane-ca"
	ControlPlaneSecretLabel      = "clrk.apoxy.dev/control-plane-pki"
	// controlPlaneCABundleKey is the data key under which the CA bundle
	// Secret stores the root cert. The data-plane Envoy mounts this Secret and
	// the file lands at /etc/clrk/ca/ca.crt, which egextension's SslCredentials
	// pins as the server-CA path.
	controlPlaneCABundleKey = "ca.crt"
)

// EnsureControlPlaneRoot get-or-creates the control-plane root CA Secret in ns
// and returns its cert + key PEM. Reads go through r (an uncached reader --
// callers run this at startup, before the manager cache is started); writes go
// through w. A create race (another replica or a restart) is resolved by
// re-reading the winner's Secret so every replica trusts a single shared root.
func EnsureControlPlaneRoot(ctx context.Context, r client.Reader, w client.Client, ns string) (rootCertPEM, rootKeyPEM []byte, err error) {
	key := types.NamespacedName{Namespace: ns, Name: ControlPlaneRootSecretName}
	var existing corev1.Secret
	if err := r.Get(ctx, key, &existing); err == nil {
		return existing.Data[corev1.TLSCertKey], existing.Data[corev1.TLSPrivateKeyKey], nil
	} else if !apierrors.IsNotFound(err) {
		return nil, nil, fmt.Errorf("reading control-plane root: %w", err)
	}

	certPEM, keyPEM, err := egpki.MintRootCA("clrk control-plane root")
	if err != nil {
		return nil, nil, fmt.Errorf("minting control-plane root: %w", err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ControlPlaneRootSecretName,
			Namespace: ns,
			Labels:    map[string]string{ControlPlaneSecretLabel: "true"},
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{corev1.TLSCertKey: certPEM, corev1.TLSPrivateKeyKey: keyPEM},
	}
	if err := w.Create(ctx, secret); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, nil, fmt.Errorf("creating control-plane root: %w", err)
		}
		if err := r.Get(ctx, key, &existing); err != nil {
			return nil, nil, fmt.Errorf("re-reading control-plane root after create race: %w", err)
		}
		return existing.Data[corev1.TLSCertKey], existing.Data[corev1.TLSPrivateKeyKey], nil
	}
	return certPEM, keyPEM, nil
}

// EnsureControlPlaneServerCert get-or-creates the :9443 server-cert Secret in
// ns, signed by the given root, and returns the server cert/key PEM plus the CA
// bundle (the root cert). Desired SANs are derived from advertiseURI's host
// plus loopback names; an existing cert whose SANs no longer cover the desired
// set (advertiseURI changed across upgrades) is re-minted in place.
func EnsureControlPlaneServerCert(ctx context.Context, r client.Reader, w client.Client, ns, advertiseURI string, rootCertPEM, rootKeyPEM []byte) (serverCertPEM, serverKeyPEM, caBundlePEM []byte, err error) {
	dnsNames, ips := controlPlaneServerSANs(advertiseURI)
	key := types.NamespacedName{Namespace: ns, Name: ControlPlaneServerSecretName}

	var existing corev1.Secret
	if err := r.Get(ctx, key, &existing); err == nil {
		// Reuse only if the cert covers the desired SANs AND still chains to
		// the current root. Checking SANs alone misses root rotation: the CA
		// bundle would advance to the new root while the server kept serving an
		// old-root-signed cert, breaking every data-plane mTLS handshake.
		if certCoversSANs(existing.Data[corev1.TLSCertKey], dnsNames, ips) &&
			certChainsTo(existing.Data[corev1.TLSCertKey], rootCertPEM) {
			return existing.Data[corev1.TLSCertKey], existing.Data[corev1.TLSPrivateKeyKey], rootCertPEM, nil
		}
		certPEM, keyPEM, mintErr := egpki.MintServerCert(rootCertPEM, rootKeyPEM, dnsNames, ips)
		if mintErr != nil {
			return nil, nil, nil, fmt.Errorf("re-minting control-plane server cert: %w", mintErr)
		}
		if existing.Data == nil {
			existing.Data = map[string][]byte{}
		}
		existing.Data[corev1.TLSCertKey] = certPEM
		existing.Data[corev1.TLSPrivateKeyKey] = keyPEM
		if err := w.Update(ctx, &existing); err != nil {
			// A sibling replica re-minted first (both observe the same SAN
			// drift / root rotation). This runs at startup where the caller
			// os.Exit(1)s on error, so don't crash-loop on a benign conflict:
			// re-read and adopt the winner's cert, which chains to the same
			// shared root.
			if apierrors.IsConflict(err) {
				if getErr := r.Get(ctx, key, &existing); getErr != nil {
					return nil, nil, nil, fmt.Errorf("re-reading control-plane server cert after update conflict: %w", getErr)
				}
				return existing.Data[corev1.TLSCertKey], existing.Data[corev1.TLSPrivateKeyKey], rootCertPEM, nil
			}
			return nil, nil, nil, fmt.Errorf("updating control-plane server cert: %w", err)
		}
		return certPEM, keyPEM, rootCertPEM, nil
	} else if !apierrors.IsNotFound(err) {
		return nil, nil, nil, fmt.Errorf("reading control-plane server cert: %w", err)
	}

	certPEM, keyPEM, err := egpki.MintServerCert(rootCertPEM, rootKeyPEM, dnsNames, ips)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("minting control-plane server cert: %w", err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ControlPlaneServerSecretName,
			Namespace: ns,
			Labels:    map[string]string{ControlPlaneSecretLabel: "true"},
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{corev1.TLSCertKey: certPEM, corev1.TLSPrivateKeyKey: keyPEM},
	}
	if err := w.Create(ctx, secret); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, nil, nil, fmt.Errorf("creating control-plane server cert: %w", err)
		}
		if err := r.Get(ctx, key, &existing); err != nil {
			return nil, nil, nil, fmt.Errorf("re-reading control-plane server cert after create race: %w", err)
		}
		return existing.Data[corev1.TLSCertKey], existing.Data[corev1.TLSPrivateKeyKey], rootCertPEM, nil
	}
	return certPEM, keyPEM, rootCertPEM, nil
}

// EnsureControlPlaneCABundle get-or-creates the shared CA-bundle Secret in ns
// that every data-plane Envoy mounts to verify the :9443 server cert. It holds
// only the root cert (no key) and is updated in place if the bundle changed
// (root rotation, rare).
func EnsureControlPlaneCABundle(ctx context.Context, r client.Reader, w client.Client, ns string, caBundlePEM []byte) error {
	key := types.NamespacedName{Namespace: ns, Name: ControlPlaneCASecretName}
	var existing corev1.Secret
	if err := r.Get(ctx, key, &existing); err == nil {
		if bytes.Equal(existing.Data[controlPlaneCABundleKey], caBundlePEM) {
			return nil
		}
		if existing.Data == nil {
			existing.Data = map[string][]byte{}
		}
		existing.Data[controlPlaneCABundleKey] = caBundlePEM
		if err := w.Update(ctx, &existing); err != nil {
			return fmt.Errorf("updating control-plane CA bundle: %w", err)
		}
		return nil
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("reading control-plane CA bundle: %w", err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ControlPlaneCASecretName,
			Namespace: ns,
			Labels:    map[string]string{ControlPlaneSecretLabel: "true"},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{controlPlaneCABundleKey: caBundlePEM},
	}
	if err := w.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("creating control-plane CA bundle: %w", err)
	}
	return nil
}

// controlPlaneServerSANs derives the server cert SAN set from the advertise
// URI host plus loopback names, so the data-plane Envoys can dial either the
// Service DNS or localhost and still verify the cert.
func controlPlaneServerSANs(advertiseURI string) (dnsNames []string, ips []net.IP) {
	host := advertiseURI
	if h, _, splitErr := net.SplitHostPort(advertiseURI); splitErr == nil {
		host = h
	}
	dnsNames = []string{"localhost"}
	ips = []net.IP{net.ParseIP("127.0.0.1")}
	if host == "" {
		return dnsNames, ips
	}
	if ip := net.ParseIP(host); ip != nil {
		ips = append(ips, ip)
		return dnsNames, ips
	}
	dnsNames = append([]string{host}, dnsNames...)
	// A ...svc short form is often dialed as its cluster-local FQDN too.
	if strings.HasSuffix(host, ".svc") {
		dnsNames = append(dnsNames, host+".cluster.local")
	}
	return dnsNames, ips
}

// certChainsTo reports whether the leaf cert in certPEM verifies against the
// single root in rootCertPEM. It is EKU- and hostname-agnostic (callers verify
// SANs separately) so the same check serves the server cert and the per-EG
// client certs. A cert that no longer chains -- because the root rotated or the
// leaf expired -- must be re-minted, otherwise the data-plane trusts a CA
// bundle the leaf no longer descends from and every mTLS handshake fails.
func certChainsTo(certPEM, rootCertPEM []byte) bool {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return false
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return false
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(rootCertPEM) {
		return false
	}
	_, err = cert.Verify(x509.VerifyOptions{
		Roots:     roots,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	})
	return err == nil
}

// certCoversSANs reports whether the leaf cert in certPEM already includes
// every desired DNS and IP SAN.
func certCoversSANs(certPEM []byte, dnsNames []string, ips []net.IP) bool {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return false
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return false
	}
	have := make(map[string]struct{}, len(cert.DNSNames))
	for _, d := range cert.DNSNames {
		have[d] = struct{}{}
	}
	for _, d := range dnsNames {
		if _, ok := have[d]; !ok {
			return false
		}
	}
	haveIP := make(map[string]struct{}, len(cert.IPAddresses))
	for _, ip := range cert.IPAddresses {
		haveIP[ip.String()] = struct{}{}
	}
	for _, ip := range ips {
		if _, ok := haveIP[ip.String()]; !ok {
			return false
		}
	}
	return true
}
