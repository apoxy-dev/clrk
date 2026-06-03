package install

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// servingCertSecretName is the kubernetes.io/tls Secret (tls.crt/tls.key)
	// the cm mounts at --cert-dir to serve the aggregated API. cert-manager
	// mints it from the Certificate; the self-signed path writes it directly.
	servingCertSecretName = "clrk-controller-manager-tls"
	// servingCertMountPath is where the serving Secret is projected; passed to
	// the cm as --cert-dir. The apiserver reads tls.crt/tls.key from here
	// (PairName "tls").
	servingCertMountPath  = "/etc/clrk/tls"
	servingCertVolumeName = "serving-cert"

	// caSecretName persists the installer-minted self-signed CA so re-runs and
	// upgrades reuse it (stable APIService caBundle) instead of rotating.
	caSecretName = "clrk-controller-manager-ca"

	// cert-manager object names.
	certManagerIssuerName = "clrk-selfsigned"
	certManagerCertName   = "clrk-controller-manager"

	caValidity   = 10 * 365 * 24 * time.Hour
	leafValidity = 2 * 365 * 24 * time.Hour
)

// servingDNSNames are the SANs the aggregated APIService's caBundle is verified
// against: the cm Service's in-cluster DNS names.
func servingDNSNames(p Profile) []string {
	return []string{
		ControllerManagerName + "." + p.Namespace + ".svc",
		ControllerManagerName + "." + p.Namespace + ".svc.cluster.local",
	}
}

// PrepareTLS produces the serving-cert objects for the chosen TLS mode and
// configures p so the builders wire the APIService caBundle (or cert-manager
// CA-injection annotation) and the cm cert mount. It reads the cluster to reuse
// an existing self-signed CA, so re-runs don't rotate the cert.
//
//   - TLSCertManager: returns a selfSigned Issuer + a Certificate; cert-manager
//     mints servingCertSecretName and injects the CA into the APIService via the
//     cert-manager.io/inject-ca-from annotation (p.CertManagerCertRef).
//   - TLSSelfSigned: mints (or reuses) a CA + serving cert, returns the serving
//     Secret + the CA Secret, and sets p.CABundle for APIService.caBundle.
//   - TLSInsecureSkipVerify: no objects (dev posture).
func PrepareTLS(ctx context.Context, c client.Client, p *Profile) ([]client.Object, error) {
	switch p.TLS {
	case TLSCertManager:
		return certManagerObjects(p), nil
	case TLSSelfSigned:
		return selfSignedObjects(ctx, c, p)
	default:
		return nil, nil
	}
}

// certManagerObjects builds a per-install self-signed Issuer and a Certificate
// for the cm serving cert. Built as Unstructured so clrk need not vendor the
// cert-manager API types; the GVKs resolve via discovery (cert-manager CRDs are
// present, which is why this path was selected).
func certManagerObjects(p *Profile) []client.Object {
	issuer := &unstructured.Unstructured{}
	issuer.SetGroupVersionKind(schema.GroupVersionKind{Group: "cert-manager.io", Version: "v1", Kind: "Issuer"})
	issuer.SetName(certManagerIssuerName)
	issuer.SetNamespace(p.Namespace)
	issuer.Object["spec"] = map[string]interface{}{
		"selfSigned": map[string]interface{}{},
	}

	cert := &unstructured.Unstructured{}
	cert.SetGroupVersionKind(schema.GroupVersionKind{Group: "cert-manager.io", Version: "v1", Kind: "Certificate"})
	cert.SetName(certManagerCertName)
	cert.SetNamespace(p.Namespace)
	dns := servingDNSNames(*p)
	cert.Object["spec"] = map[string]interface{}{
		"secretName": servingCertSecretName,
		"commonName": dns[0],
		"dnsNames":   toIfaceSlice(dns),
		"issuerRef": map[string]interface{}{
			"name":  certManagerIssuerName,
			"kind":  "Issuer",
			"group": "cert-manager.io",
		},
		"usages": []interface{}{"server auth"},
	}

	// The APIService's caBundle is filled by cert-manager's CA injector from
	// this Certificate's CA.
	p.CertManagerCertRef = p.Namespace + "/" + certManagerCertName
	return []client.Object{issuer, cert}
}

// selfSignedObjects mints (or reuses) a CA and a serving cert, returning the CA
// Secret and the serving Secret. p.CABundle is set to the CA PEM for the
// APIService.
func selfSignedObjects(ctx context.Context, c client.Client, p *Profile) ([]client.Object, error) {
	caCertPEM, caKeyPEM, err := loadOrMintCA(ctx, c, p.Namespace)
	if err != nil {
		return nil, err
	}
	caCert, caKey, err := parseCertKey(caCertPEM, caKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("parsing CA: %w", err)
	}
	leafCertPEM, leafKeyPEM, err := mintServingCert(caCert, caKey, servingDNSNames(*p))
	if err != nil {
		return nil, fmt.Errorf("minting serving cert: %w", err)
	}

	caSecret := tlsSecret(caSecretName, p.Namespace, caCertPEM, caKeyPEM, nil)
	servingSecret := tlsSecret(servingCertSecretName, p.Namespace, leafCertPEM, leafKeyPEM, caCertPEM)

	p.CABundle = caCertPEM
	return []client.Object{caSecret, servingSecret}, nil
}

// loadOrMintCA returns the persisted CA cert/key PEM from the CA Secret, minting
// (and not yet persisting — the caller applies the returned Secret) a fresh CA
// when none exists. Reuse keeps the APIService caBundle stable across re-runs.
func loadOrMintCA(ctx context.Context, c client.Client, ns string) (certPEM, keyPEM []byte, err error) {
	var sec corev1.Secret
	gerr := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: caSecretName}, &sec)
	if gerr == nil {
		if len(sec.Data["tls.crt"]) > 0 && len(sec.Data["tls.key"]) > 0 {
			return sec.Data["tls.crt"], sec.Data["tls.key"], nil
		}
	} else if !apierrors.IsNotFound(gerr) {
		return nil, nil, fmt.Errorf("reading CA secret: %w", gerr)
	}
	return mintCA()
}

func mintCA() (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := randSerial()
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "clrk-controller-manager-ca"},
		NotBefore:             notBefore(),
		NotAfter:              notBefore().Add(caValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	return encodeCert(der), encodeKey(key), nil
}

func mintServingCert(caCert *x509.Certificate, caKey *ecdsa.PrivateKey, dnsNames []string) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := randSerial()
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: dnsNames[0]},
		NotBefore:    notBefore(),
		NotAfter:     notBefore().Add(leafValidity),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     dnsNames,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}
	return encodeCert(der), encodeKey(key), nil
}

// --- crypto/secret helpers ---

func tlsSecret(name, ns string, certPEM, keyPEM, caPEM []byte) *corev1.Secret {
	data := map[string][]byte{
		"tls.crt": certPEM,
		"tls.key": keyPEM,
	}
	if caPEM != nil {
		data["ca.crt"] = caPEM
	}
	return &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Type:       corev1.SecretTypeTLS,
		Data:       data,
	}
}

func parseCertKey(certPEM, keyPEM []byte) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	cblock, _ := pem.Decode(certPEM)
	if cblock == nil {
		return nil, nil, fmt.Errorf("invalid CA cert PEM")
	}
	cert, err := x509.ParseCertificate(cblock.Bytes)
	if err != nil {
		return nil, nil, err
	}
	kblock, _ := pem.Decode(keyPEM)
	if kblock == nil {
		return nil, nil, fmt.Errorf("invalid CA key PEM")
	}
	key, err := x509.ParseECPrivateKey(kblock.Bytes)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

func encodeCert(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func encodeKey(key *ecdsa.PrivateKey) []byte {
	b, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil
	}
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: b})
}

func randSerial() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
}

// notBefore backdates one hour to tolerate minor clock skew between the
// installer host and the cluster.
func notBefore() time.Time {
	return time.Now().Add(-time.Hour)
}

func toIfaceSlice(s []string) []interface{} {
	out := make([]interface{}, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}

// WaitServingSecret blocks until the cm serving-cert Secret carries a tls.crt,
// or timeout elapses. Used on the cert-manager path, where the Secret is minted
// asynchronously after the Certificate is applied; the cm pod can't mount it (or
// pass its readiness probe) until it exists.
func WaitServingSecret(ctx context.Context, a Applier, ns string, timeout time.Duration) error {
	cl, err := a.KubeClient(ctx)
	if err != nil {
		return err
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		var sec corev1.Secret
		gerr := cl.Get(ctx, client.ObjectKey{Namespace: ns, Name: servingCertSecretName}, &sec)
		if gerr == nil && len(sec.Data["tls.crt"]) > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timed out waiting for serving cert secret %s/%s", ns, servingCertSecretName)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// DetectCertManager reports whether the cert-manager.io API group is served,
// which selects the cert-manager TLS path over the self-signed one.
func DetectCertManager(disco discovery.DiscoveryInterface) bool {
	groups, err := disco.ServerGroups()
	if err != nil {
		return false
	}
	return hasGroup(groups, "cert-manager.io")
}
