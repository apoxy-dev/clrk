// Package certprovider implements the envoy.service.tls.v3.
// CertificateProviderService that Envoy's grpc_certificate_provider custom
// handshaker calls during MITM TLS termination. It mints leaf certs on the
// fly for the SNI observed in the ClientHello, signed by a per-EgressGateway
// CA Secret.
//
// Envoy's stock grpc_certificate_provider handshaker only forwards the SNI
// to FetchCertificate — it does NOT propagate GrpcService.InitialMetadata,
// so we can't pass the EgressGateway identity in metadata. Instead we use
// the gRPC peer's source IP to look up the calling Envoy pod and read its
// `gateway.envoyproxy.io/owning-gateway-*` labels — each EG has its own
// Envoy pod set, so peer IP uniquely identifies the EG.
package certprovider

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
	"net"
	"strings"
	"sync"
	"time"

	tlsv3 "github.com/apoxy-dev/envoy-go/envoy/service/tls/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/durationpb"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	clrkcontroller "github.com/apoxy-dev/clrk/internal/controller"
)

// Labels EG sets on the proxy Pods/Services it provisions for each
// Gateway. We read these off the calling pod (resolved via peer IP) to
// scope the CA lookup to the right EgressGateway.
const (
	labelOwningGatewayName      = "gateway.envoyproxy.io/owning-gateway-name"
	labelOwningGatewayNamespace = "gateway.envoyproxy.io/owning-gateway-namespace"
)

// envoyGatewayPodNamespace is where Envoy Gateway provisions data-plane
// pods. Standard install path; matches the constant used by the
// EgressGateway controller.
const envoyGatewayPodNamespace = "envoy-gateway-system"

const (
	// leafValidity is how long a minted leaf is advertised as valid. We
	// rotate leaves aggressively so a CA compromise has a short reuse
	// window. The custom handshaker's cache_ttl matches this.
	leafValidity = 1 * time.Hour

	// caCacheTTL bounds how long we trust an in-memory parsed CA between
	// Secret re-reads. CA rotation is manual (delete+recreate Secret), so
	// a minute of staleness is fine.
	caCacheTTL = 1 * time.Minute
)

type cachedCA struct {
	cert      *x509.Certificate
	key       *ecdsa.PrivateKey
	fetchedAt time.Time
}

type leafKey struct {
	egNamespace string
	egName      string
	sni         string
}

type cachedLeaf struct {
	certPEM string
	keyPEM  string
	expires time.Time
}

// Server implements envoy.service.tls.v3.CertificateProviderServiceServer.
type Server struct {
	tlsv3.UnimplementedCertificateProviderServiceServer

	client client.Client

	mu        sync.Mutex
	caCache   map[types.NamespacedName]*cachedCA
	leafCache map[leafKey]*cachedLeaf
}

// New constructs a handshaker gRPC server backed by the given client.
func New(c client.Client) *Server {
	return &Server{
		client:    c,
		caCache:   make(map[types.NamespacedName]*cachedCA),
		leafCache: make(map[leafKey]*cachedLeaf),
	}
}

// FetchCertificate returns a leaf cert + key for the requested SNI, signed
// by the EgressGateway CA identified via gRPC metadata.
func (s *Server) FetchCertificate(ctx context.Context, req *tlsv3.CertificateRequest) (*tlsv3.CertificateResponse, error) {
	logger := log.FromContext(ctx).WithName("certprovider")

	sni := req.GetSni()
	if sni == "" {
		return errorResponse("missing SNI"), nil
	}

	egKey, err := s.egFromPeer(ctx)
	if err != nil {
		logger.Error(err, "Resolving EgressGateway from gRPC peer")
		return errorResponse(err.Error()), nil
	}

	now := time.Now()
	lk := leafKey{egNamespace: egKey.Namespace, egName: egKey.Name, sni: sni}

	// Fast path: leaf cache hit.
	s.mu.Lock()
	if hit, ok := s.leafCache[lk]; ok && hit.expires.After(now.Add(5*time.Minute)) {
		resp := successResponse(hit.certPEM, hit.keyPEM, hit.expires.Sub(now))
		s.mu.Unlock()
		return resp, nil
	}
	s.mu.Unlock()

	ca, err := s.loadCA(ctx, egKey, now)
	if err != nil {
		logger.Error(err, "Loading EgressGateway CA", "eg", egKey)
		return errorResponse(fmt.Sprintf("load CA: %v", err)), nil
	}

	certPEM, keyPEM, notAfter, err := mintLeaf(ca, sni, now)
	if err != nil {
		logger.Error(err, "Minting leaf", "sni", sni)
		return errorResponse(fmt.Sprintf("mint leaf: %v", err)), nil
	}

	s.mu.Lock()
	s.leafCache[lk] = &cachedLeaf{certPEM: certPEM, keyPEM: keyPEM, expires: notAfter}
	s.mu.Unlock()

	return successResponse(certPEM, keyPEM, notAfter.Sub(now)), nil
}

func (s *Server) loadCA(ctx context.Context, key types.NamespacedName, now time.Time) (*cachedCA, error) {
	s.mu.Lock()
	hit, ok := s.caCache[key]
	s.mu.Unlock()
	if ok && now.Sub(hit.fetchedAt) < caCacheTTL {
		return hit, nil
	}

	var sec corev1.Secret
	secKey := types.NamespacedName{
		Namespace: key.Namespace,
		Name:      clrkcontroller.EgressGatewayCASecretName(key.Name),
	}
	if err := s.client.Get(ctx, secKey, &sec); err != nil {
		return nil, fmt.Errorf("get secret %s: %w", secKey, err)
	}
	certPEM := sec.Data[corev1.TLSCertKey]
	keyPEM := sec.Data[corev1.TLSPrivateKeyKey]
	if len(certPEM) == 0 || len(keyPEM) == 0 {
		return nil, fmt.Errorf("secret %s missing tls.crt or tls.key", secKey)
	}
	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil {
		return nil, fmt.Errorf("decoding CA cert PEM")
	}
	caCert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse CA cert: %w", err)
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return nil, fmt.Errorf("decoding CA key PEM")
	}
	caKey, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse CA key: %w", err)
	}

	entry := &cachedCA{cert: caCert, key: caKey, fetchedAt: now}
	s.mu.Lock()
	s.caCache[key] = entry
	s.mu.Unlock()
	return entry, nil
}

// egFromPeer maps the gRPC peer's source IP back to the calling Envoy pod
// and pulls the EgressGateway it belongs to off the Envoy-Gateway-managed
// owning-gateway labels. The Gateway name is `clrk-eg-<eg-name>` per the
// EgressGateway controller's GatewayNamePrefix convention; we strip the
// prefix to recover the EgressGateway name.
//
// In `clrk dev` the peer IP is the Service NAT IP (workers/k3s and the
// controller-manager are on a docker network in front of k3s) so the
// pod-IP lookup misses. As a development-only fallback we then accept the
// single EgressGateway in the cluster; multi-EG dev would need real peer
// identity (e.g. encoding the EG into the gRPC target_uri authority).
func (s *Server) egFromPeer(ctx context.Context) (types.NamespacedName, error) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return types.NamespacedName{}, fmt.Errorf("no gRPC peer info on context")
	}
	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return types.NamespacedName{}, fmt.Errorf("parse peer addr %q: %w", p.Addr.String(), err)
	}

	var pods corev1.PodList
	if err := s.client.List(ctx, &pods, client.InNamespace(envoyGatewayPodNamespace)); err != nil {
		return types.NamespacedName{}, fmt.Errorf("list pods in %s: %w", envoyGatewayPodNamespace, err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.PodIP != host {
			continue
		}
		gwName := pod.Labels[labelOwningGatewayName]
		gwNs := pod.Labels[labelOwningGatewayNamespace]
		if gwName == "" || gwNs == "" {
			return types.NamespacedName{}, fmt.Errorf("pod %s/%s missing owning-gateway labels",
				pod.Namespace, pod.Name)
		}
		egName := strings.TrimPrefix(gwName, clrkcontroller.GatewayNamePrefix)
		if egName == gwName {
			return types.NamespacedName{}, fmt.Errorf(
				"gateway name %q does not start with clrk EG prefix %q",
				gwName, clrkcontroller.GatewayNamePrefix)
		}
		return types.NamespacedName{Namespace: gwNs, Name: egName}, nil
	}

	// dev fallback: single-EG cluster.
	var egs clrkv1alpha1.EgressGatewayList
	if err := s.client.List(ctx, &egs); err != nil {
		return types.NamespacedName{}, fmt.Errorf("list EgressGateways: %w", err)
	}
	if len(egs.Items) == 1 {
		eg := egs.Items[0]
		return types.NamespacedName{Namespace: eg.Namespace, Name: eg.Name}, nil
	}
	return types.NamespacedName{}, fmt.Errorf("no pod in %s matches peer IP %s and %d EgressGateways exist (need 1 for dev fallback)",
		envoyGatewayPodNamespace, host, len(egs.Items))
}

// mintLeaf signs a new leaf certificate for sni, valid for leafValidity.
// Returns the PEM-encoded leaf cert chain, PEM-encoded ECDSA private key,
// and the absolute NotAfter timestamp.
func mintLeaf(ca *cachedCA, sni string, now time.Time) (certPEM, keyPEM string, notAfter time.Time, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("generating key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("generating serial: %w", err)
	}
	notAfter = now.Add(leafValidity)

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: sni},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	if ip := net.ParseIP(sni); ip != nil {
		tmpl.IPAddresses = []net.IP{ip}
	} else {
		tmpl.DNSNames = []string{sni}
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("signing leaf: %w", err)
	}

	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("marshal key: %w", err)
	}
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	return certPEM, keyPEM, notAfter, nil
}

func successResponse(certPEM, keyPEM string, ttl time.Duration) *tlsv3.CertificateResponse {
	return &tlsv3.CertificateResponse{
		Status:              tlsv3.CertificateResponse_SUCCESS,
		CertificateChainPem: certPEM,
		PrivateKeyPem:       keyPEM,
		CacheTtl:            durationpb.New(ttl),
	}
}

func errorResponse(msg string) *tlsv3.CertificateResponse {
	return &tlsv3.CertificateResponse{
		Status:       tlsv3.CertificateResponse_ERROR,
		ErrorMessage: msg,
	}
}

