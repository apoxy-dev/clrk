// Package certprovider implements the envoy.service.tls.v3.
// CertificateProviderService that Envoy's grpc_certificate_provider custom
// handshaker calls during MITM TLS termination. It mints leaf certs on the
// fly for the SNI observed in the ClientHello, signed by a per-EgressGateway
// CA Secret.
//
// Envoy identifies the target EgressGateway via initial gRPC metadata set on
// the handshaker config at xDS-translation time (see internal/egextension).
// See package constants for the metadata keys the EG extension must attach.
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
	"sync"
	"time"

	tlsv3 "github.com/apoxy-dev/envoy-go/envoy/service/tls/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"

	clrkcontroller "github.com/apoxy-dev/clrk/internal/controller"
)

// gRPC metadata keys carried on every FetchCertificate request. The EG
// extension server sets these via GrpcService.InitialMetadata so our
// handler can scope the CA lookup to the right EgressGateway. Both values
// are required.
const (
	MetaKeyEgressGatewayNamespace = "x-clrk-egressgateway-namespace"
	MetaKeyEgressGatewayName      = "x-clrk-egressgateway-name"
)

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

	egKey, err := egFromContext(ctx)
	if err != nil {
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

// egFromContext extracts the EgressGateway namespace+name from the gRPC
// initial metadata attached by our EG extension.
func egFromContext(ctx context.Context) (types.NamespacedName, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return types.NamespacedName{}, fmt.Errorf("missing gRPC metadata")
	}
	ns := firstMD(md, MetaKeyEgressGatewayNamespace)
	name := firstMD(md, MetaKeyEgressGatewayName)
	if ns == "" || name == "" {
		return types.NamespacedName{}, fmt.Errorf("missing %s / %s metadata",
			MetaKeyEgressGatewayNamespace, MetaKeyEgressGatewayName)
	}
	return types.NamespacedName{Namespace: ns, Name: name}, nil
}

func firstMD(md metadata.MD, key string) string {
	if v := md.Get(key); len(v) > 0 {
		return v[0]
	}
	return ""
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

