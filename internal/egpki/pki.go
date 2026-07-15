// Package egpki mints the control-plane PKI keypairs that secure the
// controller-manager's :9443 gRPC control plane: a single self-signed root
// CA, the :9443 server certificate, and the per-EgressGateway client
// certificates the data-plane Envoys present over mTLS.
//
// The crypto shape (ECDSA P-256, 128-bit serials, SEC1 "EC PRIVATE KEY"
// PEM) is deliberately identical to the per-EG MITM CA minted in
// internal/controller (see MintEgressGatewayCA), so operators see one
// consistent key type across clrk-issued material.
package egpki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/apoxy-dev/clrk/internal/egidentity"
)

// Certificate lifetimes. The control-plane root is internal and long-lived;
// leaf certs share its horizon because rotation is manual (delete the Secret
// and let the bootstrap / reconciler re-mint), matching the per-EG MITM CA.
const (
	rootValidity = 10 * 365 * 24 * time.Hour
	leafValidity = 10 * 365 * 24 * time.Hour
	// clockSkew backdates NotBefore so a freshly minted cert validates on
	// peers whose clocks run slightly ahead.
	clockSkew = 5 * time.Minute
)

// serialNumber returns a random 128-bit certificate serial.
func serialNumber() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
}

// encodeKeyPair PEM-encodes a signed DER cert and its EC private key, using
// the same SEC1 "EC PRIVATE KEY" block the per-EG MITM CA emits.
func encodeKeyPair(der []byte, key *ecdsa.PrivateKey) (certPEM, keyPEM []byte, err error) {
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("marshalling key: %w", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, nil
}

// MintRootCA generates a fresh ECDSA P-256 self-signed root CA. It is the
// single control-plane root: the issuer of both the :9443 server cert and
// every per-EG client cert.
func MintRootCA(commonName string) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generating key: %w", err)
	}
	serial, err := serialNumber()
	if err != nil {
		return nil, nil, fmt.Errorf("generating serial: %w", err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"clrk"},
		},
		NotBefore:             now.Add(-clockSkew),
		NotAfter:              now.Add(rootValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("signing certificate: %w", err)
	}
	return encodeKeyPair(der, key)
}

// parseParent decodes a CERTIFICATE + EC PRIVATE KEY PEM pair into the
// signing material for a child certificate.
func parseParent(parentCertPEM, parentKeyPEM []byte) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	cb, _ := pem.Decode(parentCertPEM)
	if cb == nil {
		return nil, nil, fmt.Errorf("decoding parent certificate PEM")
	}
	parent, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing parent certificate: %w", err)
	}
	kb, _ := pem.Decode(parentKeyPEM)
	if kb == nil {
		return nil, nil, fmt.Errorf("decoding parent key PEM")
	}
	key, err := x509.ParseECPrivateKey(kb.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing parent key: %w", err)
	}
	return parent, key, nil
}

// signLeaf generates a fresh P-256 key, stamps serial/validity onto tmpl, and
// signs it against the parent cert/key.
func signLeaf(tmpl *x509.Certificate, parentCertPEM, parentKeyPEM []byte) (certPEM, keyPEM []byte, err error) {
	parent, parentKey, err := parseParent(parentCertPEM, parentKeyPEM)
	if err != nil {
		return nil, nil, err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generating key: %w", err)
	}
	serial, err := serialNumber()
	if err != nil {
		return nil, nil, fmt.Errorf("generating serial: %w", err)
	}
	now := time.Now()
	tmpl.SerialNumber = serial
	tmpl.NotBefore = now.Add(-clockSkew)
	tmpl.NotAfter = now.Add(leafValidity)
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, parentKey)
	if err != nil {
		return nil, nil, fmt.Errorf("signing certificate: %w", err)
	}
	return encodeKeyPair(der, key)
}

// MintServerCert signs a server certificate for the :9443 control-plane
// listener with the given DNS and IP SANs, against the control-plane root.
func MintServerCert(parentCertPEM, parentKeyPEM []byte, dnsNames []string, ips []net.IP) (certPEM, keyPEM []byte, err error) {
	tmpl := &x509.Certificate{
		Subject:               pkix.Name{CommonName: "clrk control-plane server", Organization: []string{"clrk"}},
		DNSNames:              dnsNames,
		IPAddresses:           ips,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	return signLeaf(tmpl, parentCertPEM, parentKeyPEM)
}

// MintEGClientCert signs a client certificate whose sole DNS SAN is the
// synthetic EG authority egidentity.AuthorityFor(eg). The controller-
// manager's mTLS interceptor extracts the EG identity from that verified SAN,
// so the SAN is the identity claim (unforgeable because it's root-signed).
func MintEGClientCert(parentCertPEM, parentKeyPEM []byte, eg types.NamespacedName) (certPEM, keyPEM []byte, err error) {
	authority := egidentity.AuthorityFor(eg)
	tmpl := &x509.Certificate{
		Subject:               pkix.Name{CommonName: authority, Organization: []string{"clrk"}},
		DNSNames:              []string{authority},
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	return signLeaf(tmpl, parentCertPEM, parentKeyPEM)
}
