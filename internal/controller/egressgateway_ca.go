package controller

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"
)

// EgressGatewayCASecretLabel marks a Secret as the MITM CA for an EgressGateway.
const EgressGatewayCASecretLabel = "clrk.apoxy.dev/egressgateway"

// EgressGatewayCASecretName returns the deterministic Secret name holding
// the MITM CA keypair for the named EgressGateway. The Secret lives in the
// EgressGateway's namespace. The private key is consumed by the MITM
// handshaker in controller-manager; sandbox workers only need the
// certificate PEM.
func EgressGatewayCASecretName(egName string) string {
	return fmt.Sprintf("clrk-egressgateway-ca-%s", egName)
}

// caValidity is the lifetime of the minted CA. Long enough that we don't
// rotate within a practical deployment window; rotation is manual (delete
// the Secret and let the controller re-mint).
const caValidity = 10 * 365 * 24 * time.Hour

// MintEgressGatewayCA generates a fresh ECDSA P-256 self-signed CA whose
// subject carries the EgressGateway coordinates for operator debuggability.
func MintEgressGatewayCA(namespace, name string) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generating key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, fmt.Errorf("generating serial: %w", err)
	}

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   fmt.Sprintf("clrk EgressGateway %s/%s", namespace, name),
			Organization: []string{"clrk"},
		},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(caValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("signing certificate: %w", err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("marshalling key: %w", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return certPEM, keyPEM, nil
}
