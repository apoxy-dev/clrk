package controller

import (
	"fmt"

	"github.com/apoxy-dev/clrk/internal/egpki"
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

// MintEgressGatewayCA generates a fresh ECDSA P-256 self-signed CA whose
// subject carries the EgressGateway coordinates for operator debuggability. It
// delegates to egpki.MintRootCA so the MITM CA and the control-plane PKI share
// one crypto shape (ECDSA P-256, 128-bit serial, SEC1 "EC PRIVATE KEY" PEM,
// 10-year validity); the only difference is the subject CommonName.
func MintEgressGatewayCA(namespace, name string) (certPEM, keyPEM []byte, err error) {
	return egpki.MintRootCA(fmt.Sprintf("clrk EgressGateway %s/%s", namespace, name))
}
