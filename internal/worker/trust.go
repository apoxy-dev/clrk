//go:build linux

package worker

import (
	"fmt"
	"os"
	"path/filepath"
)

// wellKnownTrustPaths lists CA bundle locations that common Linux
// distributions ship with. We bind-mount over every one that already exists
// in the sandbox rootfs so that programs relying on the default system
// bundle (OpenSSL via the distro path, curl, Python's ssl module, etc.)
// trust our MITM CA.
var wellKnownTrustPaths = []string{
	"/etc/ssl/certs/ca-certificates.crt",                // Debian / Ubuntu.
	"/etc/pki/tls/certs/ca-bundle.crt",                  // RHEL / Fedora / CentOS.
	"/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", // Fedora 22+.
	"/etc/ssl/cert.pem",                                 // Alpine / macOS-style.
}

// trustEnvVars are env vars respected by various TLS client libraries. We
// point them all at one of the well-known system paths so that bind-mounted
// bundles are picked up by programs that honor env over the default path.
var trustEnvVars = []string{
	"SSL_CERT_FILE",
	"REQUESTS_CA_BUNDLE",  // Python requests.
	"CURL_CA_BUNDLE",      // curl.
	"GIT_SSL_CAINFO",      // git.
	"NODE_EXTRA_CA_CERTS", // Node.js.
}

// writeAgentCA stores the agent's CA PEM in a per-sandbox file on the host
// so that the OCI spec's bind mounts can reference it. Returns the host
// path.
func (m *SandboxManager) writeAgentCA(id SandboxID, caPEM []byte) (string, error) {
	dir := filepath.Join(m.rootDir, string(id)+"-trust")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating trust dir: %w", err)
	}
	path := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(path, caPEM, 0o444); err != nil {
		return "", fmt.Errorf("writing ca.crt: %w", err)
	}
	return path, nil
}

// removeAgentCA cleans up the per-sandbox trust dir on sandbox delete.
func (m *SandboxManager) removeAgentCA(id SandboxID) {
	_ = os.RemoveAll(filepath.Join(m.rootDir, string(id)+"-trust"))
}

// trustEnv returns env vars pointing every common TLS trust override at the
// given in-sandbox path. The caller prepends these to the process env so
// user-supplied values override them.
func trustEnv(sandboxPath string) []string {
	out := make([]string, 0, len(trustEnvVars))
	for _, name := range trustEnvVars {
		out = append(out, name+"="+sandboxPath)
	}
	return out
}
