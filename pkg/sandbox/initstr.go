//go:build linux

package sandbox

import (
	"crypto/sha256"
	"fmt"

	"github.com/apoxy-dev/clrk/pkg/sandbox/sentrystack"
)

// buildSandboxInitStr renders the per-sandbox sentrystack InitStr the
// host ships to the Sentry's PluginStack via the CLRK_SENTRYSTACK_INITSTR
// env var on the runsc-create subprocess.
//
// The core fills the addressing fields it owns — SandboxID, eth0 IPv4
// (/32, since loopether short-circuits delivery so single-host eth0 is
// what userspace tooling sees), the synthesized MAC, and the cosmetic
// gateway — and copies the opaque egress fields ([Spec.Egress]) through
// verbatim. A zero EgressInit yields a lo+eth0 sandbox with no egress
// routing, which an installed forwarder (or its absence) interprets as
// direct dial.
func buildSandboxInitStr(sb *Instance, eg EgressInit) (string, error) {
	is := &sentrystack.InitStr{
		SandboxID:      string(sb.ID),
		Eth0MAC:        synthesizeSandboxMAC(string(sb.ID)),
		IMDSHostAddr:   eg.IMDSHostAddr,
		IMDSV4:         eg.IMDSV4,
		IMDSV6:         eg.IMDSV6,
		EgressHostAddr: eg.EgressHostAddr,
		DNSResolvers:   eg.DNSResolvers,
	}
	if sb.SandboxIP.IsValid() && sb.SandboxIP.Is4() {
		is.Eth0V4 = sb.SandboxIP.String()
		is.Eth0V4PrefixLen = 32
	}
	if sb.GatewayIP.IsValid() && sb.GatewayIP.Is4() {
		is.GatewayV4 = sb.GatewayIP.String()
	}
	return is.Encode()
}

// synthesizeSandboxMAC returns a stable locally-administered MAC for a
// sandbox given its ID. 0x02 prefix marks the address as locally
// administered (not vendor-assigned); the remaining 5 bytes are
// sha256(id)[:5]. Stable per-ID so restarts see the same MAC.
func synthesizeSandboxMAC(id string) string {
	sum := sha256.Sum256([]byte(id))
	return fmt.Sprintf("02:%02x:%02x:%02x:%02x:%02x", sum[0], sum[1], sum[2], sum[3], sum[4])
}
