//go:build linux

package worker

import (
	"crypto/sha256"
	"fmt"

	"github.com/apoxy-dev/clrk/internal/sentrystack"
)

// buildSandboxInitStr renders the per-sandbox InitStr that the worker
// ships to the Sentry's PluginStack via the CLRK_SENTRYSTACK_INITSTR
// env var on the runsc-create subprocess.
func buildSandboxInitStr(sb *SandboxInstance) (string, error) {
	is := &sentrystack.InitStr{
		SandboxID: string(sb.ID),
		Eth0MAC:   synthesizeSandboxMAC(string(sb.ID)),
	}
	// /32 (not the allocator's /30) because loopether short-circuits
	// delivery — single-host eth0 is what userspace tooling sees.
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
