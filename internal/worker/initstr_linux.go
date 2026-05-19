//go:build linux

package worker

import (
	"crypto/sha256"
	"fmt"
	"net/netip"

	"github.com/apoxy-dev/clrk/internal/ports"
	"github.com/apoxy-dev/clrk/internal/sentrystack"
)

// buildSandboxInitStr renders the per-sandbox InitStr that the worker
// ships to the Sentry's PluginStack via the CLRK_SENTRYSTACK_INITSTR
// env var on the runsc-create subprocess.
//
// imdsHostAddr is the worker-bound IMDS dial target (typically
// "127.0.0.1:<WorkerIMDSPort>") the in-Sentry TCP forwarder dials
// when it matches an outbound SYN to the link-local IMDS addr.
// egressHostAddr is the worker-bound egress dial target (typically
// "127.0.0.1:<WorkerEgressPort>") — every non-IMDS, non-DNS
// outbound TCP from the Sentry lands here for central policy + MITM
// dispatch. resolvers is the worker's DNS resolver list — the
// in-Sentry UDP forwarder dials these for any :53 traffic.
func buildSandboxInitStr(sb *SandboxInstance, imdsHostAddr, egressHostAddr string, resolvers []netip.AddrPort) (string, error) {
	is := &sentrystack.InitStr{
		SandboxID:      string(sb.ID),
		Eth0MAC:        synthesizeSandboxMAC(string(sb.ID)),
		IMDSHostAddr:   imdsHostAddr,
		IMDSV4:         fmt.Sprintf("%s:%d", ports.MetadataAddrV4, ports.MetadataPort),
		IMDSV6:         fmt.Sprintf("[%s]:%d", ports.MetadataAddrV6, ports.MetadataPort),
		EgressHostAddr: egressHostAddr,
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
	for _, r := range resolvers {
		is.DNSResolvers = append(is.DNSResolvers, r.String())
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
