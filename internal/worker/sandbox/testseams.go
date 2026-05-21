//go:build linux

package sandbox

import (
	"net"
	"net/netip"

	"github.com/apoxy-dev/clrk/internal/egress"
)

// Test seams for the linux-only surface of internal/worker. Lives
// alongside the platform code so apoxy-cloud//clrk/worker/ can drive
// the production paths without exporting helpers to non-test callers.

// PickBackendForTest exposes pickBackend (egress_bridge_linux.go) so
// the test target can lock down the listener-selection algorithm
// without going through a full bridge accept loop.
func PickBackendForTest(backends []egress.BackendListener, dstPort uint16) *egress.BackendListener {
	return pickBackend(backends, dstPort)
}

// SanitizedSrcForTest exposes sanitizedSrc for the test target.
// Caller passes the local addr as a net.Addr the same way the bridge
// reads it off upstream.LocalAddr().
func SanitizedSrcForTest(la net.Addr, dst netip.AddrPort) netip.AddrPort {
	return sanitizedSrc(la, dst)
}

// SynthesizeSandboxMACForTest exposes synthesizeSandboxMAC.
func SynthesizeSandboxMACForTest(id string) string {
	return synthesizeSandboxMAC(id)
}

// BuildSandboxInitStrForTest exposes buildSandboxInitStr.
func BuildSandboxInitStrForTest(sb *Instance, imdsHostAddr, egressHostAddr string, resolvers []netip.AddrPort) (string, error) {
	return buildSandboxInitStr(sb, imdsHostAddr, egressHostAddr, resolvers)
}

// NewEgressBridgePermissiveForTest builds an EgressBridge with the
// worker-local destination filter disabled. Never use in production.
func NewEgressBridgePermissiveForTest(hostAddr string, lookup EgressStateLookup) (*EgressBridge, error) {
	return newBridge(hostAddr, lookup, nil)
}

// NewEgressBridgeWithLocalIPsForTest replaces the local-interface IP
// snapshot with the caller-supplied set so tests can exercise the
// worker_local_ifaddr branch with an injected IP. Never use in
// production.
func NewEgressBridgeWithLocalIPsForTest(hostAddr string, lookup EgressStateLookup, localIPs []netip.Addr) (*EgressBridge, error) {
	set := make(map[netip.Addr]struct{}, len(localIPs))
	for _, ip := range localIPs {
		set[ip.Unmap()] = struct{}{}
	}
	return newBridge(hostAddr, lookup, &localDstFilter{localIPs: set})
}
