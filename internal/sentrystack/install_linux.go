//go:build linux

package sentrystack

import (
	sandboxsentrystack "github.com/apoxy-dev/clrk/pkg/sandbox/sentrystack"
)

// Stack is the sentrystack PluginStack. Aliased from the neutral core; the
// egress forwarder install (installEgressForwarders) and the test seams in
// this package operate on the same type via this alias.
type Stack = sandboxsentrystack.Stack

// Singleton returns the registered PluginStack. Re-exported from the core
// so cmd/worker's runsc dispatch keeps its existing call site.
var Singleton = sandboxsentrystack.Singleton

// init wires clrk's egress/IMDS/DNS forwarder data path into the core's
// PluginStack. The core wires lo + eth0 addressing and then calls
// ForwarderInstaller (if set) from inside Init; this package, blank-
// imported by the worker binary, sets it so every sandbox boots with the
// full egress data path. A standalone core consumer that doesn't import
// this package leaves the hook nil and gets a lo+eth0 sandbox with no
// outbound forwarder (direct dial).
func init() {
	sandboxsentrystack.ForwarderInstaller = installEgressForwarders
}

// installEgressForwarders builds the per-sandbox DNS cache + routed TCP/UDP
// dialers from the decoded InitStr and registers the TCP and UDP
// forwarders on the stack. Invoked by the core's Init after the lo/eth0
// NICs are wired.
//
// TCP: routedDialer branches on dst — IMDS dsts go to the worker's
// host-bound 127.0.0.1 listener with a PROXY v2 frame carrying SandboxID;
// everything else dials the egress bridge. The frame's TLVDstName is
// filled from dnsCache (populated by the UDP forwarder's :53 response
// path) so the worker bridge and Envoy MITM can attribute by hostname.
//
// UDP: routedUDPDialer branches on port — :53 dials the worker's resolver
// list (from initStr); everything else is denied (worker-side UDP policy
// is not wired yet, so non-DNS UDP fails closed to prevent agents from
// bypassing SandboxPolicy via protocol switch). The forwarder feeds every
// :53 response payload into dnsCache before forwarding it back.
func installEgressForwarders(s *Stack, init *InitStr) {
	dns := newDNSCache()
	tcpDial := newRoutedTCPDialer(init, dns)
	installTCPForwarder(s, tcpDial.DialTCP)
	udpDial := newRoutedUDPDialer(init)
	installUDPForwarder(s, udpDial.DialUDP, dns)
}
