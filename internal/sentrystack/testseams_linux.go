//go:build linux

package sentrystack

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sort"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
)

// This file exposes the egress-forwarder surface that cross-module unit
// tests in apoxy-cloud//clrk/sentrystack/ reach via the regular import
// path. The lo/eth0 NIC-wiring seams moved to the neutral core
// (pkg/sandbox/sentrystack); only the egress data path this wrapper owns
// is exercised here. go_test targets don't pick up `_test.go` files from
// the imported library, which is why these aren't in an `export_test.go`.

// RoutedTCPDialer aliases the unexported routedDialer so tests can name
// the value type without the package exposing routedDialer to production
// callers.
type RoutedTCPDialer = routedDialer

// NewRoutedTCPDialerForTest constructs a routedDialer the same way the
// egress forwarder install does — see newRoutedTCPDialer in forwarder.go.
// dnsCache may be nil to disable DstName lookup.
func NewRoutedTCPDialerForTest(init *InitStr, dnsCache *DNSCache) *RoutedTCPDialer {
	return newRoutedTCPDialer(init, dnsCache)
}

// DialTCPForTest invokes (*routedDialer).DialTCP directly.
func (d *RoutedTCPDialer) DialTCPForTest(ctx context.Context, src, dst netip.AddrPort) (net.Conn, error) {
	return d.DialTCP(ctx, src, dst)
}

// IMDSTargetsForTest returns the parsed IMDS target set so a test can
// assert newRoutedTCPDialer correctly handled malformed IMDSV4/V6 without
// poking into the unexported field.
func (d *RoutedTCPDialer) IMDSTargetsForTest() []netip.AddrPort {
	out := make([]netip.AddrPort, 0, len(d.imdsTargets))
	for ap := range d.imdsTargets {
		out = append(out, ap)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

// RoutedUDPDialer aliases routedUDPDialer for the test target.
type RoutedUDPDialer = routedUDPDialer

// NewRoutedUDPDialerForTest constructs a routedUDPDialer the same way the
// egress forwarder install does (see newRoutedUDPDialer in udp_linux.go).
func NewRoutedUDPDialerForTest(init *InitStr) *RoutedUDPDialer {
	return newRoutedUDPDialer(init)
}

// DialUDPForTest invokes (*routedUDPDialer).DialUDP directly.
func (d *RoutedUDPDialer) DialUDPForTest(ctx context.Context, src, dst netip.AddrPort) (net.Conn, error) {
	return d.DialUDP(ctx, src, dst)
}

// ResolversForTest returns the parsed resolver list so a test can assert
// newRoutedUDPDialer skips unparseable entries.
func (d *RoutedUDPDialer) ResolversForTest() []netip.AddrPort {
	out := make([]netip.AddrPort, len(d.resolvers))
	copy(out, d.resolvers)
	return out
}

// DialTCPThroughStackForTest opens a TCP connection from inside s to dst,
// routed through the in-Sentry TCP forwarder that the egress installer
// registered. Returns the sandbox-side net.Conn — read/write bytes
// through it to exercise the splice loop. Caller closes the conn.
//
// Drives the same code path a Sentry-attached process would: gonet builds
// an endpoint on the stack, Connect emits a SYN, the loopether
// LinkEndpoint loops it back to DeliverNetworkPacket, the forwarder
// catches it (no listening endpoint matches), and req.CreateEndpoint
// completes the handshake.
func DialTCPThroughStackForTest(ctx context.Context, s *Stack, dst string) (net.Conn, error) {
	dstAP, err := netip.ParseAddrPort(dst)
	if err != nil {
		return nil, fmt.Errorf("parse dst %q: %w", dst, err)
	}
	proto := ipv4.ProtocolNumber
	dstAddr := tcpip.AddrFromSlice(dstAP.Addr().AsSlice())
	if dstAP.Addr().Is6() {
		proto = ipv6.ProtocolNumber
		dstAddr = tcpip.AddrFromSlice(dstAP.Addr().AsSlice())
	}
	return gonet.DialContextTCP(ctx, s.TCPIPStack(), tcpip.FullAddress{
		Addr: dstAddr,
		Port: dstAP.Port(),
	}, proto)
}

// DialUDPThroughStackForTest opens a UDP socket on s pointed at dst. The
// first datagram written through the returned conn flows out via the
// sandbox-side eth0 (loopether), gets caught by the installed UDP
// forwarder, and is dialed upstream by routedUDPDialer.DialUDP — i.e. the
// test drives runUDPFlow + copyUDPPackets through their full happy path.
func DialUDPThroughStackForTest(s *Stack, dst string) (net.Conn, error) {
	dstAP, err := netip.ParseAddrPort(dst)
	if err != nil {
		return nil, fmt.Errorf("parse dst %q: %w", dst, err)
	}
	proto := ipv4.ProtocolNumber
	dstAddr := tcpip.AddrFromSlice(dstAP.Addr().AsSlice())
	if dstAP.Addr().Is6() {
		proto = ipv6.ProtocolNumber
		dstAddr = tcpip.AddrFromSlice(dstAP.Addr().AsSlice())
	}
	return gonet.DialUDP(s.TCPIPStack(), nil, &tcpip.FullAddress{
		Addr: dstAddr,
		Port: dstAP.Port(),
	}, proto)
}
