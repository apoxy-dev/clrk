//go:build linux

package sentrystack

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"syscall"

	"github.com/dpeckett/contextio"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/waiter"
)

// tcpForwarderMaxInFlight bounds the number of half-open TCP forwarder
// requests gVisor will queue before dropping new SYNs. Mirrors the value
// used by internal/netstack/tcp_forwarder.go; high enough that a burst
// of concurrent connects from inside the sandbox doesn't get RSTed
// pre-handoff.
const tcpForwarderMaxInFlight = 65535

// dialFunc is the upstream-dial path used by onTCP. Phase 2 uses a plain
// net.Dialer; Phase 3 replaces this with a function that examines the
// dst and either dials the worker's host-bound IMDS port (with PROXY-v2
// sandbox-id TLV) or the Envoy MITM listener (with PROXY-v2 identity +
// dst-name TLVs). Keeping it as a field on Stack means swap-out is a
// single assignment in Init.
type dialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// installTCPForwarder registers a tcp.NewForwarder on the stack that
// handles every TCP SYN. Caller passes the dial function that the
// forwarder uses to reach upstream; for Phase 2 this is a stdlib
// net.Dialer talking to the original dst.
//
// The forwarder is registered once at Init time and lives for the
// stack's lifetime. tcp.NewForwarder hooks itself onto the TCP protocol
// handler — there's nothing to unregister at shutdown beyond closing
// the stack.
func (s *Stack) installTCPForwarder(dial dialFunc) {
	ts := s.tcpipStack()
	fwd := tcp.NewForwarder(ts, 0, tcpForwarderMaxInFlight, s.makeTCPHandler(dial))
	ts.SetTransportProtocolHandler(tcp.ProtocolNumber, fwd.HandlePacket)
}

// makeTCPHandler returns the onTCP callback. Each accepted SYN spawns a
// goroutine that:
//
//   1. Creates the local (sandbox-side) endpoint via req.CreateEndpoint.
//   2. Dials the upstream via dial.
//   3. Bidirectionally splices until either side closes.
//
// Mirrors internal/netstack/tcp_forwarder.go's tcpHandler in shape. The
// main differences from that file:
//
//   - No SourceAddr ctx threading. Under per-sandbox Sentry the sandbox
//     identity is implicit in the process; there's no slot table to look
//     up by source IP. Phase 3's MITM dial path encodes it into the
//     PROXY-v2 header explicitly instead.
//   - No "in-band response" path (req.Complete called on the sandbox
//     endpoint) — same as today; FIN-on-clean-close, RST-on-error.
func (s *Stack) makeTCPHandler(dial dialFunc) func(req *tcp.ForwarderRequest) {
	return func(req *tcp.ForwarderRequest) {
		details := req.ID()
		srcAddrPort := netip.AddrPortFrom(addrFromTcpip(details.RemoteAddress), details.RemotePort)
		dstAddrPort := netip.AddrPortFrom(unmap4in6(addrFromTcpip(details.LocalAddress)), details.LocalPort)

		logger := slog.With(
			slog.String("src", srcAddrPort.String()),
			slog.String("dst", dstAddrPort.String()),
		)

		go s.handleTCP(req, dstAddrPort, dial, logger)
	}
}

func (s *Stack) handleTCP(
	req *tcp.ForwarderRequest,
	dstAddrPort netip.AddrPort,
	dial dialFunc,
	logger *slog.Logger,
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wq waiter.Queue
	ep, tcpipErr := req.CreateEndpoint(&wq)
	if tcpipErr != nil {
		logger.Warn("Failed to create local endpoint", slog.String("error", tcpipErr.String()))
		req.Complete(true) // RST
		return
	}

	// Cancel splice context on TCP HUP from the sandbox side so we
	// don't keep an upstream conn open after the agent half-closes.
	waitEntry, notifyCh := waiter.NewChannelEntry(waiter.EventHUp)
	wq.EventRegister(&waitEntry)
	defer wq.EventUnregister(&waitEntry)
	go func() {
		select {
		case <-ctx.Done():
		case <-notifyCh:
			cancel()
		}
	}()

	ep.SocketOptions().SetDelayOption(false) // disable Nagle
	ep.SocketOptions().SetKeepAlive(true)

	local := gonet.NewTCPConn(&wq, ep)
	defer local.Close()

	remote, err := dial(ctx, "tcp", dstAddrPort.String())
	if err != nil {
		logger.Warn("Failed to dial upstream", slog.Any("error", err))
		req.Complete(true) // RST
		return
	}
	defer remote.Close()

	logger.Debug("Splicing TCP")
	wn, err := contextio.SpliceContext(ctx, local, remote, nil)
	if err != nil && !errors.Is(err, context.Canceled) {
		if isBenignPeerClose(err) {
			logger.Debug("Peer closed mid-splice", slog.Any("error", err))
		} else {
			logger.Warn("Splice error", slog.Any("error", err))
		}
		req.Complete(true) // RST
		return
	}
	logger.Debug("TCP session closed", slog.Int64("bytes", wn))
	req.Complete(false) // FIN
}

// directDialer is the Phase 2 dial function — dials the original dst via
// stdlib. Phase 3 will replace this with a wrapper that branches on
// dst (IMDS / MITM / direct) and writes PROXY-v2 headers.
func directDialer() dialFunc {
	d := net.Dialer{}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return d.DialContext(ctx, network, addr)
	}
}

// addrFromTcpip converts a tcpip.Address into a netip.Addr. Handles both
// the 4-byte and 16-byte cases without inferring family from length
// alone (the v4-in-v6 mapping is handled separately via unmap4in6).
func addrFromTcpip(a tcpip.Address) netip.Addr {
	switch a.Len() {
	case 4:
		return netip.AddrFrom4(a.As4())
	case 16:
		return netip.AddrFrom16(a.As16())
	default:
		return netip.Addr{}
	}
}

// unmap4in6 collapses ::ffff:0.0.0.0/96 onto v4. The tcpip stack hands
// us v4-mapped addresses for sockets opened on a v6 endpoint that ended
// up routing v4; the rest of the dial path expects native v4.
func unmap4in6(a netip.Addr) netip.Addr {
	if a.Is4In6() {
		return a.Unmap()
	}
	return a
}

// isBenignPeerClose tells routine close-on-write (ECONNRESET after the
// upstream finished writing, broken pipe in the reverse direction)
// apart from real forwarding failures. Same heuristic as the existing
// internal/netstack forwarder so log noise stays consistent across the
// migration.
func isBenignPeerClose(err error) bool {
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe")
}
