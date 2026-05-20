//go:build linux

package sentrystack

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"runtime/debug"

	"github.com/dpeckett/contextio"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/waiter"

	"github.com/apoxy-dev/clrk/internal/egress"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
)

// tcpForwarderMaxInFlight bounds the number of half-open TCP forwarder
// requests gVisor will queue before dropping new SYNs. Mirrors the value
// used by internal/netstack/tcp_forwarder.go; high enough that a burst
// of concurrent connects from inside the sandbox doesn't get RSTed
// pre-handoff.
const tcpForwarderMaxInFlight = 65535

// tcpDialFunc is the upstream-dial path used by onTCP. Receives the
// original (sandbox-visible) src and dst so the implementation can
// route (e.g. dial the host-bound IMDS port for IMDS dsts, dial the
// MITM for everything else) and emit a PROXY v2 frame carrying the
// original tuple.
type tcpDialFunc func(ctx context.Context, src, dst netip.AddrPort) (net.Conn, error)

// installTCPForwarder registers a tcp.NewForwarder on the stack that
// handles every TCP SYN. The forwarder is registered once at Init
// time and lives for the stack's lifetime — there's nothing to
// unregister at shutdown beyond closing the stack.
func (s *Stack) installTCPForwarder(dial tcpDialFunc) {
	ts := s.tcpipStack()
	fwd := tcp.NewForwarder(ts, 0, tcpForwarderMaxInFlight, s.makeTCPHandler(dial))
	ts.SetTransportProtocolHandler(tcp.ProtocolNumber, fwd.HandlePacket)
}

// makeTCPHandler returns the onTCP callback. Each accepted SYN spawns a
// goroutine that:
//
//  1. Creates the local (sandbox-side) endpoint via req.CreateEndpoint.
//  2. Dials the upstream via dial, passing original src + dst.
//  3. Bidirectionally splices until either side closes.
func (s *Stack) makeTCPHandler(dial tcpDialFunc) func(req *tcp.ForwarderRequest) {
	return func(req *tcp.ForwarderRequest) {
		details := req.ID()
		srcAddrPort := netip.AddrPortFrom(unmap4in6(addrFromTcpip(details.RemoteAddress)), details.RemotePort)
		dstAddrPort := netip.AddrPortFrom(unmap4in6(addrFromTcpip(details.LocalAddress)), details.LocalPort)

		logger := slog.With(
			slog.String("src", srcAddrPort.String()),
			slog.String("dst", dstAddrPort.String()),
		)

		go func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("TCP forwarder goroutine panic",
						slog.Any("recover", r),
						slog.String("stack", string(debug.Stack())))
				}
			}()
			s.handleTCP(req, srcAddrPort, dstAddrPort, dial, logger)
		}()
	}
}

func (s *Stack) handleTCP(
	req *tcp.ForwarderRequest,
	srcAddrPort, dstAddrPort netip.AddrPort,
	dial tcpDialFunc,
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

	remote, err := dial(ctx, srcAddrPort, dstAddrPort)
	if err != nil {
		logger.Warn("Failed to dial upstream", slog.Any("error", err))
		req.Complete(true) // RST
		return
	}
	defer remote.Close()

	logger.Debug("Splicing TCP")
	wn, err := contextio.SpliceContext(ctx, local, remote, nil)
	if err != nil && !errors.Is(err, context.Canceled) {
		if egress.IsBenignClose(err) {
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

// routedDialer is the Sentry-side TCP routing layer. The forwarder
// hands it the original src + dst the sandbox tried to reach; the
// dialer chooses an upstream (worker IMDS bridge, worker egress
// bridge, or direct) and writes a PROXY v2 frame carrying SandboxID
// so the upstream can demux without keying on the local endpoint.
//
// Worker-as-central-dispatcher: identity, InvocationID, Backends,
// and Policy live on the worker side — when the Sentry tunnels to
// the egress bridge, the worker enriches the PROXY v2 frame with
// the full identity / dst-name on the way to Envoy MITM. That keeps
// SetEgressBackends / SetEgressPolicy / SetInvocationID live-
// updatable without a urpc channel into a running Sentry.
type routedDialer struct {
	// sandboxID is the worker-side opaque identifier echoed back over
	// TLVSandboxID. Used by both the IMDS bridge and the egress
	// bridge to demux the shared 127.0.0.1 listener.
	sandboxID string

	// imdsHostAddr is the worker-bound dial target for IMDS streams
	// (typically "127.0.0.1:<WorkerIMDSPort>"). Empty disables IMDS
	// routing entirely — outbound :80 to 169.254.169.254 falls
	// through to direct dial.
	imdsHostAddr string

	// imdsTargets is the set of (sandbox-visible) AddrPorts the
	// forwarder treats as IMDS-bound. Resolved once at Init from
	// initStr so the per-flow match is a cheap map lookup.
	imdsTargets map[netip.AddrPort]struct{}

	// egressHostAddr is the worker-bound dial target for every
	// non-IMDS outbound TCP stream (typically
	// "127.0.0.1:<WorkerEgressPort>"). Empty disables egress
	// bridging — outbound TCP falls through to direct dial, which
	// loses MITM + policy enforcement and is intended only for tests
	// driving the sentrystack in isolation.
	egressHostAddr string

	// dnsCache resolves a sandbox-visible dst IP back to the qname
	// the agent looked up moments earlier (populated by the UDP
	// forwarder's :53 response path). Looked up on every egress dial;
	// the result becomes the PROXY v2 TLVDstName so the worker
	// bridge and Envoy MITM can attribute the connection by name.
	// nil disables the lookup; dialWithProxyHeader still works.
	dnsCache *dnsCache

	// fallback is the dial path for traffic that doesn't match any
	// routed branch. Plain net.Dialer through the Sentry's host
	// netns (which equals the worker process's netns under runsc +
	// sentrystack since the OCI spec omits NetworkNamespace).
	fallback *net.Dialer
}

// DialTCP implements tcpDialFunc. Branches on dst:
//
//   - IMDS dst (169.254.169.254:80 or [fd00:ec2::254]:80) → worker
//     IMDS bridge with SandboxID TLV.
//   - Everything else, if egressHostAddr is set → worker egress
//     bridge with SandboxID TLV (worker enriches identity + MITM
//     decision).
//   - Otherwise → direct dial via the Sentry's host netns.
func (d *routedDialer) DialTCP(ctx context.Context, src, dst netip.AddrPort) (net.Conn, error) {
	if d.imdsHostAddr != "" {
		if _, isIMDS := d.imdsTargets[dst]; isIMDS {
			// IMDS dials don't need dst-name (the worker keys on
			// SandboxID, not on hostname), but it's harmless to
			// pass through if the cache happens to bind it.
			return d.dialWithProxyHeader(ctx, d.imdsHostAddr, src, dst, d.dstNameFor(dst), "IMDS")
		}
	}
	if d.egressHostAddr != "" {
		return d.dialWithProxyHeader(ctx, d.egressHostAddr, src, dst, d.dstNameFor(dst), "egress")
	}
	return d.fallback.DialContext(ctx, "tcp", dst.String())
}

// dstNameFor returns the cached qname the agent looked up to reach
// dst's IP, or "" when no live binding exists.
func (d *routedDialer) dstNameFor(dst netip.AddrPort) string {
	if d.dnsCache == nil {
		return ""
	}
	return d.dnsCache.Lookup(dst.Addr())
}

// dialWithProxyHeader dials hostAddr (worker-bound 127.0.0.1 port)
// and writes a PROXY v2 frame announcing the sandbox-visible (src,
// dst) tuple plus the SandboxID + (optional) DstName TLVs. label is
// included in error strings so log readers can tell IMDS-bridge
// dials from egress-bridge dials apart.
func (d *routedDialer) dialWithProxyHeader(ctx context.Context, hostAddr string, src, dst netip.AddrPort, dstName, label string) (net.Conn, error) {
	conn, err := d.fallback.DialContext(ctx, "tcp", hostAddr)
	if err != nil {
		return nil, fmt.Errorf("dial %s host %s: %w", label, hostAddr, err)
	}
	// PROXY v2 requires matching address families. We pass the
	// sandbox-visible tuple verbatim, so v4/v6 dst → v4/v6 src
	// (both eth0 IPs) naturally match.
	hdr, err := proxyproto.EncodeHeader(src, dst, proxyproto.AgentIdentity{
		SandboxID: d.sandboxID,
	}, dstName)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("encode PROXY v2 (%s): %w", label, err)
	}
	if _, err := conn.Write(hdr); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write PROXY v2 (%s): %w", label, err)
	}
	return conn, nil
}

// newRoutedTCPDialer builds the Sentry-side dialer from initStr.
// Empty IMDS/egress fields fall through to direct dial. dnsCache is
// optional; nil disables IP → qname lookup (TLVDstName stays empty).
func newRoutedTCPDialer(init *InitStr, dnsCache *dnsCache) *routedDialer {
	d := &routedDialer{
		sandboxID:      init.SandboxID,
		imdsHostAddr:   init.IMDSHostAddr,
		egressHostAddr: init.EgressHostAddr,
		imdsTargets:    make(map[netip.AddrPort]struct{}),
		dnsCache:       dnsCache,
		fallback:       &net.Dialer{},
	}
	if init.IMDSV4 != "" {
		if ap, err := netip.ParseAddrPort(init.IMDSV4); err == nil {
			d.imdsTargets[ap] = struct{}{}
		}
	}
	if init.IMDSV6 != "" {
		if ap, err := netip.ParseAddrPort(init.IMDSV6); err == nil {
			d.imdsTargets[ap] = struct{}{}
		}
	}
	return d
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

