//go:build linux

package netstack

import (
	"context"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
	"gvisor.dev/gvisor/pkg/waiter"
)

const dnsPort = 53

var udpBuffPool = sync.Pool{
	New: func() any {
		b := make([]byte, 65536)
		return &b
	},
}

// UDPForwarder intercepts all UDP packets and forwards them to the upstream
// via the provided Dialer.
//
// When dnsCache is non-nil, upstream → sandbox traffic on UDP/53 is
// snooped: responses are parsed and any A/AAAA answers are bound to
// their qname/CNAME-aliases in the cache, so subsequent TCP dials can
// surface the agent's stated intent (the resolved hostname) on the
// PROXY v2 frame and to L4 telemetry. Snoop is observe-only — bytes
// reach the sandbox unchanged regardless of parse success.
func UDPForwarder(ctx context.Context, ipstack *stack.Stack, dialer Dialer, dnsCache *DNSCache) ProtocolHandler {
	udpForwarder := udp.NewForwarder(
		ipstack,
		udpHandler(ctx, dialer, dnsCache),
	)
	return udpForwarder.HandlePacket
}

func copyPackets(ctx context.Context, src, dst net.Conn, once bool, extend func(), tap func([]byte)) error {
	buf := udpBuffPool.Get().(*[]byte)
	pkt := (*buf)[:cap(*buf)]
	defer udpBuffPool.Put(buf)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			n, err := src.Read(pkt)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			if tap != nil && n > 0 {
				tap(pkt[:n])
			}
			if _, err := dst.Write(pkt[:n]); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			if once {
				return nil
			}
			extend()
		}
	}
}

func udpHandler(ctx context.Context, dialer Dialer, dnsCache *DNSCache) udp.ForwarderHandler {
	return func(req *udp.ForwarderRequest) bool {
		reqDetails := req.ID()

		srcAddrPort := netip.AddrPortFrom(addrFromNetstackIP(reqDetails.RemoteAddress), reqDetails.RemotePort)
		dstAddrPort := netip.AddrPortFrom(
			Unmap4in6(addrFromNetstackIP(reqDetails.LocalAddress)),
			reqDetails.LocalPort,
		)

		logger := slog.With(
			slog.String("src", srcAddrPort.String()),
			slog.String("dst", dstAddrPort.String()),
		)

		logger.Debug("Forwarding UDP session")

		go func() {
			sCtx, cancel := context.WithCancel(ctx)

			var wq waiter.Queue
			ep, tcpipErr := req.CreateEndpoint(&wq)
			if tcpipErr != nil {
				// gVisor's UDP forwarder dispatches per-packet handlers
				// without a 5-tuple session table; concurrent packets on
				// the same flow (DNS retransmits with a connect()ed
				// socket are the most common case) all race to bind the
				// same local port. The losing handler returns ErrPortInUse
				// and drops the packet. The original packet's session is
				// already in flight and DNS will retransmit if needed, so
				// this is benign — log debug instead of error.
				if _, isPortInUse := tcpipErr.(*tcpip.ErrPortInUse); isPortInUse {
					logger.Debug("Skipping duplicate UDP flow", slog.String("error", tcpipErr.String()))
				} else {
					logger.Error("Failed to create endpoint", slog.String("error", tcpipErr.String()))
				}
				cancel()
				return
			}

			downConn := gonet.NewUDPConn(&wq, ep)
			upConn, err := dialer.DialContext(sCtx, "udp", dstAddrPort.String())
			if err != nil {
				logger.Error("Failed to dial upstream", slog.Any("error", err))
				cancel()
				downConn.Close()
				return
			}

			// DNS gets a short idle timeout so the per-flow session can
			// carry multiple back-to-back queries (musl fires A and AAAA
			// in parallel on the same socket) before being torn down.
			isDNS := dstAddrPort.Port() == dnsPort
			idleTimeout := 30 * time.Second
			if isDNS {
				idleTimeout = 2 * time.Second
			}
			timer := time.AfterFunc(idleTimeout, func() {
				logger.Debug("Idle timeout reached")
				cancel()
				downConn.Close()
				upConn.Close()
			})
			cleanup := func() {
				cancel()
				downConn.Close()
				upConn.Close()
				timer.Stop()
			}
			extend := func() {
				timer.Reset(idleTimeout)
			}

			// Tap DNS responses (upstream → sandbox direction) into the
			// cache so TCP connect lookups can surface the resolved name.
			var responseTap func([]byte)
			if isDNS && dnsCache != nil {
				responseTap = dnsCache.IngestResponse
			}

			// Both directions stay open until the idle timer fires so a
			// single UDP flow can carry musl's parallel A+AAAA pair (and
			// any retransmits) without one of the responses being dropped.
			g, copyCtx := errgroup.WithContext(sCtx)
			g.Go(func() error {
				return copyPackets(copyCtx, downConn, upConn, false, extend, nil)
			})
			g.Go(func() error {
				return copyPackets(copyCtx, upConn, downConn, false, extend, responseTap)
			})

			if err := g.Wait(); err != nil {
				logger.Error("Failed to copy packets", slog.Any("error", err))
			}
			cleanup()
			logger.Debug("UDP forwarding complete")
		}()
		// The session is handled asynchronously above; tell the forwarder
		// to consider this packet handled so it isn't resent to other handlers.
		return true
	}
}
