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
// via the provided Dialer. DNS queries (port 53) use single-packet mode.
func UDPForwarder(ctx context.Context, ipstack *stack.Stack, dialer Dialer) ProtocolHandler {
	udpForwarder := udp.NewForwarder(
		ipstack,
		udpHandler(ctx, dialer),
	)
	return udpForwarder.HandlePacket
}

func copyPackets(ctx context.Context, src, dst net.Conn, once bool, extend func()) error {
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

func udpHandler(ctx context.Context, dialer Dialer) udp.ForwarderHandler {
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

			idleTimeout := 30 * time.Second
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
			// For DNS sessions, send one packet each way and then close immediately.
			once := dstAddrPort.Port() == dnsPort

			g, copyCtx := errgroup.WithContext(sCtx)
			g.Go(func() error {
				return copyPackets(copyCtx, downConn, upConn, once, extend)
			})
			g.Go(func() error {
				return copyPackets(copyCtx, upConn, downConn, once, extend)
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
