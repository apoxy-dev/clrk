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

func udpHandler(ctx context.Context, dialer Dialer) func(req *udp.ForwarderRequest) {
	return func(req *udp.ForwarderRequest) {
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
				logger.Error("Failed to create endpoint", slog.String("error", tcpipErr.String()))
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
	}
}
