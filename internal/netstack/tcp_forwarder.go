//go:build linux

package netstack

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/netip"

	"github.com/dpeckett/contextio"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/waiter"
)

// ProtocolHandler is a function that handles packets for a specific protocol.
type ProtocolHandler func(stack.TransportEndpointID, *stack.PacketBuffer) bool

// Dialer abstracts upstream connection establishment for the TCP forwarder.
// The EgressRouter implements this to apply routing decisions.
type Dialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
}

// TCPForwarder intercepts all TCP SYN packets and forwards established
// connections to the upstream via the provided Dialer.
func TCPForwarder(ctx context.Context, ipstack *stack.Stack, dialer Dialer) ProtocolHandler {
	tcpForwarder := tcp.NewForwarder(
		ipstack,
		0,     // rcvWnd (0 = default)
		65535, // maxInFlight
		tcpHandler(ctx, dialer),
	)
	return tcpForwarder.HandlePacket
}

func tcpHandler(ctx context.Context, dialer Dialer) func(req *tcp.ForwarderRequest) {
	return func(req *tcp.ForwarderRequest) {
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

		logger.Info("Forwarding TCP session")

		go func() {
			defer logger.Debug("Session finished")

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			var wq waiter.Queue
			ep, tcpipErr := req.CreateEndpoint(&wq)
			if tcpipErr != nil {
				logger.Warn("Failed to create local endpoint",
					slog.String("error", tcpipErr.String()))
				req.Complete(true) // send RST
				return
			}

			// Cancel the context when the connection is closed.
			waitEntry, notifyCh := waiter.NewChannelEntry(waiter.EventHUp)
			wq.EventRegister(&waitEntry)
			defer wq.EventUnregister(&waitEntry)

			go func() {
				select {
				case <-ctx.Done():
				case <-notifyCh:
					logger.Debug("Connection hangup, canceling context")
					cancel()
				}
			}()

			// Disable Nagle's algorithm.
			ep.SocketOptions().SetDelayOption(false)
			// Enable keep-alive to detect dead connections.
			ep.SocketOptions().SetKeepAlive(true)

			local := gonet.NewTCPConn(&wq, ep)
			defer local.Close()

			// Connect to the destination.
			remote, err := dialer.DialContext(ctx, "tcp", dstAddrPort.String())
			if err != nil {
				logger.Warn("Failed to dial destination", slog.Any("error", err))
				req.Complete(true) // send RST
				return
			}
			defer remote.Close()

			logger.Info("Connected to upstream")

			// Splice bidirectionally.
			wn, err := contextio.SpliceContext(ctx, local, remote, nil)
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.Warn("Failed to forward session", slog.Any("error", err))
				req.Complete(true) // send RST
				return
			}
			logger.Info("Connection closed", slog.Int64("bytes_written", wn))

			req.Complete(false) // send FIN
		}()
	}
}
