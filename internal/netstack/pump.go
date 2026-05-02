//go:build linux

package netstack

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
)

// pktPool reduces per-packet allocations for the read/write pump.
var pktPool = sync.Pool{
	New: func() any {
		// MTU only — TUN frames carry bare IP, no Ethernet header.
		b := make([]byte, 1500)
		return &b
	},
}

// PacketPump splices packets between a TUN file descriptor and a gVisor
// channel.Endpoint. The TUN device is assumed to be created with
// IFF_TUN | IFF_NO_PI, meaning each read/write carries a bare IP packet.
type PacketPump struct {
	tapFD *os.File
	ep    *channel.Endpoint
	mtu   uint32

	closed atomic.Bool

	// Coalesced wakeup channel: the endpoint notifies us when it has outbound
	// packets. We batch-drain in the outbound goroutine.
	wakeOutbound chan struct{}
}

// NewPacketPump creates a pump for the given TUN fd and channel endpoint.
func NewPacketPump(tapFD *os.File, ep *channel.Endpoint, mtu uint32) *PacketPump {
	p := &PacketPump{
		tapFD:        tapFD,
		ep:           ep,
		mtu:          mtu,
		wakeOutbound: make(chan struct{}, 1),
	}
	ep.AddNotify(p)
	return p
}

// WriteNotify implements channel.Notification. Called by gVisor when the
// endpoint has an outbound packet ready.
func (p *PacketPump) WriteNotify() {
	if p.closed.Load() {
		return
	}
	select {
	case p.wakeOutbound <- struct{}{}:
	default:
		// Already awake; coalesce.
	}
}

// Run starts the inbound and outbound pump goroutines and blocks until both
// return. Closing the TUN fd or the channel endpoint will cause both to exit.
func (p *PacketPump) Run() error {
	var g errgroup.Group

	// Inbound: TUN fd → channel.Endpoint (netstack).
	g.Go(p.inbound)

	// Outbound: channel.Endpoint → TUN fd.
	g.Go(p.outbound)

	return g.Wait()
}

// Close shuts down the pump by closing the wakeup channel.
func (p *PacketPump) Close() {
	p.closed.Store(true)
	close(p.wakeOutbound)
}

// inbound reads bare IP packets from the TUN fd and injects them into
// the netstack. The IP version is taken from the first byte's high nibble.
func (p *PacketPump) inbound() error {
	for {
		bufp := pktPool.Get().(*[]byte)
		buf := *bufp

		n, err := p.tapFD.Read(buf)
		if err != nil {
			pktPool.Put(bufp)
			// Sandbox teardown closes the TAP fd while inbound is
			// blocked in Read. Treat that as a clean exit instead of
			// noisily logging "Netstack pump exited" on every delete.
			if p.closed.Load() {
				return nil
			}
			return fmt.Errorf("reading from TUN: %w", err)
		}

		if n == 0 {
			pktPool.Put(bufp)
			continue
		}

		ipPayload := buf[:n]
		var proto tcpip.NetworkProtocolNumber
		switch ipPayload[0] >> 4 {
		case 4:
			proto = header.IPv4ProtocolNumber
		case 6:
			proto = header.IPv6ProtocolNumber
		default:
			pktPool.Put(bufp)
			continue
		}

		pkb := stack.NewPacketBuffer(stack.PacketBufferOptions{
			Payload: buffer.MakeWithData(ipPayload),
		})
		p.ep.InjectInbound(proto, pkb)
		pkb.DecRef()

		pktPool.Put(bufp)
	}
}

// outbound drains the channel endpoint and writes bare IP packets to
// the TUN fd.
func (p *PacketPump) outbound() error {
	for {
		_, ok := <-p.wakeOutbound
		if !ok {
			return nil // pump closed
		}

		// Drain all pending packets.
		for {
			pkt := p.ep.Read()
			if pkt == nil {
				break
			}

			view := pkt.ToView()
			pkt.DecRef()

			ipBytes := view.AsSlice()
			if len(ipBytes) == 0 {
				view.Release()
				continue
			}

			if v := ipBytes[0] >> 4; v != 4 && v != 6 {
				slog.Debug("Outbound packet with unknown IP version", slog.Int("version", int(v)))
				view.Release()
				continue
			}

			if _, err := p.tapFD.Write(ipBytes); err != nil {
				slog.Warn("Error writing to TUN", slog.Any("error", err))
			}

			view.Release()
		}
	}
}
