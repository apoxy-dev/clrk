//go:build linux

package netstack

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
)

// ethernetHdrLen is the standard Ethernet header size (dst + src + ethertype).
const ethernetHdrLen = 14

// pktPool reduces per-packet allocations for the read/write pump.
var pktPool = sync.Pool{
	New: func() any {
		// 14 (eth) + 1500 (MTU) — enough for any standard frame.
		b := make([]byte, ethernetHdrLen+1500)
		return &b
	},
}

// PacketPump splices packets between a TAP file descriptor and a gVisor
// channel.Endpoint. The TAP device is assumed to be created with
// IFF_TAP | IFF_NO_PI (no IFF_VNET_HDR), meaning each read/write
// carries a plain Ethernet frame with no virtio metadata.
type PacketPump struct {
	tapFD *os.File
	ep    *channel.Endpoint
	mtu   uint32

	closed atomic.Bool

	// Coalesced wakeup channel: the endpoint notifies us when it has outbound
	// packets. We batch-drain in the outbound goroutine.
	wakeOutbound chan struct{}
}

// NewPacketPump creates a pump for the given TAP fd and channel endpoint.
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
// return. Closing the TAP fd or the channel endpoint will cause both to exit.
func (p *PacketPump) Run() error {
	var g errgroup.Group

	// Inbound: TAP fd → channel.Endpoint (netstack).
	g.Go(p.inbound)

	// Outbound: channel.Endpoint → TAP fd.
	g.Go(p.outbound)

	return g.Wait()
}

// Close shuts down the pump by closing the wakeup channel.
func (p *PacketPump) Close() {
	p.closed.Store(true)
	close(p.wakeOutbound)
}

// inbound reads Ethernet frames from the TAP fd, strips the Ethernet
// header, and injects the IP payload into the netstack.
func (p *PacketPump) inbound() error {
	for {
		bufp := pktPool.Get().(*[]byte)
		buf := *bufp

		n, err := p.tapFD.Read(buf)
		if err != nil {
			pktPool.Put(bufp)
			return fmt.Errorf("reading from TAP: %w", err)
		}

		// Need at least an Ethernet header.
		if n < ethernetHdrLen {
			pktPool.Put(bufp)
			continue
		}

		ethFrame := buf[:n]

		// Parse ethertype from the Ethernet header to determine IP version.
		ethertype := binary.BigEndian.Uint16(ethFrame[12:14])
		ipPayload := ethFrame[ethernetHdrLen:]

		switch ethertype {
		case uint16(header.IPv4ProtocolNumber):
			pkb := stack.NewPacketBuffer(stack.PacketBufferOptions{
				Payload: buffer.MakeWithData(ipPayload),
			})
			p.ep.InjectInbound(header.IPv4ProtocolNumber, pkb)
			pkb.DecRef()
		case uint16(header.IPv6ProtocolNumber):
			pkb := stack.NewPacketBuffer(stack.PacketBufferOptions{
				Payload: buffer.MakeWithData(ipPayload),
			})
			p.ep.InjectInbound(header.IPv6ProtocolNumber, pkb)
			pkb.DecRef()
		default:
			// ARP, etc. — drop silently. The sandbox uses the TAP as a
			// point-to-point link; ARP is not needed.
		}

		pktPool.Put(bufp)
	}
}

// outbound drains the channel endpoint and writes Ethernet frames to
// the TAP fd.
func (p *PacketPump) outbound() error {
	// Scratch buffer for the Ethernet header prefix. Per-packet fields:
	//   - dst MAC: kernel's TAP MAC (we ARP'd it, but really any
	//     non-broadcast MAC works since the kernel always accepts
	//     frames with broadcast dst — we use the static-neighbor MAC
	//     to mirror what the sandbox's outbound frames carry).
	//   - src MAC: a stable locally-administered MAC for the gateway.
	//   - ethertype: filled per-packet from the IP version.
	prefix := make([]byte, ethernetHdrLen)
	// dst MAC = ff:ff:ff:ff:ff:ff (broadcast — the sandbox kernel
	// always accepts broadcast frames at L2 regardless of promisc).
	for i := 0; i < 6; i++ {
		prefix[i] = 0xff
	}
	// src MAC = 02:00:00:00:00:01 (locally-administered, matches the
	// static neighbor entry installed in the sandbox netns for the
	// gateway IP).
	prefix[6] = 0x02
	prefix[11] = 0x01

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

			// Determine ethertype from the IP version nibble.
			var ethertype uint16
			switch ipBytes[0] >> 4 {
			case 4:
				ethertype = uint16(header.IPv4ProtocolNumber)
			case 6:
				ethertype = uint16(header.IPv6ProtocolNumber)
			default:
				slog.Debug("Outbound packet with unknown IP version", slog.Int("version", int(ipBytes[0]>>4)))
				view.Release()
				continue
			}

			// Fill ethertype and assemble the frame.
			binary.BigEndian.PutUint16(prefix[12:], ethertype)

			frame := make([]byte, 0, len(prefix)+len(ipBytes))
			frame = append(frame, prefix...)
			frame = append(frame, ipBytes...)

			view.Release()

			if _, err := p.tapFD.Write(frame); err != nil {
				slog.Warn("Error writing to TAP", slog.Any("error", err))
			}
		}
	}
}
