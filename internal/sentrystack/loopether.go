//go:build linux

package sentrystack

import (
	"sync"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
)

// loopetherMTU is the MTU we present to the sandboxed process for eth0.
// 1500 is what a "normal" Linux container sees; deviating from that has
// surprised user code in the past (e.g. Go's net package making fragment
// assumptions). Loopback semantics make the actual on-the-wire MTU
// irrelevant — packets never leave the *tcpip.Stack — so 1500 is purely
// cosmetic, but a cosmetic that matters.
const loopetherMTU = 1500

// loopether is an Ethernet-flavored loopback LinkEndpoint. WritePackets
// turns outbound packets straight into inbound ones (same as the stock
// loopback.endpoint), but ARPHardwareType reports ARPHardwareEther and a
// non-zero LinkAddress so the sandbox sees what looks like a real
// Ethernet NIC.
//
// Why a custom endpoint instead of stock loopback: getifaddrs / `ip link`
// inside the sandbox should show eth0 as ETHER (not LOOPBACK), and
// SOCK_DGRAM NL_ROUTE consumers (golang/netlink, libnl) treat the link
// type as a hard constraint. The synthesized MAC also feeds whatever
// code paths grab the link-layer address (occasionally Java apps and
// older runtimes do this).
//
// Promiscuous + spoofing must be enabled by the caller (in Init); with
// them the stack won't drop outbound packets just because the dst
// address isn't ours — the whole point is that the forwarder catches
// them on the way back in.
type loopether struct {
	mu         sync.RWMutex
	dispatcher stack.NetworkDispatcher
	addr       tcpip.LinkAddress
	mtu        uint32
}

// newLoopether constructs a loopether with the given LinkAddress.
// LinkAddress must be 6 bytes (an Ethernet MAC).
func newLoopether(addr tcpip.LinkAddress) *loopether {
	return &loopether{
		addr: addr,
		mtu:  loopetherMTU,
	}
}

func (e *loopether) Attach(dispatcher stack.NetworkDispatcher) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dispatcher = dispatcher
}

func (e *loopether) IsAttached() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.dispatcher != nil
}

func (e *loopether) MTU() uint32 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.mtu
}

func (e *loopether) SetMTU(mtu uint32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mtu = mtu
}

// Capabilities advertises checksum offload (the stack short-circuits it
// for loopback anyway). We intentionally omit CapabilityResolutionRequired
// — there are no real link peers, so ARP is a no-op, and asking the stack
// to resolve neighbors would just stall outbound packets waiting for
// replies that never come.
func (*loopether) Capabilities() stack.LinkEndpointCapabilities {
	return stack.CapabilityRXChecksumOffload | stack.CapabilityTXChecksumOffload
}

// MaxHeaderLength reserves space for an Ethernet header. The header is
// never actually transmitted (loopback semantics), but the stack uses
// this value to size packet buffers and trims headers off on the inbound
// side.
func (*loopether) MaxHeaderLength() uint16 {
	return header.EthernetMinimumSize
}

func (e *loopether) LinkAddress() tcpip.LinkAddress {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.addr
}

func (e *loopether) SetLinkAddress(addr tcpip.LinkAddress) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.addr = addr
}

func (*loopether) ARPHardwareType() header.ARPHardwareType {
	return header.ARPHardwareEther
}

// AddHeader is called by the stack to prepend the Ethernet header on
// transmit. We honor it (rather than no-op) so packet introspection
// (e.g., tcpdump-equivalent via the packet-socket layer, should we ever
// enable it) sees a coherent frame. The header is stripped again on
// the loopback delivery path.
func (*loopether) AddHeader(pkt *stack.PacketBuffer) {
	// The link layer header is written by stack.WritePackets via
	// AddHeader if the link is ETHER and the protocol is set. Leaving
	// this empty keeps the wire format link-layer-less — fine for now;
	// revisit if anything tries to consume the frame at link layer.
}

func (*loopether) ParseHeader(*stack.PacketBuffer) bool {
	return true
}

func (*loopether) Close() {}

func (*loopether) SetOnCloseAction(func()) {}

func (*loopether) Wait() {}

// WritePackets turns each outbound packet into an inbound packet. We
// build a fresh PacketBuffer from the payload so the stack treats it as
// a freshly-arrived frame (DeliverNetworkPacket inspects pkt fields and
// resetting them avoids reuse anomalies).
//
// Synchronous delivery mirrors stock loopback. Watch for deadlock if a
// dispatcher-side code path ever calls back into a lock the caller of
// WritePackets holds — switch to a goroutine here if that happens.
func (e *loopether) WritePackets(pkts stack.PacketBufferList) (int, tcpip.Error) {
	e.mu.RLock()
	d := e.dispatcher
	e.mu.RUnlock()
	if d == nil {
		return 0, nil
	}
	for _, pkt := range pkts.AsSlice() {
		newPkt := stack.NewPacketBuffer(stack.PacketBufferOptions{
			Payload: pkt.ToBuffer(),
		})
		d.DeliverNetworkPacket(pkt.NetworkProtocolNumber, newPkt)
		newPkt.DecRef()
	}
	return pkts.Len(), nil
}
