// APO-725 spike scaffolding: a prototype in-Sentry overlay LinkEndpoint
// ("geneve0") that encapsulates guest L3 packets with icx (Geneve VNI +
// AES-128-GCM) and pumps them over a plain UDP socket — the exact I/O
// shape the plugin seccomp filter already permits (AF_INET6/SOCK_DGRAM
// sendto/recvfrom, no GSO, no sendmmsg). The return path re-wraps each
// received datagram in a synthetic outer Ethernet+IPv6+UDP frame (icx's
// Handler API works on full phy frames, mirroring its AF_XDP deployments;
// see apoxy pkg/tunnel/l2pc for the production precedent) and injects the
// de-encapped inner packet into the stack via DeliverNetworkPacket — the
// load-bearing mechanism this spike exists to prove.
//
// This file is intentionally NOT //go:build linux constrained: like
// inbound_demux_test.go (APO-694) it uses only gVisor's portable pkg/tcpip
// plus the portable icx core, so it compiles and runs on the developer's
// macOS host as well as in CI. The production endpoint (APO-727) will live
// in a linux-tagged file next to loopether.go and reuse this shape with
// zero-copy views instead of the Flatten copies used here.
package sentrystack

import (
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"

	"github.com/apoxy-dev/icx"
	icxudp "github.com/apoxy-dev/icx/udp"
	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
)

// overlayNICID is the NIC the overlay endpoint attaches as. Production
// reserves 1 (lo) and 2 (eth0); the geneve0 NIC is the third.
const overlayNICID tcpip.NICID = 3

// Inner-MTU clamp math. icx has no PTB/PMTUD machinery, so the build uses a
// static clamp; this spike validates the number empirically. Per-packet
// overhead between the inner IP packet and the outer UDP payload is exactly
// geneve 32 (8 fixed + 8 key-epoch option + 16 tx-counter option) + 16
// (AES-GCM tag) = 48 bytes; the outer IPv6+UDP headers add another 48.
// icx.MTU additionally rounds down to a whole AES block, which is where the
// fleet-advertised 1392 (vs the raw 1404) comes from. TestOverlayEncapMath
// pins all of this against the imported icx version so a header-layout or
// MTU-helper regression (v0.17.0 shipped one) fails loudly here.
const (
	spikePathMTU      = 1500
	aesGCMTagSize     = 16
	encapOverhead     = 32 + aesGCMTagSize // Geneve header + AEAD tag
	outerV6Overhead   = header.IPv6MinimumSize + header.UDPMinimumSize
	spikeInnerMTU     = 1392 // == icx.MTU(spikePathMTU), asserted in tests
	maxOuterUDPLen    = spikeInnerMTU + encapOverhead
	underlayUDPBudget = spikePathMTU - outerV6Overhead // what fits in one underlay datagram
)

// geneveEndpoint is the prototype overlay LinkEndpoint. TX: outbound L3
// packets routed at this NIC are sealed by icx.Handler.VirtToPhy and written
// to the underlay socket as plain UDP datagrams (per-packet sendto — exactly
// the syscall budget the Sentry will pay). RX: a pump goroutine recvfroms,
// rebuilds the outer frame PhyToVirt expects, and delivers the decrypted
// inner packet to the stack's dispatcher.
type geneveEndpoint struct {
	mu         sync.RWMutex
	dispatcher stack.NetworkDispatcher

	mtu     uint32
	handler *icx.Handler
	conn    *net.UDPConn
	localFA *tcpip.FullAddress

	// txMu serializes VirtToPhy into the shared scratch buffer. Multiple
	// guest endpoints can reach WritePackets concurrently.
	txMu  sync.Mutex
	txPhy []byte

	wg     sync.WaitGroup
	closed atomic.Bool

	// Spike instrumentation, read by the MTU/fragmentation assertions.
	txDatagrams   atomic.Int64 // datagrams put on the underlay
	maxTxDatagram atomic.Int64 // largest underlay UDP payload observed
	txDrops       atomic.Int64 // packets icx declined or socket write failures
	rxDelivered   atomic.Int64 // inner packets injected via DeliverNetworkPacket
	rxDecapDrops  atomic.Int64 // datagrams PhyToVirt rejected
}

// newGeneveEndpoint wraps an already-bound underlay socket. The handler must
// be configured with WithLayer3VirtFrames and a local addr matching conn's.
func newGeneveEndpoint(h *icx.Handler, conn *net.UDPConn, mtu uint32) (*geneveEndpoint, error) {
	la, err := localFullAddress(conn)
	if err != nil {
		return nil, err
	}
	return &geneveEndpoint{
		mtu:     mtu,
		handler: h,
		conn:    conn,
		localFA: la,
		// VirtToPhy/PhyToVirt write through append-at-zero subslices; an
		// undersized buffer makes crypto/cipher reallocate and the output
		// silently lands outside the buffer the returned length refers to.
		// Size generously.
		txPhy: make([]byte, 64<<10),
	}, nil
}

// runRX starts the receive pump. Call after the endpoint is attached to a
// NIC (CreateNIC); packets that arrive before attach are dropped.
func (e *geneveEndpoint) runRX() {
	e.wg.Add(1)
	go e.rxLoop()
}

// shutdown closes the underlay socket and joins the pump. Safe to call more
// than once (stack teardown and test cleanup can race onto it).
func (e *geneveEndpoint) shutdown() {
	if e.closed.CompareAndSwap(false, true) {
		_ = e.conn.Close()
		e.wg.Wait()
	}
}

// --- stack.LinkEndpoint ---

func (e *geneveEndpoint) Attach(dispatcher stack.NetworkDispatcher) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dispatcher = dispatcher
}

func (e *geneveEndpoint) IsAttached() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.dispatcher != nil
}

func (e *geneveEndpoint) MTU() uint32 { return e.mtu }

func (e *geneveEndpoint) SetMTU(mtu uint32) { e.mtu = mtu }

// Capabilities claims checksum offload in both directions: the AEAD tag
// authenticates every inner byte across the only real wire, so recomputing
// transport checksums on either side of the tunnel buys nothing. This is
// the intended production posture too. No CapabilityLoopback — this is a
// real exit NIC, not a loopback.
func (*geneveEndpoint) Capabilities() stack.LinkEndpointCapabilities {
	return stack.CapabilityRXChecksumOffload | stack.CapabilityTXChecksumOffload
}

// MaxHeaderLength is 0: the endpoint consumes raw L3 packets (icx L3 mode);
// all encap framing happens in VirtToPhy's own buffer.
func (*geneveEndpoint) MaxHeaderLength() uint16 { return 0 }

func (*geneveEndpoint) LinkAddress() tcpip.LinkAddress { return "" }

func (*geneveEndpoint) SetLinkAddress(tcpip.LinkAddress) {}

func (*geneveEndpoint) ARPHardwareType() header.ARPHardwareType {
	return header.ARPHardwareNone
}

func (*geneveEndpoint) AddHeader(*stack.PacketBuffer) {}

func (*geneveEndpoint) ParseHeader(*stack.PacketBuffer) bool { return true }

func (e *geneveEndpoint) Close() { e.shutdown() }

func (*geneveEndpoint) SetOnCloseAction(func()) {}

func (e *geneveEndpoint) Wait() { e.wg.Wait() }

// WritePackets seals and emits each outbound packet. Drops are counted, not
// errored: a packet icx declines (no vnet/key for the destination) must not
// wedge the whole batch — same semantics as a real NIC dropping on a full
// ring.
func (e *geneveEndpoint) WritePackets(pkts stack.PacketBufferList) (int, tcpip.Error) {
	n := 0
	for _, pkt := range pkts.AsSlice() {
		if e.writePacket(pkt) {
			n++
		}
	}
	return n, nil
}

func (e *geneveEndpoint) writePacket(pkt *stack.PacketBuffer) bool {
	// Flatten copies; the production endpoint will feed views to avoid
	// this, but the spike keeps it simple (and the bench measures the
	// realistic floor anyway: seal + syscall dominate).
	b := pkt.ToBuffer()
	inner := b.Flatten()
	b.Release()

	e.txMu.Lock()
	defer e.txMu.Unlock()

	frameLen, handledLocally := e.handler.VirtToPhy(inner, e.txPhy)
	if handledLocally {
		// L3 mode has no ARP/ND proxying; nothing should land here.
		return true
	}
	if frameLen == 0 {
		e.txDrops.Add(1)
		return false
	}
	payload, dst, err := outerUDPPayloadAndDst(e.txPhy[:frameLen])
	if err != nil {
		e.txDrops.Add(1)
		return false
	}
	if l := int64(len(payload)); l > e.maxTxDatagram.Load() {
		e.maxTxDatagram.Store(l) // only written under txMu
	}
	if _, err := e.conn.WriteToUDPAddrPort(payload, dst); err != nil {
		e.txDrops.Add(1)
		return false
	}
	e.txDatagrams.Add(1)
	return true
}

// rxLoop is the return-path pump: recvfrom → synthesize the outer frame →
// PhyToVirt (AEAD open + VNI/src validation) → DeliverNetworkPacket. This
// loop IS the per-packet cost the Sentry pays on the return path.
func (e *geneveEndpoint) rxLoop() {
	defer e.wg.Done()
	rx := make([]byte, 64<<10)
	frame := make([]byte, 64<<10)
	virt := make([]byte, 64<<10)
	for {
		n, raddr, err := e.conn.ReadFromUDPAddrPort(rx)
		if err != nil {
			return // socket closed
		}
		frameLen, err := synthOuterFrame(frame, rx[:n], raddr, e.localFA)
		if err != nil {
			e.rxDecapDrops.Add(1)
			continue
		}
		vLen := e.handler.PhyToVirt(frame[:frameLen], virt)
		if vLen == 0 {
			e.rxDecapDrops.Add(1)
			continue
		}
		if e.deliver(virt[:vLen]) {
			e.rxDelivered.Add(1)
		}
	}
}

// deliver injects one decrypted inner L3 packet into the stack — the
// APO-725 proof point. The dispatcher demuxes it like any freshly-arrived
// frame: to a listening endpoint, an ESTABLISHED endpoint, or (neither) the
// catch-all forwarder.
func (e *geneveEndpoint) deliver(ipPacket []byte) bool {
	e.mu.RLock()
	d := e.dispatcher
	e.mu.RUnlock()
	if d == nil || len(ipPacket) == 0 {
		return false
	}
	var proto tcpip.NetworkProtocolNumber
	switch ipPacket[0] >> 4 {
	case header.IPv4Version:
		proto = header.IPv4ProtocolNumber
	case header.IPv6Version:
		proto = header.IPv6ProtocolNumber
	default:
		return false
	}
	data := make([]byte, len(ipPacket))
	copy(data, ipPacket)
	pkt := stack.NewPacketBuffer(stack.PacketBufferOptions{
		Payload: buffer.MakeWithData(data),
	})
	d.DeliverNetworkPacket(proto, pkt)
	pkt.DecRef()
	return true
}

// outerUDPPayloadAndDst extracts the Geneve payload and underlay destination
// from a VirtToPhy output frame (synthetic Ethernet + IP + UDP). The Sentry
// sends only the payload; the kernel rebuilds the real outer headers.
func outerUDPPayloadAndDst(frame []byte) ([]byte, netip.AddrPort, error) {
	if len(frame) < header.EthernetMinimumSize {
		return nil, netip.AddrPort{}, fmt.Errorf("frame too short: %d", len(frame))
	}
	eth := header.Ethernet(frame)
	switch eth.Type() {
	case header.IPv4ProtocolNumber:
		ip := header.IPv4(frame[header.EthernetMinimumSize:])
		if !ip.IsValid(len(ip)) || ip.Protocol() != uint8(header.UDPProtocolNumber) {
			return nil, netip.AddrPort{}, fmt.Errorf("not an IPv4 UDP frame")
		}
		u := header.UDP(ip.Payload())
		if len(u) < header.UDPMinimumSize {
			return nil, netip.AddrPort{}, fmt.Errorf("UDP header truncated")
		}
		dst, _ := netip.AddrFromSlice(ip.DestinationAddressSlice())
		return u.Payload(), netip.AddrPortFrom(dst, u.DestinationPort()), nil
	case header.IPv6ProtocolNumber:
		ip := header.IPv6(frame[header.EthernetMinimumSize:])
		if !ip.IsValid(len(ip)) || ip.TransportProtocol() != header.UDPProtocolNumber {
			return nil, netip.AddrPort{}, fmt.Errorf("not an IPv6 UDP frame")
		}
		u := header.UDP(ip.Payload())
		if len(u) < header.UDPMinimumSize {
			return nil, netip.AddrPort{}, fmt.Errorf("UDP header truncated")
		}
		dst, _ := netip.AddrFromSlice(ip.DestinationAddressSlice())
		return u.Payload(), netip.AddrPortFrom(dst, u.DestinationPort()), nil
	default:
		return nil, netip.AddrPort{}, fmt.Errorf("unsupported ethertype %d", eth.Type())
	}
}

// synthOuterFrame rebuilds the outer Ethernet+IP+UDP framing around a
// received underlay datagram so PhyToVirt can parse it. src is the sender's
// underlay 4-tuple from recvfrom; checksums are skipped (PhyToVirt skips
// validation — integrity is the AEAD's job).
func synthOuterFrame(dst []byte, payload []byte, raddr netip.AddrPort, local *tcpip.FullAddress) (int, error) {
	src := &tcpip.FullAddress{Port: raddr.Port()}
	var off int
	a := raddr.Addr()
	if a.Is4() || a.Is4In6() {
		off = icxudp.PayloadOffsetIPv4
		src.Addr = tcpip.AddrFrom4(a.Unmap().As4())
	} else {
		off = icxudp.PayloadOffsetIPv6
		src.Addr = tcpip.AddrFrom16(a.As16())
	}
	if off+len(payload) > len(dst) {
		return 0, fmt.Errorf("datagram too large: %d", len(payload))
	}
	copy(dst[off:], payload)
	return icxudp.Encode(dst, src, local, len(payload), true)
}

// localFullAddress converts the socket's bound address for icx (used both as
// the handler's WithLocalAddr and as the synthetic outer dst on RX).
func localFullAddress(conn *net.UDPConn) (*tcpip.FullAddress, error) {
	ua, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok || ua == nil {
		return nil, fmt.Errorf("underlay socket must be UDP")
	}
	a, ok := netip.AddrFromSlice(ua.IP)
	if !ok {
		return nil, fmt.Errorf("invalid local address %v", ua.IP)
	}
	fa := &tcpip.FullAddress{Port: uint16(ua.Port)}
	if a.Is4() || a.Is4In6() {
		fa.Addr = tcpip.AddrFrom4(a.Unmap().As4())
	} else {
		fa.Addr = tcpip.AddrFrom16(a.As16())
	}
	return fa, nil
}
