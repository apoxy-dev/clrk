// APO-725: the mode-3 (variant 3c) gating spike. Before building the
// production geneve0 VTEP (APO-727), prove on the real pkg/tcpip stack —
// portable, no sentry, no privileges, runs on macOS like the APO-694 demux
// proof — that:
//
//  1. A de-encapped Geneve inner IPv6 packet injected via
//     DeliverNetworkPacket is demuxed into BOTH a listening endpoint and an
//     already-ESTABLISHED endpoint, so workerd sees the real ULA peer and
//     return traffic for guest-originated overlay connections actually
//     lands (TestOverlayEgressTCPRoundtrip, TestOverlayUDPRoundtrip).
//  2. The route-divert seam works: a more-specific overlay route placed
//     above the default→loopether→forwarder route steals exactly the
//     overlay-destined traffic and nothing else (the same test's negative
//     leg: a non-overlay dial still falls through to the catch-all
//     forwarder).
//  3. The static inner-MTU clamp is sound: TCP MSS derives from the geneve0
//     NIC MTU so no overlay datagram ever exceeds the underlay budget, and
//     oversized UDP datagrams source-fragment at the inner layer and
//     reassemble on the far side (TestOverlayInnerMTUClampTCP,
//     TestOverlayUDPSourceFragmentation).
//  4. The encap arithmetic the clamp rests on (Geneve 32 + GCM tag 16, and
//     icx.MTU's AES-block rounding → 1392 @ 1500) holds for the icx version
//     actually imported (TestOverlayEncapMath — a canary: v0.17.0 shipped a
//     broken MTU helper that over-estimates by 24 bytes).
//  5. Garbage, tampered, replayed, and unknown-VNI datagrams are rejected
//     at decap without reaching the stack (TestOverlayDecapRejects).
//
// Topology per test: two in-process netstacks joined by real UDP sockets
// over ::1 — side A is "the Sentry" (sandbox /128, divert route, decoy
// default route + counting forwarder), side B plays tunnelproxy + the
// customer-VPC private endpoint (peer /128, mirrored keys). Addressing
// follows the infra ULA scheme (fd61:706f:7879:nnnn:nn00:eeee::/96 — see
// apoxy-cli pkg/tunnel/net/ula.go): Network 0x000001 → /72
// fd61:706f:7879:0000:0100::/72, endpoints 0x0001/0x0002 beneath it. The
// VNI is per-sandbox (relay convention; VNI=NetworkID is retired — see
// docs/workerd-egress-encap.md §5 in apoxy-cloud).
package sentrystack

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apoxy-dev/icx"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/loopback"
	"gvisor.dev/gvisor/pkg/tcpip/network/arp"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/icmp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
)

const (
	// overlayVNI is the spike sandbox's VNI. Per-sandbox, not per-Network.
	overlayVNI = 0x101
	// overlayKeyEpoch is the single static key generation both sides install.
	overlayKeyEpoch = 1

	// decoyNICID is side A's stand-in for the production eth0/loopether NIC
	// that the default route points at (the egress-bridge path).
	decoyNICID tcpip.NICID = 2

	// spikeForwarderMaxInFlight mirrors tcpForwarderMaxInFlight from
	// forwarder.go, which is linux-tagged and invisible to this portable
	// file.
	spikeForwarderMaxInFlight = 65535
)

var (
	// Network 0x000001's /72 — every overlay address lives inside it.
	overlayNetworkPrefix = netip.MustParsePrefix("fd61:706f:7879:0000:0100::/72")
	// Side A: sandbox /128 under endpoint 0x0001.
	sandboxAddr = netip.MustParseAddr("fd61:706f:7879:0000:0100:0001::2")
	// Side B: the "customer-VPC private endpoint" under endpoint 0x0002.
	vpcPeerAddr = netip.MustParseAddr("fd61:706f:7879:0000:0100:0002::1")
	// Side A's decoy eth0 address (non-overlay ULA) and a non-overlay
	// destination used to prove the divert leaks nothing.
	decoyAddr      = netip.MustParseAddr("fd00:beef::2")
	nonOverlayDst  = netip.MustParseAddr("fd00:beef::99")
	nonOverlayPort = uint16(443)
)

// overlayNode is one side of the spike topology.
type overlayNode struct {
	stk     *stack.Stack
	ep      *geneveEndpoint
	handler *icx.Handler
	conn    *net.UDPConn
	addr    netip.Addr

	// forwarderSYNs counts TCP SYNs that fell through to the catch-all
	// forwarder (side A only). Overlay traffic must never land here.
	forwarderSYNs atomic.Int64
}

func (n *overlayNode) underlay() netip.AddrPort {
	return n.conn.LocalAddr().(*net.UDPAddr).AddrPort()
}

// newOverlayStack mirrors the production newStack() options exactly.
func newOverlayStack() *stack.Stack {
	return stack.New(stack.Options{
		NetworkProtocols: []stack.NetworkProtocolFactory{
			ipv4.NewProtocol,
			ipv6.NewProtocol,
			arp.NewProtocol,
		},
		TransportProtocols: []stack.TransportProtocolFactory{
			tcp.NewProtocol,
			udp.NewProtocol,
			icmp.NewProtocol4,
			icmp.NewProtocol6,
		},
		HandleLocal: true,
	})
}

// buildOverlayNode assembles one side: underlay socket, icx handler, stack,
// geneve0 NIC with the node's /128, and the overlay /72 route. withDecoy
// additionally wires side A's production-shaped egress path: a loopback NIC
// standing in for eth0/loopether, a default route pointing at it, and the
// catch-all counting forwarder — with the overlay route placed FIRST, since
// gVisor walks the route table in order and the divert depends on it.
func buildOverlayNode(t testing.TB, addr netip.Addr, withDecoy bool) *overlayNode {
	t.Helper()

	conn, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback, Port: 0})
	if err != nil {
		t.Fatalf("binding underlay socket: %v", err)
	}
	la, err := localFullAddress(conn)
	if err != nil {
		t.Fatalf("local underlay address: %v", err)
	}
	h, err := icx.NewHandler(
		icx.WithLayer3VirtFrames(),
		icx.WithLocalAddr(la),
	)
	if err != nil {
		t.Fatalf("creating icx handler: %v", err)
	}

	s := newOverlayStack()
	ep, err := newGeneveEndpoint(h, conn, spikeInnerMTU)
	if err != nil {
		t.Fatalf("creating geneve endpoint: %v", err)
	}
	if err := s.CreateNICWithOptions(overlayNICID, ep, stack.NICOptions{Name: "geneve0"}); err != nil {
		t.Fatalf("creating geneve0 NIC: %s", err)
	}
	if err := s.AddProtocolAddress(overlayNICID, tcpip.ProtocolAddress{
		Protocol: ipv6.ProtocolNumber,
		AddressWithPrefix: tcpip.AddressWithPrefix{
			Address:   tcpip.AddrFrom16(addr.As16()),
			PrefixLen: 128,
		},
	}, stack.AddressProperties{}); err != nil {
		t.Fatalf("adding overlay address: %s", err)
	}

	routes := []tcpip.Route{
		// THE DIVERT: the overlay /72 must precede any default route.
		{Destination: mustSubnet(t, overlayNetworkPrefix), NIC: overlayNICID},
	}

	node := &overlayNode{stk: s, ep: ep, handler: h, conn: conn, addr: addr}

	if withDecoy {
		// Production shape: default → eth0 (loopether) where promiscuous +
		// spoofing loop every packet back in and the catch-all forwarder
		// picks it up. Stock loopback gives identical loop-back semantics.
		if err := s.CreateNICWithOptions(decoyNICID, loopback.New(), stack.NICOptions{Name: "eth0"}); err != nil {
			t.Fatalf("creating decoy NIC: %s", err)
		}
		if err := s.SetPromiscuousMode(decoyNICID, true); err != nil {
			t.Fatalf("decoy promiscuous: %s", err)
		}
		if err := s.SetSpoofing(decoyNICID, true); err != nil {
			t.Fatalf("decoy spoofing: %s", err)
		}
		if err := s.AddProtocolAddress(decoyNICID, tcpip.ProtocolAddress{
			Protocol: ipv6.ProtocolNumber,
			AddressWithPrefix: tcpip.AddressWithPrefix{
				Address:   tcpip.AddrFrom16(decoyAddr.As16()),
				PrefixLen: 128,
			},
		}, stack.AddressProperties{}); err != nil {
			t.Fatalf("adding decoy address: %s", err)
		}
		routes = append(routes, tcpip.Route{Destination: header.IPv6EmptySubnet, NIC: decoyNICID})

		// The catch-all, installed exactly as installTCPForwarder does.
		fwd := tcp.NewForwarder(s, 0, spikeForwarderMaxInFlight, func(req *tcp.ForwarderRequest) {
			node.forwarderSYNs.Add(1)
			req.Complete(true) // RST
		})
		s.SetTransportProtocolHandler(tcp.ProtocolNumber, fwd.HandlePacket)
	}

	s.SetRouteTable(routes)
	ep.runRX()

	t.Cleanup(func() {
		s.Close()
		ep.shutdown()
	})
	return node
}

// connectOverlayPair installs mirrored vnets and keys on both handlers: one
// VNI, two simplex directions (A.tx == B.rx and vice versa), single static
// epoch. AllowedRoutes scope each side to {its own /128 → the Network /72},
// which is also what PhyToVirt validates return-packet inner sources
// against.
func connectOverlayPair(t testing.TB, a, b *overlayNode) {
	t.Helper()

	keyAB := randomKeyTB(t)
	keyBA := randomKeyTB(t)
	expiry := time.Now().Add(time.Hour)

	for _, side := range []struct {
		node   *overlayNode
		remote netip.AddrPort
		rx, tx [16]byte
	}{
		{node: a, remote: b.underlay(), rx: keyBA, tx: keyAB},
		{node: b, remote: a.underlay(), rx: keyAB, tx: keyBA},
	} {
		remoteFA := &tcpip.FullAddress{
			Addr: tcpip.AddrFrom16(side.remote.Addr().As16()),
			Port: side.remote.Port(),
		}
		err := side.node.handler.AddVirtualNetwork(overlayVNI, remoteFA, []icx.Route{
			{
				Src: netip.PrefixFrom(side.node.addr, 128),
				Dst: overlayNetworkPrefix,
			},
		})
		if err != nil {
			t.Fatalf("adding vnet: %v", err)
		}
		if err := side.node.handler.UpdateVirtualNetworkKeys(overlayVNI, overlayKeyEpoch, side.rx, side.tx, expiry); err != nil {
			t.Fatalf("installing keys: %v", err)
		}
	}
}

func mustSubnet(t testing.TB, p netip.Prefix) tcpip.Subnet {
	t.Helper()
	mask := net.CIDRMask(p.Bits(), 128)
	sub, err := tcpip.NewSubnet(tcpip.AddrFrom16(p.Masked().Addr().As16()), tcpip.MaskFromBytes(mask))
	if err != nil {
		t.Fatalf("building subnet for %v: %v", p, err)
	}
	return sub
}

func fullAddr(a netip.Addr, port uint16) tcpip.FullAddress {
	return tcpip.FullAddress{Addr: tcpip.AddrFrom16(a.As16()), Port: port}
}

// TestOverlayEgressTCPRoundtrip is the headline proof. A guest-originated
// TCP connection to an overlay destination crosses encap → real UDP socket
// → decap → DeliverNetworkPacket on side B (SYN into a LISTENING endpoint),
// and every return segment crosses the same machinery in reverse into side
// A's ESTABLISHED client endpoint — handshake ACKs, two request/response
// rounds, and FIN teardown all ride return-path injection. The far side
// must see the sandbox's real ULA /128, not a flattened proxy address. The
// negative leg proves the divert seam is exact: a non-overlay destination
// still falls through to the catch-all forwarder.
func TestOverlayEgressTCPRoundtrip(t *testing.T) {
	a := buildOverlayNode(t, sandboxAddr, true)
	b := buildOverlayNode(t, vpcPeerAddr, false)
	connectOverlayPair(t, a, b)

	const port = 8080
	ln, err := gonet.ListenTCP(b.stk, fullAddr(vpcPeerAddr, port), ipv6.ProtocolNumber)
	if err != nil {
		t.Fatalf("listening on B: %v", err)
	}
	defer ln.Close()

	type acceptResult struct {
		remote net.Addr
		err    error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			accepted <- acceptResult{err: err}
			return
		}
		defer c.Close()
		accepted <- acceptResult{remote: c.RemoteAddr()}
		// Two echo rounds: every response after the handshake exercises
		// delivery into A's ESTABLISHED endpoint.
		buf := make([]byte, 16)
		for i := 0; i < 2; i++ {
			n, err := io.ReadFull(c, buf[:4])
			if err != nil {
				return
			}
			if _, err := c.Write(bytes.ToUpper(buf[:n])); err != nil {
				return
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := gonet.DialContextTCP(ctx, a.stk, fullAddr(vpcPeerAddr, port), ipv6.ProtocolNumber)
	if err != nil {
		t.Fatalf("overlay dial failed (encap or inject path broken): %v", err)
	}
	defer conn.Close()

	res := <-accepted
	if res.err != nil {
		t.Fatalf("accept on B: %v", res.err)
	}
	remoteHost, _, err := net.SplitHostPort(res.remote.String())
	if err != nil {
		t.Fatalf("parsing B-side remote addr %q: %v", res.remote, err)
	}
	if got := netip.MustParseAddr(remoteHost); got != sandboxAddr {
		t.Errorf("B saw peer %v, want the sandbox's real ULA %v", got, sandboxAddr)
	}

	conn.SetDeadline(time.Now().Add(10 * time.Second))
	buf := make([]byte, 4)
	for i, msg := range []string{"ping", "pong"} {
		if _, err := conn.Write([]byte(msg)); err != nil {
			t.Fatalf("round %d write: %v", i, err)
		}
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatalf("round %d: response never reached the established endpoint: %v", i, err)
		}
		if want := string(bytes.ToUpper([]byte(msg))); string(buf) != want {
			t.Fatalf("round %d: got %q, want %q", i, buf, want)
		}
	}

	if got := a.forwarderSYNs.Load(); got != 0 {
		t.Errorf("overlay traffic leaked to the egress forwarder: %d SYNs", got)
	}
	if drops := a.ep.txDrops.Load(); drops != 0 {
		t.Errorf("unexpected encap drops on A: %d", drops)
	}
	if a.ep.rxDelivered.Load() == 0 {
		t.Error("no return packets were injected via DeliverNetworkPacket")
	}

	// Negative leg: a non-overlay destination must miss the /72 divert,
	// take the default route, and land in the catch-all forwarder (which
	// RSTs). Both the error and the counter prove the seam is exact.
	negCtx, negCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer negCancel()
	if c, err := gonet.DialContextTCP(negCtx, a.stk, fullAddr(nonOverlayDst, nonOverlayPort), ipv6.ProtocolNumber); err == nil {
		c.Close()
		t.Fatal("non-overlay dial unexpectedly succeeded")
	}
	if got := a.forwarderSYNs.Load(); got == 0 {
		t.Error("non-overlay dial never reached the catch-all forwarder")
	}
}

// TestOverlayUDPRoundtrip proves the connection-less return path: UDP
// responses have no handshake state at all — each return datagram is
// delivered purely by dst-address demux after injection.
func TestOverlayUDPRoundtrip(t *testing.T) {
	a := buildOverlayNode(t, sandboxAddr, true)
	b := buildOverlayNode(t, vpcPeerAddr, false)
	connectOverlayPair(t, a, b)

	const port = 9053
	srvAddr := fullAddr(vpcPeerAddr, port)
	srv, err := gonet.DialUDP(b.stk, &srvAddr, nil, ipv6.ProtocolNumber)
	if err != nil {
		t.Fatalf("binding UDP on B: %v", err)
	}
	defer srv.Close()

	peerSeen := make(chan string, 1)
	go func() {
		buf := make([]byte, 2048)
		n, from, err := srv.ReadFrom(buf)
		if err != nil {
			return
		}
		peerSeen <- from.String()
		_, _ = srv.WriteTo(append([]byte("echo:"), buf[:n]...), from)
	}()

	laddr := fullAddr(sandboxAddr, 0)
	raddr := fullAddr(vpcPeerAddr, port)
	cli, err := gonet.DialUDP(a.stk, &laddr, &raddr, ipv6.ProtocolNumber)
	if err != nil {
		t.Fatalf("overlay UDP dial: %v", err)
	}
	defer cli.Close()

	cli.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := cli.Write([]byte("hello")); err != nil {
		t.Fatalf("UDP write: %v", err)
	}
	buf := make([]byte, 2048)
	n, err := cli.Read(buf)
	if err != nil {
		t.Fatalf("UDP echo never came back through the inject path: %v", err)
	}
	if got := string(buf[:n]); got != "echo:hello" {
		t.Fatalf("echo = %q, want %q", got, "echo:hello")
	}

	select {
	case from := <-peerSeen:
		host, _, err := net.SplitHostPort(from)
		if err != nil {
			t.Fatalf("parsing B-side peer %q: %v", from, err)
		}
		if got := netip.MustParseAddr(host); got != sandboxAddr {
			t.Errorf("B saw UDP peer %v, want %v", got, sandboxAddr)
		}
	default:
		t.Error("server never recorded a peer")
	}
}

// TestOverlayInnerMTUClampTCP proves the static clamp end to end: with the
// geneve0 NIC MTU set to the 1392 clamp, TCP MSS derivation keeps every
// underlay datagram of a 1 MiB bulk transfer within both the inner budget
// (1392+48) and the underlay path budget (1500-48) — no PTB/PMTUD machinery
// required, which icx does not have.
func TestOverlayInnerMTUClampTCP(t *testing.T) {
	a := buildOverlayNode(t, sandboxAddr, true)
	b := buildOverlayNode(t, vpcPeerAddr, false)
	connectOverlayPair(t, a, b)

	const port = 8081
	const payloadLen = 1 << 20
	ln, err := gonet.ListenTCP(b.stk, fullAddr(vpcPeerAddr, port), ipv6.ProtocolNumber)
	if err != nil {
		t.Fatalf("listening on B: %v", err)
	}
	defer ln.Close()

	srvDigest := make(chan [32]byte, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		var ln4 [4]byte
		if _, err := io.ReadFull(c, ln4[:]); err != nil {
			return
		}
		h := sha256.New()
		if _, err := io.CopyN(h, c, int64(binary.BigEndian.Uint32(ln4[:]))); err != nil {
			return
		}
		var d [32]byte
		copy(d[:], h.Sum(nil))
		srvDigest <- d
		_, _ = c.Write(d[:8]) // ack so the client can assert full receipt
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := gonet.DialContextTCP(ctx, a.stk, fullAddr(vpcPeerAddr, port), ipv6.ProtocolNumber)
	if err != nil {
		t.Fatalf("overlay dial: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	payload := make([]byte, payloadLen)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("generating payload: %v", err)
	}
	var ln4 [4]byte
	binary.BigEndian.PutUint32(ln4[:], payloadLen)
	if _, err := conn.Write(ln4[:]); err != nil {
		t.Fatalf("writing length: %v", err)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("writing payload: %v", err)
	}
	var ack [8]byte
	if _, err := io.ReadFull(conn, ack[:]); err != nil {
		t.Fatalf("reading ack: %v", err)
	}

	want := sha256.Sum256(payload)
	select {
	case got := <-srvDigest:
		if got != want {
			t.Error("payload corrupted in transit")
		}
	case <-time.After(time.Second):
		t.Fatal("server never produced a digest")
	}
	if !bytes.Equal(ack[:], want[:8]) {
		t.Error("ack does not match payload digest")
	}

	maxSeen := a.ep.maxTxDatagram.Load()
	if maxSeen > maxOuterUDPLen {
		t.Errorf("underlay datagram %d exceeds inner clamp budget %d (MSS not honoring geneve0 MTU)", maxSeen, maxOuterUDPLen)
	}
	if maxSeen > underlayUDPBudget {
		t.Errorf("underlay datagram %d exceeds path budget %d — would fragment or blackhole on a real 1500 underlay", maxSeen, underlayUDPBudget)
	}
	t.Logf("bulk transfer: %d datagrams, max underlay payload %d (budget %d)",
		a.ep.txDatagrams.Load(), maxSeen, underlayUDPBudget)
}

// TestOverlayUDPSourceFragmentation proves the fragmentation story for
// connection-less traffic: a UDP datagram larger than the inner MTU is
// source-fragmented by the originating netstack (locally-originated IPv6
// fragments at source against the route MTU), each fragment is sealed and
// shipped as its own underlay datagram within budget, and the far stack
// reassembles the original datagram intact.
func TestOverlayUDPSourceFragmentation(t *testing.T) {
	a := buildOverlayNode(t, sandboxAddr, true)
	b := buildOverlayNode(t, vpcPeerAddr, false)
	connectOverlayPair(t, a, b)

	const port = 9054
	const datagramLen = 4000 // ~3 fragments at inner MTU 1392
	srvAddr := fullAddr(vpcPeerAddr, port)
	srv, err := gonet.DialUDP(b.stk, &srvAddr, nil, ipv6.ProtocolNumber)
	if err != nil {
		t.Fatalf("binding UDP on B: %v", err)
	}
	defer srv.Close()

	type recvResult struct {
		n      int
		digest [32]byte
	}
	received := make(chan recvResult, 1)
	go func() {
		buf := make([]byte, 64<<10)
		n, _, err := srv.ReadFrom(buf)
		if err != nil {
			return
		}
		received <- recvResult{n: n, digest: sha256.Sum256(buf[:n])}
	}()

	laddr := fullAddr(sandboxAddr, 0)
	raddr := fullAddr(vpcPeerAddr, port)
	cli, err := gonet.DialUDP(a.stk, &laddr, &raddr, ipv6.ProtocolNumber)
	if err != nil {
		t.Fatalf("overlay UDP dial: %v", err)
	}
	defer cli.Close()

	payload := make([]byte, datagramLen)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("generating payload: %v", err)
	}
	before := a.ep.txDatagrams.Load()
	if _, err := cli.Write(payload); err != nil {
		t.Fatalf("oversized UDP write failed (source fragmentation broken?): %v", err)
	}

	select {
	case got := <-received:
		if got.n != datagramLen {
			t.Fatalf("B reassembled %d bytes, want %d", got.n, datagramLen)
		}
		if got.digest != sha256.Sum256(payload) {
			t.Error("reassembled datagram corrupted")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("fragmented datagram never reassembled on B")
	}

	frags := a.ep.txDatagrams.Load() - before
	if frags < 3 {
		t.Errorf("expected >=3 underlay datagrams for a %dB datagram at inner MTU %d, got %d",
			datagramLen, spikeInnerMTU, frags)
	}
	if maxSeen := a.ep.maxTxDatagram.Load(); maxSeen > underlayUDPBudget {
		t.Errorf("fragment's underlay datagram %d exceeds path budget %d", maxSeen, underlayUDPBudget)
	}
}

// TestOverlayEncapMath pins the byte arithmetic the static inner-MTU clamp
// rests on against the imported icx version. If a bump changes the Geneve
// option layout, the AEAD tag size, or the MTU helper (v0.17.0 shipped a
// headerLength regression that over-estimates inner MTU by 24 bytes), this
// fails before any deployment math goes stale.
func TestOverlayEncapMath(t *testing.T) {
	if icx.HeaderSize != 32 {
		t.Errorf("icx.HeaderSize = %d, clamp math assumes 32", icx.HeaderSize)
	}
	if got := icx.MTU(spikePathMTU); got != spikeInnerMTU {
		t.Errorf("icx.MTU(%d) = %d, want %d", spikePathMTU, got, spikeInnerMTU)
	}
	// The deployed-floor variant (APO-543 pinned the kernel-Geneve leg to
	// 1280): document the number mode 3 inherits if the 1280 pin is still
	// load-bearing when it ships.
	if got := icx.MTU(1280); got != 1184 {
		t.Errorf("icx.MTU(1280) = %d, want 1184", got)
	}

	// Empirical overhead check: a sealed frame must carry exactly
	// encapOverhead bytes on top of the inner packet inside the outer UDP
	// payload.
	a, b := newHandlerPair(t)
	_ = b
	inner := synthInnerUDP6(sandboxAddr, vpcPeerAddr, 4242, 5353, 100)
	phy := make([]byte, 64<<10)
	frameLen, handled := a.VirtToPhy(inner, phy)
	if handled || frameLen == 0 {
		t.Fatalf("VirtToPhy declined the packet: len=%d handled=%v", frameLen, handled)
	}
	payload, _, err := outerUDPPayloadAndDst(phy[:frameLen])
	if err != nil {
		t.Fatalf("parsing VirtToPhy output: %v", err)
	}
	if got, want := len(payload)-len(inner), encapOverhead; got != want {
		t.Errorf("encap overhead = %d bytes, want %d", got, want)
	}
}

// TestOverlayDecapRejects drives the handler pair directly (no stacks, no
// sockets) and proves the decap path fail-closed properties the in-Sentry
// placement relies on: tampered ciphertext, replayed datagrams, and unknown
// VNIs all die at PhyToVirt without reaching the netstack.
func TestOverlayDecapRejects(t *testing.T) {
	a, b := newHandlerPair(t)

	inner := synthInnerUDP6(sandboxAddr, vpcPeerAddr, 4242, 5353, 256)
	phy := make([]byte, 64<<10)
	virt := make([]byte, 64<<10)

	seal := func() []byte {
		frameLen, handled := a.VirtToPhy(inner, phy)
		if handled || frameLen == 0 {
			t.Fatalf("VirtToPhy declined: len=%d handled=%v", frameLen, handled)
		}
		out := make([]byte, frameLen)
		copy(out, phy[:frameLen])
		return out
	}
	vnetB, ok := b.GetVirtualNetwork(overlayVNI)
	if !ok {
		t.Fatal("vnet missing on B")
	}

	t.Run("valid_frame_decrypts", func(t *testing.T) {
		if n := b.PhyToVirt(seal(), virt); n != len(inner) {
			t.Fatalf("PhyToVirt = %d, want %d", n, len(inner))
		}
		if !bytes.Equal(virt[:len(inner)], inner) {
			t.Error("decrypted inner packet differs from original")
		}
	})

	t.Run("tampered_ciphertext_rejected", func(t *testing.T) {
		frame := seal()
		frame[len(frame)-1] ^= 0xff // flip a ciphertext/tag byte
		before := vnetB.Stats.RXDecryptErrors.Load()
		if n := b.PhyToVirt(frame, virt); n != 0 {
			t.Fatalf("tampered frame decapped to %d bytes", n)
		}
		if got := vnetB.Stats.RXDecryptErrors.Load(); got != before+1 {
			t.Errorf("RXDecryptErrors = %d, want %d", got, before+1)
		}
	})

	t.Run("replay_rejected", func(t *testing.T) {
		frame := seal()
		if n := b.PhyToVirt(frame, virt); n == 0 {
			t.Fatal("first delivery rejected")
		}
		before := vnetB.Stats.RXReplayDrops.Load()
		if n := b.PhyToVirt(frame, virt); n != 0 {
			t.Fatalf("replayed frame decapped to %d bytes", n)
		}
		if got := vnetB.Stats.RXReplayDrops.Load(); got != before+1 {
			t.Errorf("RXReplayDrops = %d, want %d", got, before+1)
		}
	})

	t.Run("unknown_vni_rejected", func(t *testing.T) {
		// A second sandbox's VNI that B has no vnet for: re-key A's frame
		// under a different VNI by standing up a third handler.
		c := newKeyedHandler(t, a, 0x202)
		phyC := make([]byte, 64<<10)
		frameLen, handled := c.VirtToPhy(inner, phyC)
		if handled || frameLen == 0 {
			t.Fatalf("VirtToPhy on C declined: len=%d handled=%v", frameLen, handled)
		}
		rxBefore := vnetB.Stats.RXPackets.Load()
		if n := b.PhyToVirt(phyC[:frameLen], virt); n != 0 {
			t.Fatalf("unknown-VNI frame decapped to %d bytes", n)
		}
		if got := vnetB.Stats.RXPackets.Load(); got != rxBefore {
			t.Error("unknown-VNI frame counted as received")
		}
	})

	t.Run("garbage_rejected", func(t *testing.T) {
		junk := synthInnerUDP6(sandboxAddr, vpcPeerAddr, 1, 2, 64) // not Geneve at all
		frame := make([]byte, 64<<10)
		frameLen, err := synthOuterFrame(frame, junk, netip.AddrPortFrom(netip.MustParseAddr("::1"), 12345), &tcpip.FullAddress{
			Addr: tcpip.AddrFrom16(netip.MustParseAddr("::1").As16()),
			Port: 6081,
		})
		if err != nil {
			t.Fatalf("synthesizing junk frame: %v", err)
		}
		if n := b.PhyToVirt(frame[:frameLen], virt); n != 0 {
			t.Fatalf("garbage decapped to %d bytes", n)
		}
	})
}

// newHandlerPair builds two keyed, mutually-wired icx handlers without any
// netstack or socket plumbing, for the handler-level tests and benchmarks.
// Underlay addresses are synthetic (loopback + fixed ports) — nothing is
// actually sent.
func newHandlerPair(t testing.TB) (*icx.Handler, *icx.Handler) {
	t.Helper()

	keyAB := randomKeyTB(t)
	keyBA := randomKeyTB(t)
	expiry := time.Now().Add(time.Hour)

	mk := func(localPort, remotePort uint16, localPfx netip.Prefix, rx, tx [16]byte) *icx.Handler {
		local := &tcpip.FullAddress{
			Addr: tcpip.AddrFrom16(netip.MustParseAddr("::1").As16()),
			Port: localPort,
		}
		h, err := icx.NewHandler(icx.WithLayer3VirtFrames(), icx.WithLocalAddr(local))
		if err != nil {
			t.Fatalf("creating handler: %v", err)
		}
		remote := &tcpip.FullAddress{
			Addr: tcpip.AddrFrom16(netip.MustParseAddr("::1").As16()),
			Port: remotePort,
		}
		if err := h.AddVirtualNetwork(overlayVNI, remote, []icx.Route{
			{Src: localPfx, Dst: overlayNetworkPrefix},
		}); err != nil {
			t.Fatalf("adding vnet: %v", err)
		}
		if err := h.UpdateVirtualNetworkKeys(overlayVNI, overlayKeyEpoch, rx, tx, expiry); err != nil {
			t.Fatalf("installing keys: %v", err)
		}
		return h
	}

	a := mk(40001, 40002, netip.PrefixFrom(sandboxAddr, 128), keyBA, keyAB)
	b := mk(40002, 40001, netip.PrefixFrom(vpcPeerAddr, 128), keyAB, keyBA)
	return a, b
}

// newKeyedHandler clones A's tx role under a different VNI (a "second
// sandbox" for the unknown-VNI rejection test).
func newKeyedHandler(t testing.TB, _ *icx.Handler, vni uint) *icx.Handler {
	t.Helper()
	local := &tcpip.FullAddress{
		Addr: tcpip.AddrFrom16(netip.MustParseAddr("::1").As16()),
		Port: 40003,
	}
	h, err := icx.NewHandler(icx.WithLayer3VirtFrames(), icx.WithLocalAddr(local))
	if err != nil {
		t.Fatalf("creating handler: %v", err)
	}
	remote := &tcpip.FullAddress{
		Addr: tcpip.AddrFrom16(netip.MustParseAddr("::1").As16()),
		Port: 40002,
	}
	if err := h.AddVirtualNetwork(vni, remote, []icx.Route{
		{Src: netip.PrefixFrom(sandboxAddr, 128), Dst: overlayNetworkPrefix},
	}); err != nil {
		t.Fatalf("adding vnet: %v", err)
	}
	if err := h.UpdateVirtualNetworkKeys(vni, overlayKeyEpoch, randomKeyTB(t), randomKeyTB(t), time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("installing keys: %v", err)
	}
	return h
}

func randomKeyTB(t testing.TB) [16]byte {
	t.Helper()
	var k [16]byte
	if _, err := rand.Read(k[:]); err != nil {
		t.Fatalf("generating key: %v", err)
	}
	return k
}

// synthInnerUDP6 hand-builds an inner IPv6+UDP packet (the thing a guest
// stack would emit). The UDP checksum is left zero: the AEAD covers
// integrity and PhyToVirt never validates inner transport checksums.
func synthInnerUDP6(src, dst netip.Addr, sport, dport uint16, payloadLen int) []byte {
	pkt := make([]byte, header.IPv6MinimumSize+header.UDPMinimumSize+payloadLen)
	ip := header.IPv6(pkt)
	ip.Encode(&header.IPv6Fields{
		PayloadLength:     uint16(header.UDPMinimumSize + payloadLen),
		TransportProtocol: header.UDPProtocolNumber,
		HopLimit:          64,
		SrcAddr:           tcpip.AddrFrom16(src.As16()),
		DstAddr:           tcpip.AddrFrom16(dst.As16()),
	})
	u := header.UDP(pkt[header.IPv6MinimumSize:])
	u.Encode(&header.UDPFields{
		SrcPort: sport,
		DstPort: dport,
		Length:  uint16(header.UDPMinimumSize + payloadLen),
	})
	payload := pkt[header.IPv6MinimumSize+header.UDPMinimumSize:]
	for i := range payload {
		payload[i] = byte(i)
	}
	return pkt
}
