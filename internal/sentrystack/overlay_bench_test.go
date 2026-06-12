// APO-725 pump benchmarks: the load-bearing numbers for the B1 flip trigger
// (docs/workerd-egress-encap.md §6 in apoxy-cloud). Mode 3c stands or falls
// on what a per-packet sendto/recvfrom pump plus Go AES-128-GCM sustains —
// no GSO, no sendmmsg, exactly the syscall budget the plugin seccomp filter
// grants a Sentry.
//
// Three tiers isolate where the cost lives:
//
//   - BenchmarkOverlaySealOpen: VirtToPhy+PhyToVirt back-to-back in memory.
//     The pure crypto+framing ceiling — no syscalls, no netstack.
//   - BenchmarkOverlayPumpPingPong: the same plus two real UDP sockets over
//     ::1. One op = seal → sendto → recvfrom → open in each direction, i.e.
//     2 inner packets; ns/op ÷ 2 ≈ per-packet serialized pump cost on this
//     host. The delta over SealOpen is the syscall share — the part systrap
//     will amplify.
//   - BenchmarkOverlayTCPBulk: the full picture — two netstacks, TCP through
//     the geneve0 endpoints, bulk one-way transfer. MB/s here is the
//     end-to-end single-flow figure to quote against the >~1-2 Gbps/flow
//     trigger.
//
// Host-native numbers (macOS/Linux) are the upper bound; the same pump
// under systrap inside a real sandbox is the production number and runs as
// the follow-up harness on the dev cluster (APO-694's manager-test pattern).
package sentrystack

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/apoxy-dev/icx"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
)

// benchInnerPayload sizes the synthetic inner IPv6+UDP packet to exactly the
// 1392 inner-MTU clamp (40 IPv6 + 8 UDP + payload).
const benchInnerPayload = spikeInnerMTU - 48

func BenchmarkOverlaySealOpen(b *testing.B) {
	ha, hb := newHandlerPair(b)
	inner := synthInnerUDP6(sandboxAddr, vpcPeerAddr, 4242, 5353, benchInnerPayload)
	if len(inner) != spikeInnerMTU {
		b.Fatalf("inner packet is %d bytes, want %d", len(inner), spikeInnerMTU)
	}
	phy := make([]byte, 64<<10)
	virt := make([]byte, 64<<10)

	b.SetBytes(int64(len(inner)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frameLen, handled := ha.VirtToPhy(inner, phy)
		if handled || frameLen == 0 {
			b.Fatal("VirtToPhy declined")
		}
		if n := hb.PhyToVirt(phy[:frameLen], virt); n != len(inner) {
			b.Fatalf("PhyToVirt = %d, want %d", n, len(inner))
		}
	}
}

func BenchmarkOverlayPumpPingPong(b *testing.B) {
	connA, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback, Port: 0})
	if err != nil {
		b.Fatalf("binding A: %v", err)
	}
	defer connA.Close()
	connB, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback, Port: 0})
	if err != nil {
		b.Fatalf("binding B: %v", err)
	}
	defer connB.Close()

	ha := newBenchSocketHandler(b, connA, sandboxAddr, connB)
	hb := newBenchSocketHandler(b, connB, vpcPeerAddr, connA)
	// Mirror the simplex keys: A.tx == B.rx and vice versa.
	keyAB := randomKeyTB(b)
	keyBA := randomKeyTB(b)
	expiry := time.Now().Add(time.Hour)
	if err := ha.UpdateVirtualNetworkKeys(overlayVNI, overlayKeyEpoch, keyBA, keyAB, expiry); err != nil {
		b.Fatal(err)
	}
	if err := hb.UpdateVirtualNetworkKeys(overlayVNI, overlayKeyEpoch, keyAB, keyBA, expiry); err != nil {
		b.Fatal(err)
	}

	laA, err := localFullAddress(connA)
	if err != nil {
		b.Fatal(err)
	}
	laB, err := localFullAddress(connB)
	if err != nil {
		b.Fatal(err)
	}

	inner := synthInnerUDP6(sandboxAddr, vpcPeerAddr, 4242, 5353, benchInnerPayload)
	reply := synthInnerUDP6(vpcPeerAddr, sandboxAddr, 5353, 4242, benchInnerPayload)

	// B-side echo pump: recvfrom → synth → open → seal reply → sendto.
	done := make(chan struct{})
	go func() {
		defer close(done)
		rx := make([]byte, 64<<10)
		frame := make([]byte, 64<<10)
		virt := make([]byte, 64<<10)
		phy := make([]byte, 64<<10)
		for {
			n, raddr, err := connB.ReadFromUDPAddrPort(rx)
			if err != nil {
				return
			}
			frameLen, err := synthOuterFrame(frame, rx[:n], raddr, laB)
			if err != nil {
				continue
			}
			if hb.PhyToVirt(frame[:frameLen], virt) == 0 {
				continue
			}
			outLen, handled := hb.VirtToPhy(reply, phy)
			if handled || outLen == 0 {
				continue
			}
			payload, dst, err := outerUDPPayloadAndDst(phy[:outLen])
			if err != nil {
				continue
			}
			if _, err := connB.WriteToUDPAddrPort(payload, dst); err != nil {
				return
			}
		}
	}()

	phy := make([]byte, 64<<10)
	frame := make([]byte, 64<<10)
	virt := make([]byte, 64<<10)
	rx := make([]byte, 64<<10)

	// One op moves 2 full-sized inner packets (there and back).
	b.SetBytes(int64(2 * len(inner)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frameLen, handled := ha.VirtToPhy(inner, phy)
		if handled || frameLen == 0 {
			b.Fatal("VirtToPhy declined")
		}
		payload, dst, err := outerUDPPayloadAndDst(phy[:frameLen])
		if err != nil {
			b.Fatal(err)
		}
		if _, err := connA.WriteToUDPAddrPort(payload, dst); err != nil {
			b.Fatal(err)
		}
		n, raddr, err := connA.ReadFromUDPAddrPort(rx)
		if err != nil {
			b.Fatal(err)
		}
		inLen, err := synthOuterFrame(frame, rx[:n], raddr, laA)
		if err != nil {
			b.Fatal(err)
		}
		if ha.PhyToVirt(frame[:inLen], virt) == 0 {
			b.Fatal("PhyToVirt declined the echo")
		}
	}
	b.StopTimer()
	connB.Close()
	<-done

	b.ReportMetric(2*float64(b.N)/b.Elapsed().Seconds(), "pkt/s")
}

func BenchmarkOverlayTCPBulk(b *testing.B) {
	a := buildOverlayNode(b, sandboxAddr, false)
	peer := buildOverlayNode(b, vpcPeerAddr, false)
	connectOverlayPair(b, a, peer)

	const port = 8090
	ln, err := gonet.ListenTCP(peer.stk, fullAddr(vpcPeerAddr, port), ipv6.ProtocolNumber)
	if err != nil {
		b.Fatalf("listening: %v", err)
	}
	defer ln.Close()

	const chunk = 64 << 10
	total := int64(b.N) * chunk

	srvDone := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			srvDone <- err
			return
		}
		defer c.Close()
		var ln8 [8]byte
		if _, err := io.ReadFull(c, ln8[:]); err != nil {
			srvDone <- err
			return
		}
		if _, err := io.CopyN(io.Discard, c, int64(binary.BigEndian.Uint64(ln8[:]))); err != nil {
			srvDone <- err
			return
		}
		_, err = c.Write([]byte{1}) // ack full receipt
		srvDone <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn, err := gonet.DialContextTCP(ctx, a.stk, fullAddr(vpcPeerAddr, port), ipv6.ProtocolNumber)
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	payload := make([]byte, chunk)
	if _, err := rand.Read(payload); err != nil {
		b.Fatal(err)
	}
	var ln8 [8]byte
	binary.BigEndian.PutUint64(ln8[:], uint64(total))

	b.SetBytes(chunk)
	b.ResetTimer()
	if _, err := conn.Write(ln8[:]); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(payload); err != nil {
			b.Fatalf("write %d: %v", i, err)
		}
	}
	var ack [1]byte
	if _, err := io.ReadFull(conn, ack[:]); err != nil {
		b.Fatalf("ack: %v", err)
	}
	b.StopTimer()
	if err := <-srvDone; err != nil {
		b.Fatalf("server: %v", err)
	}
	b.ReportMetric(float64(a.ep.txDatagrams.Load())/b.Elapsed().Seconds(), "datagrams/s")
}

// newBenchSocketHandler wires an L3 handler onto a real socket, with the
// vnet's remote pointed at the peer socket. Keys are installed by the
// caller (the two simplex directions must mirror).
func newBenchSocketHandler(b *testing.B, conn *net.UDPConn, local netip.Addr, peer *net.UDPConn) *icx.Handler {
	b.Helper()
	la, err := localFullAddress(conn)
	if err != nil {
		b.Fatal(err)
	}
	h, err := icx.NewHandler(icx.WithLayer3VirtFrames(), icx.WithLocalAddr(la))
	if err != nil {
		b.Fatal(err)
	}
	pap := peer.LocalAddr().(*net.UDPAddr).AddrPort()
	remote := &tcpip.FullAddress{
		Addr: tcpip.AddrFrom16(pap.Addr().As16()),
		Port: pap.Port(),
	}
	if err := h.AddVirtualNetwork(overlayVNI, remote, []icx.Route{
		{Src: netip.PrefixFrom(local, 128), Dst: overlayNetworkPrefix},
	}); err != nil {
		b.Fatal(err)
	}
	return h
}
