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

const (
	dnsPort = 53

	// udpFlowTableCap bounds the per-stack 4-tuple session table.
	// 4096 is two orders of magnitude above any realistic concurrent
	// UDP flow count per revision; the cap exists only to keep a
	// misbehaving sandbox from growing the map unboundedly via
	// spoofed 4-tuples (NTP amplification, QUIC connection-ID
	// rotation, etc).
	udpFlowTableCap = 4096

	udpIdleTimeout = 30 * time.Second

	// udpSiblingWait bounds how long a late-arriving sibling on the
	// same 4-tuple waits for the creator goroutine to publish its
	// upstream conn. Long enough to cover an upstream dial plus
	// endpoint registration under load; short enough that a stuck
	// dial can't pin a handler goroutine forever.
	udpSiblingWait = 2 * time.Second
)

var udpBuffPool = sync.Pool{
	New: func() any {
		b := make([]byte, 65536)
		return &b
	},
}

// udpFlowKey is the gVisor 4-tuple used by the UDP demuxer. It's used
// here as a map key for the per-flow session table; uses tcpip.Address
// directly so insertion is allocation-free.
type udpFlowKey struct {
	rAddr tcpip.Address
	rPort uint16
	lAddr tcpip.Address
	lPort uint16
}

func udpFlowKeyFromID(id stack.TransportEndpointID) udpFlowKey {
	return udpFlowKey{
		rAddr: id.RemoteAddress,
		rPort: id.RemotePort,
		lAddr: id.LocalAddress,
		lPort: id.LocalPort,
	}
}

// udpFlow is the per-(4-tuple) session state. The creator goroutine
// (runUDPFlow) assigns upConn and extend, then closes ready; siblings
// that lost the table-insert race block on ready, then write payload
// directly to upConn. The channel close acts as the happens-before
// barrier for those reads — no atomic / mutex needed on the fields.
//
// Multiple goroutines may write to upConn concurrently (the creator's
// down→up copy loop and any number of fast-path injectors). That is
// safe because UDP datagram writes are atomic at the sendmsg(2) level.
type udpFlow struct {
	upConn net.Conn
	extend func()
	ready  chan struct{}
}

// udpFlowTable tracks active UDP flows by 4-tuple so packets racing
// through the handler dispatch path on the same flow are coalesced
// into a single session.
type udpFlowTable struct {
	mu    sync.Mutex
	flows map[udpFlowKey]*udpFlow
}

func newUDPFlowTable() *udpFlowTable {
	return &udpFlowTable{flows: make(map[udpFlowKey]*udpFlow)}
}

// getOrInsert returns:
//   - (flow, true)  → newly inserted; caller is the creator.
//   - (flow, false) → existing flow; caller is a late sibling.
//   - (nil,  false) → table at cap; caller should drop the packet.
func (t *udpFlowTable) getOrInsert(key udpFlowKey) (*udpFlow, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if f, ok := t.flows[key]; ok {
		return f, false
	}
	if len(t.flows) >= udpFlowTableCap {
		return nil, false
	}
	f := &udpFlow{ready: make(chan struct{})}
	t.flows[key] = f
	return f, true
}

// remove evicts the entry only if it still matches the supplied flow,
// so a stale defer in a long-since-replaced session can't delete the
// successor.
func (t *udpFlowTable) remove(key udpFlowKey, f *udpFlow) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, ok := t.flows[key]; ok && cur == f {
		delete(t.flows, key)
	}
}

// UDPForwarder intercepts UDP packets that don't match an existing
// endpoint and forwards them to the upstream via the provided Dialer.
//
// Unlike gVisor's udp.Forwarder, this handler maintains a per-flow
// 4-tuple session table. When two packets on the same flow arrive
// within the dispatch window — the canonical case is musl's parallel
// A+AAAA DNS queries from the same source port — the second packet
// joins the first's session via direct upConn.Write rather than
// racing CreateEndpoint and losing to ErrPortInUse. That race is what
// caused Alpine-based sandboxes to eat a ~2 s resolver timeout on
// every DNS lookup.
//
// When dnsCache is non-nil, upstream → sandbox traffic on UDP/53 is
// snooped: responses are parsed and any A/AAAA answers are bound to
// their qname/CNAME-aliases in the cache, so subsequent TCP dials can
// surface the agent's stated intent (the resolved hostname) on the
// PROXY v2 frame and to L4 telemetry. Snoop is observe-only — bytes
// reach the sandbox unchanged regardless of parse success.
func UDPForwarder(ctx context.Context, ipstack *stack.Stack, dialer Dialer, dnsCache *DNSCache) ProtocolHandler {
	table := newUDPFlowTable()
	return func(id stack.TransportEndpointID, pkt *stack.PacketBuffer) bool {
		key := udpFlowKeyFromID(id)

		flow, created := table.getOrInsert(key)
		if flow == nil {
			slog.Warn("UDP flow table full, dropping packet",
				slog.Int("cap", udpFlowTableCap))
			return true
		}

		if !created {
			return injectSiblingPacket(flow, pkt)
		}

		// Clone the packet buffer so the goroutine's CreateEndpoint
		// can use it after the stack's handler-dispatch path has
		// released the original. Matches what gVisor's own forwarder
		// does in HandlePacket.
		go runUDPFlow(ctx, ipstack, dialer, dnsCache, table, key, flow, id, pkt.Clone())
		return true
	}
}

// injectSiblingPacket handles a packet whose 4-tuple matches an
// in-flight flow created by a sibling handler invocation. It blocks
// briefly on the creator's ready signal, then writes the payload
// directly to upConn. This is the path that fixes musl A+AAAA: the
// AAAA packet (race loser at the gVisor port-bind layer) still
// reaches the upstream resolver, and its response comes back via the
// gVisor endpoint registered by the A query's CreateEndpoint (both
// queries share the same src IP/port + dst IP/port so the demux
// delivers both responses to the same endpoint).
func injectSiblingPacket(flow *udpFlow, pkt *stack.PacketBuffer) bool {
	select {
	case <-flow.ready:
	case <-time.After(udpSiblingWait):
		return true
	}
	if flow.upConn == nil {
		return true
	}
	payload, ok := udpPayload(pkt)
	if !ok {
		return true
	}
	if _, err := flow.upConn.Write(payload); err != nil {
		return true
	}
	flow.extend()
	return true
}

func runUDPFlow(ctx context.Context, ipstack *stack.Stack, dialer Dialer, dnsCache *DNSCache,
	table *udpFlowTable, key udpFlowKey, flow *udpFlow,
	id stack.TransportEndpointID, pkt *stack.PacketBuffer) {

	srcAddrPort := netip.AddrPortFrom(addrFromNetstackIP(id.RemoteAddress), id.RemotePort)
	dstAddrPort := netip.AddrPortFrom(Unmap4in6(addrFromNetstackIP(id.LocalAddress)), id.LocalPort)

	logger := slog.With(
		slog.String("src", srcAddrPort.String()),
		slog.String("dst", dstAddrPort.String()),
	)
	logger.Debug("Forwarding UDP session")

	// Remove from the table first, then close the endpoint, so a
	// sibling packet arriving in the tear-down window either finds
	// the flow gone (and starts a fresh flow via CreateEndpoint) or
	// finds it but discovers upConn closed on Write. Either is
	// correct — UDP semantics allow the drop.
	defer table.remove(key, flow)

	sCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wq waiter.Queue
	req := udp.NewForwarderRequest(ipstack, id, pkt)
	ep, tcpipErr := req.CreateEndpoint(&wq)
	if tcpipErr != nil {
		// The per-flow table above eliminates the dominant race
		// (sibling packets within the same dispatch window). This
		// branch remains as defence-in-depth for the edge case
		// where the same 4-tuple is reused before the prior
		// endpoint has been deregistered from the demux.
		if _, isPortInUse := tcpipErr.(*tcpip.ErrPortInUse); isPortInUse {
			logger.Debug("Skipping duplicate UDP flow", slog.String("error", tcpipErr.String()))
		} else {
			logger.Error("Failed to create endpoint", slog.String("error", tcpipErr.String()))
		}
		close(flow.ready)
		return
	}

	downConn := gonet.NewUDPConn(&wq, ep)
	upConn, err := dialer.DialContext(sCtx, "udp", dstAddrPort.String())
	if err != nil {
		logger.Error("Failed to dial upstream", slog.Any("error", err))
		downConn.Close()
		close(flow.ready)
		return
	}

	timer := time.AfterFunc(udpIdleTimeout, func() {
		logger.Debug("Idle timeout reached")
		cancel()
		downConn.Close()
		upConn.Close()
	})
	extend := func() {
		timer.Reset(udpIdleTimeout)
	}

	// Publish for sibling fast path. The assignment must precede
	// the close so the channel close acts as a happens-before
	// barrier for sibling reads of upConn / extend.
	flow.upConn = upConn
	flow.extend = extend
	close(flow.ready)

	// Tap DNS responses (upstream → sandbox direction) into the
	// cache so TCP connect lookups can surface the resolved name.
	var responseTap func([]byte)
	if dstAddrPort.Port() == dnsPort && dnsCache != nil {
		responseTap = dnsCache.IngestResponse
	}

	g, copyCtx := errgroup.WithContext(sCtx)
	g.Go(func() error {
		return copyPackets(copyCtx, downConn, upConn, extend, nil)
	})
	g.Go(func() error {
		return copyPackets(copyCtx, upConn, downConn, extend, responseTap)
	})

	if err := g.Wait(); err != nil {
		logger.Error("Failed to copy packets", slog.Any("error", err))
	}

	timer.Stop()
	downConn.Close()
	upConn.Close()
	logger.Debug("UDP forwarding complete")
}

// copyPackets pumps datagrams from src to dst until ctx is cancelled
// or either side returns an error. extend is called after each
// successful write so the idle timer is reset; tap (if non-nil) is
// invoked with the payload before the write so callers can snoop the
// stream (used for the DNS response cache).
func copyPackets(ctx context.Context, src, dst net.Conn, extend func(), tap func([]byte)) error {
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
			extend()
		}
	}
}

// udpPayload returns the UDP payload of pkt as a caller-owned slice.
// gVisor's stack dispatch parses the UDP header off the packet buffer
// before invoking the unknown-destination handler, so pkt.Data() is
// the payload directly.
func udpPayload(pkt *stack.PacketBuffer) ([]byte, bool) {
	data := pkt.Data().AsRange().ToSlice()
	if len(data) == 0 {
		return nil, false
	}
	return data, true
}
