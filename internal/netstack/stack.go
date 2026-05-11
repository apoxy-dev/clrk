//go:build linux

package netstack

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/icmp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"

	"github.com/apoxy-dev/clrk/internal/ports"
)

const (
	// Standard Ethernet MTU. Sandbox traffic is over a virtual TAP — no tunnel
	// overhead to account for.
	sandboxMTU = 1500

	// channelSize is the number of packets the channel endpoint can buffer.
	channelSize = 4096
)

// RevisionStack is a per-(TaskAgent, revision) userspace TCP/IP stack
// built on gVisor. One *stack.Stack hosts N NICs — one per sandbox of
// the revision on this worker — so the protocol-state cost (channel
// rings, forwarders, DNS cache, IMDS listener) is paid once per
// revision instead of once per sandbox.
//
// Lifecycle: NewRevisionStack constructs the empty stack; Start
// installs TCP/UDP forwarders with the supplied dialer; Attach adds
// one NIC per sandbox; Detach removes one; Close tears everything
// down. The dialer hands every intercepted connection's source IP
// into ctx via WithSourceAddr so a shared IdentityDialer can
// attribute per-dial state to the originating sandbox.
type RevisionStack struct {
	ipstack  *stack.Stack
	ipt      *IPTables
	dnsCache *DNSCache

	startOnce sync.Once
	started   atomic.Bool
	closed    atomic.Bool

	mu   sync.Mutex
	nics map[tcpip.NICID]*nicAttachment
}

// nicAttachment is one sandbox's slot on a shared RevisionStack: the
// TAP fd, the channel endpoint feeding gVisor, and the per-NIC packet
// pump. Detach closes them in reverse order so the pump exits cleanly
// before the endpoint is destroyed.
type nicAttachment struct {
	nicID     tcpip.NICID
	ep        *channel.Endpoint
	tapFD     *os.File
	pump      *PacketPump
	pumpDone  chan struct{}
	sandboxIP netip.Addr
	gwIP      netip.Addr
}

// NewRevisionStack constructs an empty multi-NIC gVisor stack ready
// for Start + Attach. IMDS addresses are not bound here — they're
// installed on each per-sandbox NIC by Attach so packets destined for
// 169.254.169.254 from any sandbox route into the shared stack and
// reach the single metadata listener bound by the caller.
func NewRevisionStack() (*RevisionStack, error) {
	// SNAT is mostly a no-op on this path because TCP/UDP traffic is
	// intercepted by the in-process forwarders and never leaves
	// through a NIC. Construct the iptables config with a zero gateway
	// (snatTarget.Action passes through when addr.Len() == 0) so we
	// don't accidentally rewrite a per-NIC source to a single shared
	// address — each sandbox NIC carries its own /30 gateway today.
	ipt := newIPTables(tcpip.Address{}, tcpip.Address{})

	opts := stack.Options{
		NetworkProtocols: []stack.NetworkProtocolFactory{
			ipv4.NewProtocol,
			ipv6.NewProtocol,
		},
		TransportProtocols: []stack.TransportProtocolFactory{
			tcp.NewProtocol,
			udp.NewProtocol,
			icmp.NewProtocol4,
			icmp.NewProtocol6,
		},
		DefaultIPTables: ipt.defaultIPTables,
	}

	ipstack := stack.New(opts)
	if err := setTCPOptions(ipstack); err != nil {
		return nil, err
	}

	// Per-NIC routes are appended in Attach as each sandbox joins.
	// Without a NIC-scoped default route, gVisor's UDP forwarder
	// can't allocate a local endpoint for an incoming packet
	// (CreateEndpoint does a route lookup against the destination
	// address). Initialize empty; Attach builds the table up.
	ipstack.SetRouteTable(nil)

	return &RevisionStack{
		ipstack:  ipstack,
		ipt:      ipt,
		dnsCache: NewDNSCache(),
		nics:     make(map[tcpip.NICID]*nicAttachment),
	}, nil
}

// DNSCache returns the per-revision DNS-answer cache populated by the
// UDP/53 response snoop. Shared across all sandboxes of the revision
// — entries are keyed by resolved IP, so cross-sandbox sharing is
// safe (two sandboxes asking for the same name hit the same entry).
func (r *RevisionStack) DNSCache() *DNSCache { return r.dnsCache }

// Stack returns the underlying gVisor stack so callers can bind
// their own gonet listeners (e.g. the shared IMDS metadata server).
// Lifetime is tied to the RevisionStack — once Close is called the
// stack is torn down.
func (r *RevisionStack) Stack() *stack.Stack { return r.ipstack }

// Start installs the TCP/UDP forwarders with the supplied dialer.
// Idempotent — only the first call takes effect, so callers can
// Start before any Attach without racing against subsequent
// per-sandbox attachments.
func (r *RevisionStack) Start(ctx context.Context, dialer Dialer) {
	r.startOnce.Do(func() {
		tcpFwd := TCPForwarder(ctx, r.ipstack, dialer)
		r.ipstack.SetTransportProtocolHandler(tcp.ProtocolNumber, tcpFwd)
		udpFwd := UDPForwarder(ctx, r.ipstack, dialer, r.dnsCache)
		r.ipstack.SetTransportProtocolHandler(udp.ProtocolNumber, udpFwd)
		r.started.Store(true)
	})
}

// Attach adds one NIC backed by tapFD with the supplied gateway/IP
// pair (per-sandbox /30). The IMDS v4 link-local address is bound on
// this NIC so packets the sandbox sends to 169.254.169.254 reach the
// listener bound by metadata.New (NIC: 0 — accept from any NIC).
//
// The returned NIC ID identifies the attachment for later Detach.
// Spawns a per-NIC packet pump goroutine that runs until Detach or
// Close.
func (r *RevisionStack) Attach(tapFD *os.File, gw, sandboxIP netip.Addr) (tcpip.NICID, error) {
	if r.closed.Load() {
		return 0, errors.New("RevisionStack closed")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	nicID := r.ipstack.NextNICID()
	linkEP := channel.New(channelSize, uint32(sandboxMTU), "")

	if tcpipErr := r.ipstack.CreateNIC(nicID, linkEP); tcpipErr != nil {
		linkEP.Close()
		return 0, fmt.Errorf("creating NIC: %v", tcpipErr)
	}

	// Allow spoofing (source addr != NIC addr) for SNAT'd return
	// traffic, and promiscuous so the netstack accepts packets
	// destined for any IP routed via this NIC. Matches the original
	// per-sandbox SandboxStack semantics.
	if tcpipErr := r.ipstack.SetSpoofing(nicID, true); tcpipErr != nil {
		r.ipstack.RemoveNIC(nicID)
		linkEP.Close()
		return 0, fmt.Errorf("enabling spoofing on NIC %d: %v", nicID, tcpipErr)
	}
	if tcpipErr := r.ipstack.SetPromiscuousMode(nicID, true); tcpipErr != nil {
		r.ipstack.RemoveNIC(nicID)
		linkEP.Close()
		return 0, fmt.Errorf("enabling promiscuous mode on NIC %d: %v", nicID, tcpipErr)
	}

	gwV4 := tcpip.AddrFrom4(gw.As4())
	if tcpipErr := r.ipstack.AddProtocolAddress(nicID, tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: gwV4.WithPrefix(),
	}, stack.AddressProperties{}); tcpipErr != nil {
		r.ipstack.RemoveNIC(nicID)
		linkEP.Close()
		return 0, fmt.Errorf("adding gateway address: %v", tcpipErr)
	}

	imdsV4, err := netip.ParseAddr(ports.MetadataAddrV4)
	if err != nil {
		r.ipstack.RemoveNIC(nicID)
		linkEP.Close()
		return 0, fmt.Errorf("parsing IMDS v4 address: %w", err)
	}
	if tcpipErr := r.ipstack.AddProtocolAddress(nicID, tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddrFrom4(imdsV4.As4()).WithPrefix(),
	}, stack.AddressProperties{}); tcpipErr != nil {
		r.ipstack.RemoveNIC(nicID)
		linkEP.Close()
		return 0, fmt.Errorf("adding IMDS v4 address: %v", tcpipErr)
	}
	imdsV6, err := netip.ParseAddr(ports.MetadataAddrV6)
	if err != nil {
		r.ipstack.RemoveNIC(nicID)
		linkEP.Close()
		return 0, fmt.Errorf("parsing IMDS v6 address: %w", err)
	}
	if tcpipErr := r.ipstack.AddProtocolAddress(nicID, tcpip.ProtocolAddress{
		Protocol:          ipv6.ProtocolNumber,
		AddressWithPrefix: tcpip.AddrFrom16(imdsV6.As16()).WithPrefix(),
	}, stack.AddressProperties{}); tcpipErr != nil {
		r.ipstack.RemoveNIC(nicID)
		linkEP.Close()
		return 0, fmt.Errorf("adding IMDS v6 address: %v", tcpipErr)
	}

	// Per-NIC default route. With multiple NICs on a shared stack,
	// the route table needs a NIC-scoped entry per sandbox so the
	// UDP forwarder's CreateEndpoint (which does a route lookup
	// against the packet's destination address) can resolve the
	// outbound interface for replies. We add a default-route entry
	// pointing at this NIC — gVisor matches the inbound packet's
	// destination against bound addresses for delivery, and uses
	// this route entry for the reply path.
	r.ipstack.AddRoute(tcpip.Route{
		Destination: header.IPv4EmptySubnet,
		NIC:         nicID,
	})

	pump := NewPacketPump(tapFD, linkEP, sandboxMTU)
	att := &nicAttachment{
		nicID:     nicID,
		ep:        linkEP,
		tapFD:     tapFD,
		pump:      pump,
		pumpDone:  make(chan struct{}),
		sandboxIP: sandboxIP,
		gwIP:      gw,
	}
	r.nics[nicID] = att

	go func() {
		defer close(att.pumpDone)
		_ = pump.Run()
	}()

	return nicID, nil
}

// Detach removes the NIC and stops its packet pump. Idempotent —
// repeat calls return nil. Safe to call before Close.
func (r *RevisionStack) Detach(nicID tcpip.NICID) error {
	r.mu.Lock()
	att, ok := r.nics[nicID]
	if ok {
		delete(r.nics, nicID)
	}
	r.mu.Unlock()
	if !ok {
		return nil
	}
	r.detachOne(att)
	return nil
}

// detachOne tears down one attachment. Caller must already have
// removed it from r.nics.
//
// The pump's inbound goroutine reads from the TAP fd and only exits
// when that fd is closed by the caller's later TeardownNetNS — pump.
// Close() only stops the outbound side. So we do not block on pumpDone
// here; the inbound goroutine reaps after the netns/TAP teardown that
// follows Detach in the sandbox-delete sequence.
func (r *RevisionStack) detachOne(att *nicAttachment) {
	// Remove NIC + close channel endpoint first so gVisor stops
	// pushing to the pump's wakeup channel before we close it.
	r.ipstack.RemoveNIC(att.nicID)
	att.ep.Close()
	att.pump.Close()
}

// Close tears down every attachment and the stack itself. Idempotent.
func (r *RevisionStack) Close() error {
	if r.closed.Swap(true) {
		return nil
	}
	r.mu.Lock()
	attachments := make([]*nicAttachment, 0, len(r.nics))
	for _, att := range r.nics {
		attachments = append(attachments, att)
	}
	r.nics = nil
	r.mu.Unlock()
	for _, att := range attachments {
		r.detachOne(att)
	}
	return nil
}

// RegisterTCPMetrics registers gVisor TCP stats as Prometheus gauges.
func (r *RevisionStack) RegisterTCPMetrics(reg prometheus.Registerer) {
	st := r.ipstack.Stats().TCP
	gauges := []struct {
		name string
		help string
		fn   func() float64
	}{
		{"clrk_netstack_tcp_segments_sent_total", "TCP segments sent.", func() float64 { return float64(st.SegmentsSent.Value()) }},
		{"clrk_netstack_tcp_segments_received_total", "TCP segments received.", func() float64 { return float64(st.ValidSegmentsReceived.Value()) }},
		{"clrk_netstack_tcp_retransmits_total", "TCP segments retransmitted.", func() float64 { return float64(st.Retransmits.Value()) }},
		{"clrk_netstack_tcp_fast_retransmit_total", "TCP fast retransmits.", func() float64 { return float64(st.FastRetransmit.Value()) }},
		{"clrk_netstack_tcp_timeouts_total", "TCP RTO timeouts.", func() float64 { return float64(st.Timeouts.Value()) }},
		{"clrk_netstack_tcp_fast_recovery_total", "TCP fast recovery events.", func() float64 { return float64(st.FastRecovery.Value()) }},
		{"clrk_netstack_tcp_sack_recovery_total", "TCP SACK recovery events.", func() float64 { return float64(st.SACKRecovery.Value()) }},
		{"clrk_netstack_tcp_checksum_errors_total", "TCP checksum errors.", func() float64 { return float64(st.ChecksumErrors.Value()) }},
		{"clrk_netstack_tcp_established", "Current established TCP connections.", func() float64 { return float64(st.CurrentEstablished.Value()) }},
		{"clrk_netstack_tcp_resets_sent_total", "TCP resets sent.", func() float64 { return float64(st.ResetsSent.Value()) }},
		{"clrk_netstack_tcp_resets_received_total", "TCP resets received.", func() float64 { return float64(st.ResetsReceived.Value()) }},
	}
	for _, g := range gauges {
		reg.MustRegister(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{Name: g.name, Help: g.help},
			g.fn,
		))
	}
}

// setTCPOptions configures high-performance TCP settings on the stack.
func setTCPOptions(ipstack *stack.Stack) error {
	type tcpOpt struct {
		name string
		opt  tcpip.SettableTransportProtocolOption
	}
	sack := tcpip.TCPSACKEnabled(true)
	// gVisor netstack only ships reno + cubic; bbr returns ENOENT.
	cc := tcpip.CongestionControlOption("cubic")
	delay := tcpip.TCPDelayEnabled(false)
	rcvBuf := tcpip.TCPReceiveBufferSizeRangeOption{Min: 64 << 10, Default: 2 << 20, Max: 16 << 20}
	sndBuf := tcpip.TCPSendBufferSizeRangeOption{Min: 64 << 10, Default: 2 << 20, Max: 16 << 20}
	modBuf := tcpip.TCPModerateReceiveBufferOption(true)
	twReuse := tcpip.TCPTimeWaitReuseOption(tcpip.TCPTimeWaitReuseGlobal)
	twTimeout := tcpip.TCPTimeWaitTimeoutOption(10 * time.Second)
	linger := tcpip.TCPLingerTimeoutOption(10 * time.Second)
	minRTO := tcpip.TCPMinRTOOption(100 * time.Millisecond)

	opts := []tcpOpt{
		{"SACK", &sack},
		{"congestion control", &cc},
		{"Nagle disable", &delay},
		{"receive buffer", &rcvBuf},
		{"send buffer", &sndBuf},
		{"moderate receive buffer", &modBuf},
		{"TIME_WAIT reuse", &twReuse},
		{"TIME_WAIT timeout", &twTimeout},
		{"linger timeout", &linger},
		{"min RTO", &minRTO},
	}
	for _, o := range opts {
		if tcpipErr := ipstack.SetTransportProtocolOption(tcp.ProtocolNumber, o.opt); tcpipErr != nil {
			return fmt.Errorf("setting TCP %s: %v", o.name, tcpipErr)
		}
	}
	return nil
}
