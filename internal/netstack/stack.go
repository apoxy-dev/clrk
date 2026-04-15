//go:build linux

package netstack

import (
	"context"
	"fmt"
	"net/netip"
	"os"
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
)

const (
	// Standard Ethernet MTU. Sandbox traffic is over a virtual TAP — no tunnel
	// overhead to account for.
	sandboxMTU = 1500

	// channelSize is the number of packets the channel endpoint can buffer.
	channelSize = 4096
)

// SandboxStack is a per-sandbox userspace TCP/IP stack built on gVisor. It
// reads raw packets from a TAP fd and forwards intercepted connections via
// the provided Dialer.
type SandboxStack struct {
	ipstack *stack.Stack
	ep      *channel.Endpoint
	ipt     *IPTables
	nicID   tcpip.NICID
	tapFD   *os.File
	pump    *PacketPump
	closed  atomic.Bool
}

// NewSandboxStack creates a gVisor network stack for a single sandbox.
// gwAddr is the gateway IP assigned to this sandbox's /30 subnet (used for SNAT).
// The returned stack is not yet running; call Start to begin packet processing.
func NewSandboxStack(tapFD *os.File, gwAddr netip.Addr) (*SandboxStack, error) {
	// Build SNAT address.
	gwV4 := tcpip.AddrFrom4(gwAddr.As4())
	// No IPv6 gateway assigned by current IP allocator; leave as invalid.
	var gwV6 tcpip.Address
	ipt := newIPTables(gwV4, gwV6)

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

	// TCP tuning — ported from apoxy-cli.
	if err := setTCPOptions(ipstack); err != nil {
		return nil, err
	}

	nicID := ipstack.NextNICID()
	linkEP := channel.New(channelSize, uint32(sandboxMTU), "")

	if tcpipErr := ipstack.CreateNIC(nicID, linkEP); tcpipErr != nil {
		return nil, fmt.Errorf("creating NIC: %v", tcpipErr)
	}

	// Route all traffic through this NIC.
	ipstack.SetRouteTable([]tcpip.Route{
		{Destination: header.IPv4EmptySubnet, NIC: nicID},
		{Destination: header.IPv6EmptySubnet, NIC: nicID},
	})

	// Add the gateway address to the stack so SNAT works.
	protoAddr := tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: gwV4.WithPrefix(),
	}
	if tcpipErr := ipstack.AddProtocolAddress(nicID, protoAddr, stack.AddressProperties{}); tcpipErr != nil {
		return nil, fmt.Errorf("adding gateway address: %v", tcpipErr)
	}

	pump := NewPacketPump(tapFD, linkEP, sandboxMTU)

	return &SandboxStack{
		ipstack: ipstack,
		ep:      linkEP,
		ipt:     ipt,
		nicID:   nicID,
		tapFD:   tapFD,
		pump:    pump,
	}, nil
}

// Start enables packet forwarding and begins the TAP pump. It blocks until
// ctx is cancelled or the pump encounters a fatal error.
func (s *SandboxStack) Start(ctx context.Context, dialer Dialer) error {
	// Allow spoofing (source addr != NIC addr) for SNAT'd return traffic.
	if tcpipErr := s.ipstack.SetSpoofing(s.nicID, true); tcpipErr != nil {
		return fmt.Errorf("enabling spoofing: %v", tcpipErr)
	}
	// Allow promiscuous mode so we receive packets destined for any IP.
	if tcpipErr := s.ipstack.SetPromiscuousMode(s.nicID, true); tcpipErr != nil {
		return fmt.Errorf("enabling promiscuous mode: %v", tcpipErr)
	}

	// Wire up TCP and UDP forwarders.
	tcpFwd := TCPForwarder(ctx, s.ipstack, dialer)
	s.ipstack.SetTransportProtocolHandler(tcp.ProtocolNumber, tcpFwd)

	udpFwd := UDPForwarder(ctx, s.ipstack, dialer)
	s.ipstack.SetTransportProtocolHandler(udp.ProtocolNumber, udpFwd)

	// Run the TAP packet pump (blocks).
	return s.pump.Run()
}

// Close tears down the stack and pump.
func (s *SandboxStack) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	// Remove NIC and close endpoint first to deregister the WriteNotify
	// callback before closing the pump's wakeup channel.
	s.ipstack.RemoveNIC(s.nicID)
	s.ep.Close()
	s.pump.Close()
	return nil
}

// RegisterTCPMetrics registers gVisor TCP stats as Prometheus gauges.
func (s *SandboxStack) RegisterTCPMetrics(reg prometheus.Registerer) {
	st := s.ipstack.Stats().TCP
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
	cc := tcpip.CongestionControlOption("bbr")
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
