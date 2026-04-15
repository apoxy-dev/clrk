//go:build linux

// Package netstack implements per-sandbox userspace TCP/IP stacks using gVisor.
package netstack

import (
	"math/rand"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
)

// snatTarget implements stack.Target that performs SNAT using a fixed address.
type snatTarget struct {
	stack.SNATTarget
	addr tcpip.Address
}

// Action implements stack.Target.
func (t *snatTarget) Action(
	pkt *stack.PacketBuffer,
	hook stack.Hook,
	r *stack.Route,
	_ stack.AddressableEndpoint,
) (stack.RuleVerdict, int) {
	if t.addr.Len() == 0 {
		// No gateway address configured for this protocol; pass through
		// without SNAT rather than dropping.
		return stack.RuleAccept, 0
	}
	return t.SNATTarget.Action(pkt, hook, r, nil)
}

// IPTables configures gVisor iptables for a sandbox stack.
type IPTables struct {
	snatV4 *snatTarget
	snatV6 *snatTarget
}

func newIPTables(v4Addr, v6Addr tcpip.Address) *IPTables {
	v4snat := &snatTarget{
		SNATTarget: stack.SNATTarget{
			NetworkProtocol: header.IPv4ProtocolNumber,
			ChangeAddress:   true,
		},
		addr: v4Addr,
	}
	// Set Addr at construction time to avoid a data race in Action().
	v4snat.SNATTarget.Addr = v4Addr

	v6snat := &snatTarget{
		SNATTarget: stack.SNATTarget{
			NetworkProtocol: header.IPv6ProtocolNumber,
			ChangeAddress:   true,
		},
		addr: v6Addr,
	}
	if v6Addr.Len() > 0 {
		v6snat.SNATTarget.Addr = v6Addr
	}

	return &IPTables{
		snatV4: v4snat,
		snatV6: v6snat,
	}
}

func (ipt *IPTables) defaultIPTables(clock tcpip.Clock, rng *rand.Rand) *stack.IPTables {
	iptables := stack.DefaultTables(clock, rng)

	// IPv4 filter: accept TCP and UDP, drop everything else.
	ipv4filter := iptables.GetTable(stack.FilterID, false)
	ipv4filter.Rules = []stack.Rule{
		{
			Filter: stack.IPHeaderFilter{Protocol: header.TCPProtocolNumber, CheckProtocol: true},
			Target: &stack.AcceptTarget{NetworkProtocol: header.IPv4ProtocolNumber},
		},
		{
			Filter: stack.IPHeaderFilter{Protocol: header.UDPProtocolNumber, CheckProtocol: true},
			Target: &stack.AcceptTarget{NetworkProtocol: header.IPv4ProtocolNumber},
		},
		{Target: &stack.DropTarget{NetworkProtocol: header.IPv4ProtocolNumber}},
		{Target: &stack.AcceptTarget{NetworkProtocol: header.IPv4ProtocolNumber}},
	}
	ipv4filter.BuiltinChains = [stack.NumHooks]int{
		stack.Prerouting: 0, stack.Input: 0, stack.Forward: 0,
		stack.Output: 0, stack.Postrouting: 3,
	}
	ipv4filter.Underflows = [stack.NumHooks]int{
		stack.Prerouting: 2, stack.Input: 2, stack.Forward: 2,
		stack.Output: 2, stack.Postrouting: 2,
	}
	iptables.ReplaceTable(stack.FilterID, ipv4filter, false)

	// IPv4 NAT: SNAT TCP and UDP in postrouting.
	ipv4nat := iptables.GetTable(stack.NATID, false)
	ipv4nat.Rules = []stack.Rule{
		{
			Filter: stack.IPHeaderFilter{Protocol: header.TCPProtocolNumber, CheckProtocol: true},
			Target: ipt.snatV4,
		},
		{
			Filter: stack.IPHeaderFilter{Protocol: header.UDPProtocolNumber, CheckProtocol: true},
			Target: ipt.snatV4,
		},
		{Target: &stack.DropTarget{NetworkProtocol: header.IPv4ProtocolNumber}},
		{Target: &stack.AcceptTarget{NetworkProtocol: header.IPv4ProtocolNumber}},
	}
	ipv4nat.BuiltinChains = [stack.NumHooks]int{
		stack.Prerouting: 3, stack.Input: 3, stack.Forward: stack.HookUnset,
		stack.Output: 3, stack.Postrouting: 0,
	}
	ipv4nat.Underflows = [stack.NumHooks]int{
		stack.Prerouting: 2, stack.Input: 2, stack.Forward: 2,
		stack.Output: 2, stack.Postrouting: 2,
	}
	iptables.ReplaceTable(stack.NATID, ipv4nat, false)

	// IPv6 filter: accept TCP and UDP, drop everything else.
	ipv6filter := iptables.GetTable(stack.FilterID, true)
	ipv6filter.Rules = []stack.Rule{
		{
			Filter: stack.IPHeaderFilter{Protocol: header.TCPProtocolNumber, CheckProtocol: true},
			Target: &stack.AcceptTarget{NetworkProtocol: header.IPv6ProtocolNumber},
		},
		{
			Filter: stack.IPHeaderFilter{Protocol: header.UDPProtocolNumber, CheckProtocol: true},
			Target: &stack.AcceptTarget{NetworkProtocol: header.IPv6ProtocolNumber},
		},
		{Target: &stack.DropTarget{NetworkProtocol: header.IPv6ProtocolNumber}},
		{Target: &stack.AcceptTarget{NetworkProtocol: header.IPv6ProtocolNumber}},
	}
	ipv6filter.BuiltinChains = [stack.NumHooks]int{
		stack.Prerouting: 0, stack.Input: 0, stack.Forward: 0,
		stack.Output: 0, stack.Postrouting: 3,
	}
	ipv6filter.Underflows = [stack.NumHooks]int{
		stack.Prerouting: 2, stack.Input: 2, stack.Forward: 2,
		stack.Output: 2, stack.Postrouting: 2,
	}
	iptables.ReplaceTable(stack.FilterID, ipv6filter, true)

	// IPv6 NAT: SNAT TCP and UDP in postrouting.
	ipv6nat := iptables.GetTable(stack.NATID, true)
	ipv6nat.Rules = []stack.Rule{
		{
			Filter: stack.IPHeaderFilter{Protocol: header.TCPProtocolNumber, CheckProtocol: true},
			Target: ipt.snatV6,
		},
		{
			Filter: stack.IPHeaderFilter{Protocol: header.UDPProtocolNumber, CheckProtocol: true},
			Target: ipt.snatV6,
		},
		{Target: &stack.DropTarget{NetworkProtocol: header.IPv6ProtocolNumber}},
		{Target: &stack.AcceptTarget{NetworkProtocol: header.IPv6ProtocolNumber}},
	}
	ipv6nat.BuiltinChains = [stack.NumHooks]int{
		stack.Prerouting: 3, stack.Input: 3, stack.Forward: stack.HookUnset,
		stack.Output: 3, stack.Postrouting: 0,
	}
	ipv6nat.Underflows = [stack.NumHooks]int{
		stack.Prerouting: 2, stack.Input: 2, stack.Forward: 2,
		stack.Output: 2, stack.Postrouting: 2,
	}
	iptables.ReplaceTable(stack.NATID, ipv6nat, true)

	return iptables
}
