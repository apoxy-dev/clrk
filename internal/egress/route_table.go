//go:build linux

package egress

import (
	"net"
	"net/netip"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// routeEntry is a single compiled routing rule.
type routeEntry struct {
	// Match criteria.
	cidrs    []netip.Prefix
	ports    []portRange
	protocol *clrkv1alpha1.L4Protocol

	// Result to return on match.
	result RouteResult
}

// portRange is an inclusive port range.
type portRange struct {
	start uint16
	end   uint16
}

// routeTable is an immutable, compiled set of routing rules.
type routeTable struct {
	entries []routeEntry
}

// match finds the first matching route for the given destination and protocol.
// Returns nil if no route matches.
func (t *routeTable) match(dst netip.AddrPort, proto clrkv1alpha1.L4Protocol) *RouteResult {
	for i := range t.entries {
		if t.entries[i].matches(dst, proto) {
			r := t.entries[i].result
			return &r
		}
	}
	return nil
}

// matches checks whether the given destination and protocol match this entry.
func (e *routeEntry) matches(dst netip.AddrPort, proto clrkv1alpha1.L4Protocol) bool {
	// Protocol filter.
	if e.protocol != nil && *e.protocol != proto {
		return false
	}

	// CIDR filter.
	if len(e.cidrs) > 0 {
		matched := false
		for _, cidr := range e.cidrs {
			if cidr.Contains(dst.Addr()) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Port filter.
	if len(e.ports) > 0 {
		matched := false
		port := dst.Port()
		for _, pr := range e.ports {
			if port >= pr.start && port <= pr.end {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	return true
}

// compileRouteTable builds a routeTable from CRD objects.
func compileRouteTable(
	_ []clrkv1alpha1.EgressGateway,
	l4Routes []clrkv1alpha1.EgressL4Route,
) *routeTable {
	var entries []routeEntry

	for _, route := range l4Routes {
		for _, rule := range route.Spec.Rules {
			for _, m := range rule.Matches {
				entry := routeEntry{
					result: RouteResult{Action: ActionPassthrough},
				}

				// Parse CIDRs.
				for _, cidrStr := range m.DestinationCIDRs {
					_, ipNet, err := net.ParseCIDR(cidrStr)
					if err != nil {
						continue
					}
					prefix, ok := netip.AddrFromSlice(ipNet.IP)
					if !ok {
						continue
					}
					ones, _ := ipNet.Mask.Size()
					entry.cidrs = append(entry.cidrs, netip.PrefixFrom(prefix.Unmap(), ones))
				}

				// Parse ports.
				for _, pm := range m.Ports {
					if pm.Port != nil {
						p := uint16(*pm.Port)
						entry.ports = append(entry.ports, portRange{start: p, end: p})
					} else if pm.StartPort != nil && pm.EndPort != nil {
						entry.ports = append(entry.ports, portRange{
							start: uint16(*pm.StartPort),
							end:   uint16(*pm.EndPort),
						})
					}
				}

				// Protocol.
				if m.Protocol != nil {
					entry.protocol = m.Protocol
				}

				// Backend refs.
				if len(rule.BackendRefs) > 0 {
					entry.result.Action = ActionBackendRef
					// Use the first backend ref for now.
					ref := rule.BackendRefs[0]
					entry.result.BackendAddr = string(ref.Name)
				}

				entries = append(entries, entry)
			}
		}
	}

	return &routeTable{entries: entries}
}
