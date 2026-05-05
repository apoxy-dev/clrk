//go:build linux

package egress

import (
	"net"
	"net/netip"
	"strings"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// routeEntry is a single compiled routing rule.
type routeEntry struct {
	// Match criteria.
	cidrs     []netip.Prefix
	hostnames []string // exact ("api.openai.com") or wildcard ("*.openai.com")
	ports     []portRange
	protocol  *clrkv1alpha1.L4Protocol

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

// match finds the first matching route for the given destination,
// protocol, and DNS-bound destination name. dstName is the hostname
// the agent's resolver answered for dst.Addr (see DNSCache); empty
// when nothing was bound (direct-IP dial, DNS bypass, or expired
// entry), in which case hostname-only rules cannot match.
// Returns nil if no route matches.
func (t *routeTable) match(dst netip.AddrPort, proto clrkv1alpha1.L4Protocol, dstName string) *RouteResult {
	for i := range t.entries {
		if t.entries[i].matches(dst, proto, dstName) {
			r := t.entries[i].result
			return &r
		}
	}
	return nil
}

// matches checks whether the given destination, protocol, and bound
// DNS name match this entry. CIDR and hostname criteria are OR'd
// within a single match block: either CIDR or hostname is enough.
func (e *routeEntry) matches(dst netip.AddrPort, proto clrkv1alpha1.L4Protocol, dstName string) bool {
	// Protocol filter.
	if e.protocol != nil && *e.protocol != proto {
		return false
	}

	// Either CIDR or hostname must match within the block.
	if len(e.cidrs) > 0 || len(e.hostnames) > 0 {
		dstMatch := false
		for _, cidr := range e.cidrs {
			if cidr.Contains(dst.Addr()) {
				dstMatch = true
				break
			}
		}
		if !dstMatch && dstName != "" {
			for _, h := range e.hostnames {
				if hostnameMatches(h, dstName) {
					dstMatch = true
					break
				}
			}
		}
		if !dstMatch {
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

// hostnameMatches reports whether candidate satisfies the rule.
// Exact form ("api.openai.com") requires equality; leading-star
// wildcard ("*.openai.com") matches exactly one extra label per
// RFC 4592 / gwapiv1.Hostname — `*.openai.com` matches
// `api.openai.com` but not `openai.com` and not `foo.bar.openai.com`.
func hostnameMatches(rule, candidate string) bool {
	if strings.HasPrefix(rule, "*.") {
		suffix := rule[1:]
		if !strings.HasSuffix(candidate, suffix) {
			return false
		}
		return strings.Count(candidate, ".") == strings.Count(rule, ".")
	}
	return rule == candidate
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

				// Hostnames — exact + leading-wildcard, matched against
				// the DNS-bound destination name on TCP connect (see
				// DNSCache). On TLS listeners SNI matching takes over;
				// see internal/egextension for that path.
				for _, h := range m.DestinationHostnames {
					entry.hostnames = append(entry.hostnames, string(h))
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
