// The DNS cache is pure Go and shared between the Sentry-side UDP
// forwarder (writer) and the Sentry-side TCP forwarder (reader). It
// has no gvisor dependencies — leave it without a //go:build constraint
// so cross-platform unit tests in apoxy-cloud//clrk can exercise it.
package sentrystack

import (
	"container/list"
	"net/netip"
	"sync"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

// Per-sandbox DNS-answer cache: resolved IP → name. Populated by the
// UDP forwarder on the UDP/53 response path; consulted on TCP connect
// to attach the agent's stated intent (the qname they asked for) to
// PROXY v2 frames and L4 telemetry.

const (
	dnsCacheCapacity = 4096

	// TTL clamps. A 0-TTL response still binds for the floor so
	// transient connections land before the entry vanishes; a
	// pathological 86400-TTL upstream answer doesn't pin a stale
	// name forever.
	dnsTTLFloor   = 5 * time.Second
	dnsTTLCeiling = 10 * time.Minute
)

// dnsCache is a bounded LRU keyed by resolved destination IP.
type dnsCache struct {
	cap int
	now func() time.Time

	mu   sync.Mutex
	lru  *list.List
	byIP map[netip.Addr]*list.Element
}

type dnsEntry struct {
	ip        netip.Addr
	name      string
	expiresAt time.Time
}

func newDNSCache() *dnsCache {
	return &dnsCache{
		cap:  dnsCacheCapacity,
		lru:  list.New(),
		byIP: make(map[netip.Addr]*list.Element),
		now:  time.Now,
	}
}

// Bind associates resolvedIP → name with ttl clamped to
// [dnsTTLFloor, dnsTTLCeiling]. Last write wins on collision.
func (c *dnsCache) Bind(resolvedIP netip.Addr, name string, ttl time.Duration) {
	if !resolvedIP.IsValid() || name == "" {
		return
	}
	if ttl < dnsTTLFloor {
		ttl = dnsTTLFloor
	}
	if ttl > dnsTTLCeiling {
		ttl = dnsTTLCeiling
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	expires := c.now().Add(ttl)
	if el, ok := c.byIP[resolvedIP]; ok {
		ent := el.Value.(*dnsEntry)
		ent.name = name
		ent.expiresAt = expires
		c.lru.MoveToFront(el)
		return
	}

	ent := &dnsEntry{ip: resolvedIP, name: name, expiresAt: expires}
	el := c.lru.PushFront(ent)
	c.byIP[resolvedIP] = el

	if c.lru.Len() > c.cap {
		oldest := c.lru.Back()
		if oldest != nil {
			old := oldest.Value.(*dnsEntry)
			delete(c.byIP, old.ip)
			c.lru.Remove(oldest)
		}
	}
}

// Lookup returns the bound name for resolvedIP, or "" if no live
// binding exists. Expired entries are pruned lazily on access.
func (c *dnsCache) Lookup(resolvedIP netip.Addr) string {
	if !resolvedIP.IsValid() {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.byIP[resolvedIP]
	if !ok {
		return ""
	}
	ent := el.Value.(*dnsEntry)
	if c.now().After(ent.expiresAt) {
		delete(c.byIP, ent.ip)
		c.lru.Remove(el)
		return ""
	}
	c.lru.MoveToFront(el)
	return ent.name
}

// IngestResponse parses a DNS response and binds every A/AAAA answer
// IP to the agent's stated intent — the qname they asked for, even
// when the answer arrived through a CNAME chain. The qname is always
// the *last* Bind so last-write-wins surfaces it to Lookup. Errors
// and malformed messages are silently dropped so a busted reply
// never disrupts DNS forwarding.
func (c *dnsCache) IngestResponse(msg []byte) {
	var p dnsmessage.Parser
	if _, err := p.Start(msg); err != nil {
		return
	}

	var qnames []string
	for {
		q, err := p.Question()
		if err == dnsmessage.ErrSectionDone {
			break
		}
		if err != nil {
			return
		}
		qnames = append(qnames, stripTrailingDot(q.Name.String()))
	}

	type aRR struct {
		name string
		ip   netip.Addr
		ttl  uint32
	}
	var ips []aRR
	var cnames map[string]string

	for {
		hdr, err := p.AnswerHeader()
		if err == dnsmessage.ErrSectionDone {
			break
		}
		if err != nil {
			return
		}
		name := stripTrailingDot(hdr.Name.String())
		switch hdr.Type {
		case dnsmessage.TypeA:
			r, err := p.AResource()
			if err != nil {
				return
			}
			ips = append(ips, aRR{name: name, ip: netip.AddrFrom4(r.A), ttl: hdr.TTL})
		case dnsmessage.TypeAAAA:
			r, err := p.AAAAResource()
			if err != nil {
				return
			}
			ips = append(ips, aRR{name: name, ip: netip.AddrFrom16(r.AAAA), ttl: hdr.TTL})
		case dnsmessage.TypeCNAME:
			r, err := p.CNAMEResource()
			if err != nil {
				return
			}
			if cnames == nil {
				cnames = make(map[string]string, 4)
			}
			cnames[name] = stripTrailingDot(r.CNAME.String())
		default:
			if err := p.SkipAnswer(); err != nil {
				return
			}
		}
	}

	// Bind in oldest-first order so the qname (the agent's actual
	// intent) ends up as the most recent — and therefore winning —
	// entry under last-write-wins:
	//
	//   1. Each A/AAAA record's own name.
	//   2. CNAME aliases on the chain qname → ... → A-record-name,
	//      from terminal end inward.
	//   3. The qname itself, last.
	//
	// Chain-depth cap defuses pathological cycles.
	for _, a := range ips {
		ttl := time.Duration(a.ttl) * time.Second
		c.Bind(a.ip, a.name, ttl)

		for _, qn := range qnames {
			var chain []string
			cur := qn
			for hops := 0; hops < 10; hops++ {
				if cur == a.name {
					for i := len(chain) - 1; i >= 0; i-- {
						c.Bind(a.ip, chain[i], ttl)
					}
					c.Bind(a.ip, qn, ttl)
					break
				}
				chain = append(chain, cur)
				next, ok := cnames[cur]
				if !ok {
					break
				}
				cur = next
			}
		}
	}
}

func stripTrailingDot(s string) string {
	if len(s) > 0 && s[len(s)-1] == '.' {
		return s[:len(s)-1]
	}
	return s
}
