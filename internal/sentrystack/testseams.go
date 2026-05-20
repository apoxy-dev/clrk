package sentrystack

import "time"

// Cross-platform test seams for portable surfaces of this package
// (the dnsCache type — InitStr is exported in initstr.go). Linux-only
// seams live in testseams_linux.go alongside the gvisor-dependent
// types so this file compiles on darwin without pulling in gvisor.

// DNSCache aliases the unexported dnsCache so tests in
// apoxy-cloud//clrk/sentrystack/ can reference the type by name. The
// methods (Bind, Lookup, IngestResponse) are already uppercase on the
// underlying type, so callers just invoke them directly.
type DNSCache = dnsCache

// NewDNSCacheForTest builds a fresh dnsCache and overrides its clock
// with `now`. Used by tests that pin time deterministically to assert
// TTL clamping and expiry-driven eviction without sleeping.
func NewDNSCacheForTest(now func() time.Time) *DNSCache {
	c := newDNSCache()
	if now != nil {
		c.now = now
	}
	return c
}

// DNSCacheCapForTest returns the LRU capacity. Tests rely on it to
// drive eviction without hardcoding the constant on the call site.
func DNSCacheCapForTest() int {
	return dnsCacheCapacity
}
