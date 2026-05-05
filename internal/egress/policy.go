package egress

import (
	"net/netip"
	"sync/atomic"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// SandboxPolicy is the per-EgressGateway authorization decision plane.
// It pairs the EG's DefaultPolicy with the routes that target it and
// answers Allow for each outbound dial. Held by every IdentityDialer
// bound to that EG; the router hot-swaps the underlying state on
// CRD changes so existing sandboxes pick up route/policy edits without
// restart.
type SandboxPolicy struct {
	state atomic.Pointer[policyState]
}

// policyState is the immutable snapshot the SandboxPolicy points at.
// Replaced in-place via SandboxPolicy.update on Rebuild.
type policyState struct {
	routes        *routeTable
	defaultPolicy clrkv1alpha1.EgressPolicy
}

// newSandboxPolicy returns a policy seeded with the given state.
func newSandboxPolicy(st *policyState) *SandboxPolicy {
	p := &SandboxPolicy{}
	p.state.Store(st)
	return p
}

// update atomically swaps the SandboxPolicy's underlying state.
func (p *SandboxPolicy) update(st *policyState) {
	p.state.Store(st)
}

// Allow reports whether a dial to dst with the given protocol and
// DNS-bound name is permitted under this policy. A route match (by
// CIDR, hostname, port, or protocol) is allow; otherwise the EG's
// DefaultPolicy decides. A nil receiver allows everything — used for
// agents with no EgressRefs (no MITM, no enforced policy).
func (p *SandboxPolicy) Allow(dst netip.AddrPort, proto clrkv1alpha1.L4Protocol, dstName string) bool {
	if p == nil {
		return true
	}
	s := p.state.Load()
	if s == nil {
		return true
	}
	if s.routes != nil && s.routes.match(dst, proto, dstName) != nil {
		return true
	}
	return s.defaultPolicy != clrkv1alpha1.EgressPolicyDenyAll
}

// DefaultPolicy returns the captured DefaultPolicy. Useful for logging
// the reason a connection was denied.
func (p *SandboxPolicy) DefaultPolicy() clrkv1alpha1.EgressPolicy {
	if p == nil {
		return clrkv1alpha1.EgressPolicyAllowAll
	}
	s := p.state.Load()
	if s == nil {
		return clrkv1alpha1.EgressPolicyAllowAll
	}
	return s.defaultPolicy
}
