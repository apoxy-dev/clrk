// Package egress implements CRD-driven egress routing and policy enforcement
// for sandbox network traffic.
package egress

import (
	"context"
	"net/netip"
	"sync"
	"sync/atomic"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// RouteAction is the routing decision for a connection.
type RouteAction int

const (
	// ActionPassthrough dials the original destination directly.
	ActionPassthrough RouteAction = iota
	// ActionDeny rejects the connection (RST for TCP, drop for UDP).
	ActionDeny
	// ActionTLSTerminate terminates TLS for L7 inspection and re-encrypts.
	ActionTLSTerminate
	// ActionBackendRef routes through an explicit backend proxy.
	ActionBackendRef
)

// RouteResult holds the routing decision and associated metadata.
type RouteResult struct {
	Action      RouteAction
	BackendAddr string // Populated when Action == ActionBackendRef.
}

// Router is a compiled egress routing table built from CRD snapshots.
// It is safe for concurrent use. Call Route to get the action for a connection.
//
// In addition to the global routing table consulted by direct-dial
// callers, the router holds per-EgressGateway SandboxPolicy handles.
// Each per-sandbox IdentityDialer holds the handle for its bound EG
// and consults it before backend selection so default-deny is honored
// even on backend-routed traffic. The router updates these handles
// in-place on Rebuild so existing sandboxes pick up edits without
// restart.
type Router struct {
	table atomic.Pointer[routeTable]
	// defaultPolicy is the action for traffic matching no route.
	defaultPolicy clrkv1alpha1.EgressPolicy

	// policiesMu protects policies. The handles themselves are
	// hot-swapped via atomic so reads on the dial path are lock-free.
	policiesMu sync.Mutex
	policies   map[string]*SandboxPolicy
}

// NewRouter creates a Router with the given default policy and an empty route table.
func NewRouter(defaultPolicy clrkv1alpha1.EgressPolicy) *Router {
	r := &Router{
		defaultPolicy: defaultPolicy,
		policies:      make(map[string]*SandboxPolicy),
	}
	r.table.Store(&routeTable{})
	return r
}

// SyncPolicy registers or updates the SandboxPolicy for an EG, recompiled
// from the EG's DefaultPolicy and the routes (filtered from allL4Routes)
// whose ParentRefs target it. Returns the stable handle the
// IdentityDialer holds across CRD changes.
func (r *Router) SyncPolicy(eg clrkv1alpha1.EgressGateway, allL4Routes []clrkv1alpha1.EgressL4Route) *SandboxPolicy {
	targeted := routesForEG(allL4Routes, eg.Namespace, eg.Name)
	state := &policyState{
		routes:        compileRouteTable(nil, targeted),
		defaultPolicy: eg.Spec.DefaultPolicy,
	}
	key := eg.Namespace + "/" + eg.Name

	r.policiesMu.Lock()
	defer r.policiesMu.Unlock()
	if p, ok := r.policies[key]; ok {
		p.update(state)
		return p
	}
	p := newSandboxPolicy(state)
	r.policies[key] = p
	return p
}

// PolicyFor returns the SandboxPolicy registered for the EG, or nil if
// none has been synced yet. Callers normally use SyncPolicy directly.
func (r *Router) PolicyFor(egNamespace, egName string) *SandboxPolicy {
	r.policiesMu.Lock()
	defer r.policiesMu.Unlock()
	return r.policies[egNamespace+"/"+egName]
}

// Route returns the routing decision for the given connection
// parameters. dstName is the DNS-bound destination hostname (see
// DNSCache); empty when nothing was bound, in which case
// destinationHostnames rules cannot match.
func (r *Router) Route(_ context.Context, dst netip.AddrPort, proto clrkv1alpha1.L4Protocol, dstName string) *RouteResult {
	tbl := r.table.Load()
	if result := tbl.match(dst, proto, dstName); result != nil {
		return result
	}
	// No route matched — apply default policy.
	if r.defaultPolicy == clrkv1alpha1.EgressPolicyDenyAll {
		return &RouteResult{Action: ActionDeny}
	}
	return &RouteResult{Action: ActionPassthrough}
}

// Rebuild atomically swaps the route table with one compiled from the
// provided CRD objects, and refreshes every per-EG SandboxPolicy
// handle. This is called by the config watcher on CRD changes.
func (r *Router) Rebuild(
	gateways []clrkv1alpha1.EgressGateway,
	l4Routes []clrkv1alpha1.EgressL4Route,
) {
	r.table.Store(compileRouteTable(gateways, l4Routes))
	for _, eg := range gateways {
		r.SyncPolicy(eg, l4Routes)
	}
}
