//go:build linux

// Package egress implements CRD-driven egress routing and policy enforcement
// for sandbox network traffic.
package egress

import (
	"context"
	"fmt"
	"net"
	"net/netip"
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
type Router struct {
	table atomic.Pointer[routeTable]
	// defaultPolicy is the action for traffic matching no route.
	defaultPolicy clrkv1alpha1.EgressPolicy
}

// NewRouter creates a Router with the given default policy and an empty route table.
func NewRouter(defaultPolicy clrkv1alpha1.EgressPolicy) *Router {
	r := &Router{
		defaultPolicy: defaultPolicy,
	}
	r.table.Store(&routeTable{})
	return r
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

// DialContext implements the netstack.Dialer interface. It consults the routing
// table and applies the egress policy before dialing.
func (r *Router) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	dst, err := netip.ParseAddrPort(addr)
	if err != nil {
		return nil, fmt.Errorf("parsing destination address: %w", err)
	}

	var proto clrkv1alpha1.L4Protocol
	switch network {
	case "tcp", "tcp4", "tcp6":
		proto = clrkv1alpha1.L4ProtocolTCP
	case "udp", "udp4", "udp6":
		proto = clrkv1alpha1.L4ProtocolUDP
	}

	result := r.Route(ctx, dst, proto, dstNameFromContext(ctx))
	switch result.Action {
	case ActionDeny:
		return nil, fmt.Errorf("connection to %s denied by egress policy", addr)
	case ActionBackendRef:
		var d net.Dialer
		return d.DialContext(ctx, network, result.BackendAddr)
	default:
		var d net.Dialer
		return d.DialContext(ctx, network, addr)
	}
}

// Rebuild atomically swaps the route table with one compiled from the
// provided CRD objects. This is called by the config watcher on CRD changes.
func (r *Router) Rebuild(
	gateways []clrkv1alpha1.EgressGateway,
	l4Routes []clrkv1alpha1.EgressL4Route,
) {
	tbl := compileRouteTable(gateways, l4Routes)
	r.table.Store(tbl)
}
