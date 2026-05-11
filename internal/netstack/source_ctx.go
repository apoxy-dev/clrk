package netstack

import (
	"context"
	"net/netip"
)

// srcAddrKey carries the per-sandbox source IP of an intercepted TCP
// connection through the dial context. The IdentityDialer reads it
// per-dial to resolve per-dispatch state (InvocationID, Backends,
// Policy) against the RevisionStack's shared slot table — without
// this hop, a stack shared across sandboxes can't tell whose
// outbound connection it's currently handling.
type srcAddrKey struct{}

// WithSourceAddr attaches the intercepted connection's source IP to
// ctx. Empty / invalid IPs are no-ops so DaemonAgent dials (which
// have no per-attempt source attribution today) fall through cleanly.
func WithSourceAddr(ctx context.Context, src netip.Addr) context.Context {
	if !src.IsValid() {
		return ctx
	}
	return context.WithValue(ctx, srcAddrKey{}, src)
}

// SourceAddrFromContext returns the source IP attached by
// WithSourceAddr, or the zero netip.Addr if none.
func SourceAddrFromContext(ctx context.Context) netip.Addr {
	if v, ok := ctx.Value(srcAddrKey{}).(netip.Addr); ok {
		return v
	}
	return netip.Addr{}
}
