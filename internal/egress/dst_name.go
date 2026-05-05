//go:build linux

package egress

import "context"

// dstNameKey carries a DNS-bound destination name (the agent's stated
// intent for the connection, derived from the per-sandbox DNS-answer
// cache) through the dial chain. IdentityDialer attaches it on
// outbound TCP dials so Router can consult it for hostname-based
// route matching without changing the netstack.Dialer interface.
type dstNameKey struct{}

// withDstName attaches name to ctx; empty is a no-op.
func withDstName(ctx context.Context, name string) context.Context {
	if name == "" {
		return ctx
	}
	return context.WithValue(ctx, dstNameKey{}, name)
}

// dstNameFromContext returns the name attached by withDstName, or "".
func dstNameFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(dstNameKey{}).(string); ok {
		return v
	}
	return ""
}
