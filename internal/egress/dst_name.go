package egress

import "context"

// dstNameKey carries a DNS-bound destination name through the dial
// chain. Under sentrystack the in-Sentry forwarder emits PROXY v2
// TLVDstName directly, so the worker-side Router only reads this for
// completeness — currently always empty, preserved as a hook for the
// future urpc-fed router-side enforcement path.
type dstNameKey struct{}

// dstNameFromContext returns the name attached to ctx, or "".
func dstNameFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(dstNameKey{}).(string); ok {
		return v
	}
	return ""
}
