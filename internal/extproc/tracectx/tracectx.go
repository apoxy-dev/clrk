// Package tracectx is the shared W3C trace-context utility used by
// both the ingress and egress ext_proc filters: it owns the singleton
// TextMapPropagator, the sensitive-header redaction list, and the
// helpers that extract a parent context from inbound headers / build
// a child traceparent for outbound injection.
//
// Lives in its own package so the two ext_proc directions don't
// reach into each other's internals just to share a propagator.
package tracectx

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// HeaderTraceparent / HeaderTracestate are the lowercased W3C header
// names callers should look for in normalized header maps. ext_proc
// pre-lowercases keys via headersToMap.
const (
	HeaderTraceparent = "traceparent"
	HeaderTracestate  = "tracestate"
)

// Propagator is the package-level W3C TextMapPropagator. Stateless;
// safe to share across goroutines.
var Propagator propagation.TextMapPropagator = propagation.TraceContext{}

// Extract threads any inbound W3C traceparent through as the span's
// parent. Returns ctx unchanged when no traceparent header is set,
// so callers can unconditionally pass the result into tracer.Start.
func Extract(ctx context.Context, headers map[string]string) context.Context {
	if _, ok := headers[HeaderTraceparent]; !ok {
		return ctx
	}
	return Propagator.Extract(ctx, propagation.MapCarrier(headers))
}

// Inject builds the traceparent and tracestate header values that
// represent the given span context as a parent for an outbound
// request. The returned tracestate is empty when sc carries no
// vendor state — caller should omit the header in that case.
//
// Used by the egress filter to stamp a parent on outbound LLM/MCP
// calls when the agent itself didn't set one. The caller is
// responsible for materializing these strings into Envoy header
// mutations.
func Inject(sc trace.SpanContext) (traceparent, tracestate string) {
	if !sc.IsValid() {
		return "", ""
	}
	carrier := propagation.MapCarrier{}
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	Propagator.Inject(ctx, carrier)
	return carrier.Get(HeaderTraceparent), carrier.Get(HeaderTracestate)
}

// IsSensitiveHeader reports whether a (lowercased) header name is in
// the conservative redact list. Header capture paths replace the
// value with "[redacted]" before exporting telemetry.
func IsSensitiveHeader(name string) bool {
	_, ok := sensitiveHeaders[name]
	return ok
}

// sensitiveHeaders is the conservative redact list. Keys are
// lowercase to match the form headersToMap produces. Extend
// cautiously — over-redacting silently hides telemetry operators may
// need.
var sensitiveHeaders = map[string]struct{}{
	"authorization":       {},
	"proxy-authorization": {},
	"cookie":              {},
	"set-cookie":          {},
	"x-api-key":           {},
	"x-auth-token":        {},
	"openai-api-key":      {},
	"anthropic-api-key":   {},
}
