package extproc

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	oteltrace "go.opentelemetry.io/otel/trace"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// otlpSink fans out each captured Record to two OTLP signals:
//
//   - Logs: one slim summary record per HTTP transaction. Body is a
//     human-readable one-liner; attributes carry agent identity, the
//     core HTTP fields, and the trace_id/span_id of the matching span
//     so log/trace backends can hyperlink between them.
//   - Traces: one span per HTTP transaction with start/end clamped to
//     the actual request/response wall-clock window. Headers and
//     bodies (subject to CaptureBody bounds) ride as span events,
//     timestamped at the corresponding ext_proc callback.
//
// Both signals share one OTLP/HTTP endpoint today (see APO-554 for
// per-signal endpoint splitting).
type otlpSink struct {
	logger     otellog.Logger
	tracer     oteltrace.Tracer
	propagator propagation.TextMapPropagator
}

// newOTLPSink builds OTLP/HTTP exporters for both logs and traces
// pointed at spec.Endpoint and returns a Sink plus a shutdown function
// that flushes + closes both pipelines on EG removal or process exit.
func newOTLPSink(ctx context.Context, spec clrkv1alpha1.OTLPLogsSinkSpec) (Sink, func(context.Context) error, error) {
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName("clrk-egress"),
		attribute.String("clrk.component", "extproc"),
	)

	logsExp, err := otlploghttp.New(ctx, logsExporterOptions(spec)...)
	if err != nil {
		return nil, nil, fmt.Errorf("construct otlploghttp exporter: %w", err)
	}
	logsProvider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logsExp)),
		sdklog.WithResource(res),
	)

	tracesExp, err := otlptrace.New(ctx, otlptracehttp.NewClient(tracesExporterOptions(spec)...))
	if err != nil {
		_ = logsProvider.Shutdown(ctx)
		return nil, nil, fmt.Errorf("construct otlptracehttp exporter: %w", err)
	}
	tracesProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(tracesExp),
		sdktrace.WithResource(res),
	)

	sink := &otlpSink{
		logger:     logsProvider.Logger("github.com/apoxy-dev/clrk/internal/extproc"),
		tracer:     tracesProvider.Tracer("github.com/apoxy-dev/clrk/internal/extproc"),
		propagator: propagation.TraceContext{},
	}
	shutdown := func(ctx context.Context) error {
		// Flush both providers; combine errors so callers see anything
		// that went wrong without dropping signals on the floor.
		var errs []string
		if err := tracesProvider.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Sprintf("traces: %v", err))
		}
		if err := logsProvider.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Sprintf("logs: %v", err))
		}
		if len(errs) == 0 {
			return nil
		}
		return fmt.Errorf("otlp sink shutdown: %s", strings.Join(errs, "; "))
	}
	return sink, shutdown, nil
}

func logsExporterOptions(spec clrkv1alpha1.OTLPLogsSinkSpec) []otlploghttp.Option {
	opts := []otlploghttp.Option{otlploghttp.WithEndpointURL(spec.Endpoint)}
	if len(spec.Headers) > 0 {
		opts = append(opts, otlploghttp.WithHeaders(spec.Headers))
	}
	if isInsecureEndpoint(spec.Endpoint) {
		opts = append(opts, otlploghttp.WithInsecure())
	}
	return opts
}

func tracesExporterOptions(spec clrkv1alpha1.OTLPLogsSinkSpec) []otlptracehttp.Option {
	opts := []otlptracehttp.Option{otlptracehttp.WithEndpointURL(spec.Endpoint)}
	if len(spec.Headers) > 0 {
		opts = append(opts, otlptracehttp.WithHeaders(spec.Headers))
	}
	if isInsecureEndpoint(spec.Endpoint) {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	return opts
}

func isInsecureEndpoint(endpoint string) bool {
	return strings.HasPrefix(strings.ToLower(endpoint), "http://")
}

func (s *otlpSink) Emit(r Record) {
	span := s.emitSpan(r)
	s.emitLog(r, span.SpanContext())
	span.End(oteltrace.WithTimestamp(spanEnd(r)))
}

// emitSpan starts a span for the captured transaction, populates its
// attributes + events, and returns it un-Ended so the caller can stamp
// the end timestamp after emitting the back-linked log record.
func (s *otlpSink) emitSpan(r Record) oteltrace.Span {
	parentCtx := s.extractParent(context.Background(), r.RequestHeaders)

	method := r.RequestHeaders[":method"]
	authority := r.RequestHeaders[":authority"]
	scheme := r.RequestHeaders[":scheme"]
	if scheme == "" {
		scheme = "https"
	}
	path := r.RequestHeaders[":path"]
	status := parseStatus(r.ResponseHeaders[":status"])

	host, port := splitHostPort(authority)
	urlFull := buildURL(scheme, authority, path)
	spanName := spanNameFor(method, host, path)

	startOpts := []oteltrace.SpanStartOption{
		oteltrace.WithTimestamp(spanStart(r)),
		oteltrace.WithSpanKind(oteltrace.SpanKindServer),
		oteltrace.WithAttributes(
			attribute.String("agent.kind", r.AgentKind),
			attribute.String("agent.namespace", r.AgentNamespace),
			attribute.String("agent.name", r.AgentName),
			attribute.String("agent.uid", r.AgentUID),
			attribute.String("agent.revision", r.AgentRevision),
			attribute.String("invocation.id", r.InvocationID),
			attribute.String("http.request.method", method),
			attribute.String("server.address", host),
			attribute.Int("server.port", port),
			attribute.String("url.scheme", scheme),
			attribute.String("url.path", path),
			attribute.String("url.full", urlFull),
			attribute.Int("http.response.status_code", status),
			attribute.Int("clrk.req.bytes", len(r.RequestBody)),
			attribute.Int("clrk.resp.bytes", len(r.ResponseBody)),
			attribute.Bool("clrk.req.truncated", r.RequestTruncated),
			attribute.Bool("clrk.resp.truncated", r.ResponseTruncated),
		),
	}

	_, span := s.tracer.Start(parentCtx, spanName, startOpts...)

	// Span events for each phase. Header events flatten the captured
	// header map onto the event as `http.<dir>.header.<lowercase>`
	// attributes (multi-value headers are joined with ", " — there is
	// no list-of-string attribute kind that backends agree on). Body
	// events carry the captured bytes base64'd plus a truncation flag.
	if !r.RequestHeadersAt.IsZero() {
		span.AddEvent("http.request.headers",
			oteltrace.WithTimestamp(r.RequestHeadersAt),
			oteltrace.WithAttributes(headerAttrs("http.request.header.", r.RequestHeaders)...))
	}
	if !r.RequestBodyAt.IsZero() && len(r.RequestBody) > 0 {
		span.AddEvent("http.request.body",
			oteltrace.WithTimestamp(r.RequestBodyAt),
			oteltrace.WithAttributes(bodyAttrs(r.RequestBody, r.RequestTruncated)...))
	}
	if !r.ResponseHeadersAt.IsZero() {
		span.AddEvent("http.response.headers",
			oteltrace.WithTimestamp(r.ResponseHeadersAt),
			oteltrace.WithAttributes(headerAttrs("http.response.header.", r.ResponseHeaders)...))
	}
	if !r.ResponseBodyAt.IsZero() && len(r.ResponseBody) > 0 {
		span.AddEvent("http.response.body",
			oteltrace.WithTimestamp(r.ResponseBodyAt),
			oteltrace.WithAttributes(bodyAttrs(r.ResponseBody, r.ResponseTruncated)...))
	}

	if status >= 400 {
		span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", status))
	}
	return span
}

// emitLog writes the slim summary log record. Agent + HTTP attrs are
// indexed for query; the body is a one-line human-readable summary.
// trace_id and span_id back-link to the matching span.
func (s *otlpSink) emitLog(r Record, sc oteltrace.SpanContext) {
	method := r.RequestHeaders[":method"]
	authority := r.RequestHeaders[":authority"]
	path := r.RequestHeaders[":path"]
	status := parseStatus(r.ResponseHeaders[":status"])

	var rec otellog.Record
	rec.SetTimestamp(r.Timestamp)
	rec.SetSeverity(severityFor(status))
	rec.SetBody(otellog.StringValue(summaryLine(r, method, authority, path, status)))
	attrs := []otellog.KeyValue{
		otellog.String("agent.kind", r.AgentKind),
		otellog.String("agent.namespace", r.AgentNamespace),
		otellog.String("agent.name", r.AgentName),
		otellog.String("agent.uid", r.AgentUID),
		otellog.String("agent.revision", r.AgentRevision),
		otellog.String("invocation.id", r.InvocationID),
		otellog.String("http.request.method", method),
		otellog.String("server.address", authority),
		otellog.String("url.path", path),
		otellog.Int("http.response.status_code", status),
		otellog.Int("clrk.req.bytes", len(r.RequestBody)),
		otellog.Int("clrk.resp.bytes", len(r.ResponseBody)),
		otellog.Bool("clrk.req.truncated", r.RequestTruncated),
		otellog.Bool("clrk.resp.truncated", r.ResponseTruncated),
	}
	if dur := durationMillis(r); dur >= 0 {
		attrs = append(attrs, otellog.Int("clrk.duration_ms", dur))
	}
	if sc.HasTraceID() {
		attrs = append(attrs,
			otellog.String("trace_id", sc.TraceID().String()),
			otellog.String("span_id", sc.SpanID().String()),
		)
	}
	rec.AddAttributes(attrs...)
	s.logger.Emit(context.Background(), rec)
}

// extractParent reads a W3C `traceparent` header off the captured
// request and uses it as the parent SpanContext for our span. When no
// parent is present we emit a root span; in either case we never
// rewrite traceparent on the wire — the upstream sees whatever the
// agent emitted unchanged.
func (s *otlpSink) extractParent(ctx context.Context, headers map[string]string) context.Context {
	if len(headers) == 0 {
		return ctx
	}
	carrier := propagation.MapCarrier{}
	if v, ok := headers["traceparent"]; ok && v != "" {
		carrier["traceparent"] = v
	}
	if v, ok := headers["tracestate"]; ok && v != "" {
		carrier["tracestate"] = v
	}
	if len(carrier) == 0 {
		return ctx
	}
	return s.propagator.Extract(ctx, carrier)
}

// spanStart picks the most accurate start timestamp available.
// Prefer RequestHeadersAt (when ext_proc first saw the request);
// fall back to Timestamp (stream start).
func spanStart(r Record) time.Time {
	if !r.RequestHeadersAt.IsZero() {
		return r.RequestHeadersAt
	}
	return r.Timestamp
}

// spanEnd is the latest meaningful timestamp we observed.
func spanEnd(r Record) time.Time {
	if !r.EndAt.IsZero() {
		return r.EndAt
	}
	if !r.ResponseBodyAt.IsZero() {
		return r.ResponseBodyAt
	}
	if !r.ResponseHeadersAt.IsZero() {
		return r.ResponseHeadersAt
	}
	return time.Now()
}

func durationMillis(r Record) int {
	start := spanStart(r)
	end := spanEnd(r)
	if start.IsZero() || end.IsZero() || !end.After(start) {
		return -1
	}
	return int(end.Sub(start) / time.Millisecond)
}

func headerAttrs(prefix string, headers map[string]string) []attribute.KeyValue {
	if len(headers) == 0 {
		return nil
	}
	out := make([]attribute.KeyValue, 0, len(headers))
	for k, v := range headers {
		// Authorization-like headers carry credentials; redact rather
		// than ship to telemetry. The body capture path stays
		// untouched — operators opt into that explicitly via
		// CaptureBody.
		if isSensitiveHeader(k) {
			v = "[redacted]"
		}
		out = append(out, attribute.String(prefix+strings.ToLower(k), v))
	}
	return out
}

func bodyAttrs(body []byte, truncated bool) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int("clrk.body.bytes", len(body)),
		attribute.Bool("clrk.body.truncated", truncated),
		attribute.String("clrk.body.b64", base64.StdEncoding.EncodeToString(body)),
	}
}

// isSensitiveHeader matches header names whose values shouldn't ride
// in telemetry. Conservative list — extend cautiously.
func isSensitiveHeader(name string) bool {
	switch strings.ToLower(name) {
	case "authorization", "proxy-authorization", "cookie", "set-cookie",
		"x-api-key", "x-auth-token", "openai-api-key", "anthropic-api-key":
		return true
	}
	return false
}

func severityFor(status int) otellog.Severity {
	switch {
	case status == 0:
		return otellog.SeverityWarn
	case status >= 500:
		return otellog.SeverityError
	case status >= 400:
		return otellog.SeverityWarn
	default:
		return otellog.SeverityInfo
	}
}

func summaryLine(r Record, method, authority, path string, status int) string {
	dur := durationMillis(r)
	durTxt := "?"
	if dur >= 0 {
		durTxt = strconv.Itoa(dur) + "ms"
	}
	return fmt.Sprintf("%s %s%s %d %s req=%dB resp=%dB",
		method, authority, path, status, durTxt, len(r.RequestBody), len(r.ResponseBody))
}

func spanNameFor(method, host, path string) string {
	if method == "" && host == "" {
		return "HTTP"
	}
	if method == "" {
		method = "REQUEST"
	}
	if host == "" {
		host = path
	}
	return method + " " + host
}

func buildURL(scheme, authority, path string) string {
	if authority == "" {
		return ""
	}
	u := url.URL{Scheme: scheme, Host: authority, Path: path}
	return u.String()
}

func splitHostPort(authority string) (string, int) {
	if authority == "" {
		return "", 0
	}
	if i := strings.LastIndex(authority, ":"); i > 0 {
		host := authority[:i]
		if p, err := strconv.Atoi(authority[i+1:]); err == nil {
			return host, p
		}
	}
	return authority, 0
}

func parseStatus(s string) int {
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}
