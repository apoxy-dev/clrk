package extproc

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
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

// Attribute keys clrk publishes that don't live in OTel semconv.
// Anything semconv has a key for (http.*, server.*, url.*) is sourced
// from the semconv package directly.
//
// gen_ai.* are OTel GenAI semconv keys. semconv/v1.26.0 doesn't ship
// typed helpers for them yet, so we declare the strings here. Swap to
// typed helpers once the upstream package adds them.
const (
	attrAgentKind      = "agent.kind"
	attrAgentNamespace = "agent.namespace"
	attrAgentName      = "agent.name"
	attrAgentUID       = "agent.uid"
	attrAgentRevision  = "agent.revision"
	attrInvocationID   = "invocation.id"
	attrReqBytes       = "clrk.req.bytes"
	attrRespBytes      = "clrk.resp.bytes"
	attrReqTruncated   = "clrk.req.truncated"
	attrRespTruncated  = "clrk.resp.truncated"
	attrDurationMs     = "clrk.duration_ms"
	attrTraceID        = "trace_id"
	attrSpanID         = "span_id"
	attrBodyBytes      = "clrk.body.bytes"
	attrBodyTruncated  = "clrk.body.truncated"
	attrBodyB64        = "clrk.body.b64"

	attrGenAISystem        = "gen_ai.system"
	attrGenAIOperationName = "gen_ai.operation.name"
	attrGenAIRequestModel  = "gen_ai.request.model"
	attrGenAIResponseModel = "gen_ai.response.model"
	attrGenAIInputTokens   = "gen_ai.usage.input_tokens"
	attrGenAIOutputTokens  = "gen_ai.usage.output_tokens"
	attrGenAIStream        = "gen_ai.response.stream"

	attrAPRRouteMatched   = "clrk.aiproviderroute.matched"
	attrAPRRouteName      = "clrk.aiproviderroute.name"
	attrAPRRouteNamespace = "clrk.aiproviderroute.namespace"
	attrBodyUsageVisible  = "clrk.body.usage_visible"
	attrBodyReqRewritten  = "clrk.body.request_rewritten"

	attrBudgetDenied    = "clrk.budget.denied"
	attrBudgetDailyUsed = "clrk.budget.daily_used"
	attrBudgetDailyMax  = "clrk.budget.daily_max"

	attrL4BytesUpstream = "clrk.l4.bytes_upstream"
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
	// 1s flush keeps dev/TUI feedback snappy; the upstream defaults
	// (logs: 1s, traces: 5s) introduced a visible 5s gap between an
	// HTTP transaction completing and its span landing in the
	// otel-traces pane. Production OTLP collectors batch on their own
	// receiver side, so making the exporter chatty here doesn't change
	// what the backend ultimately stores.
	const dfltBatchTimeout = 1 * time.Second
	logsProvider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logsExp,
			sdklog.WithExportInterval(dfltBatchTimeout),
		)),
		sdklog.WithResource(res),
	)

	tracesExp, err := otlptrace.New(ctx, otlptracehttp.NewClient(tracesExporterOptions(spec)...))
	if err != nil {
		_ = logsProvider.Shutdown(ctx)
		return nil, nil, fmt.Errorf("construct otlptracehttp exporter: %w", err)
	}
	tracesProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(tracesExp,
			sdktrace.WithBatchTimeout(dfltBatchTimeout),
		),
		sdktrace.WithResource(res),
	)

	sink := &otlpSink{
		logger:     logsProvider.Logger("github.com/apoxy-dev/clrk/internal/extproc"),
		tracer:     tracesProvider.Tracer("github.com/apoxy-dev/clrk/internal/extproc"),
		propagator: propagation.TraceContext{},
	}
	shutdown := func(ctx context.Context) error {
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

// WithEndpointURL on both exporters already derives insecure-mode from
// the http:// scheme — no separate WithInsecure needed. We always
// append the signal-specific path (`/v1/logs`, `/v1/traces`) when the
// configured endpoint has no path or just `/` so users can write
// `Endpoint: http://otel-collector:4318` without remembering to
// enumerate per-signal suffixes. Existing endpoints that already
// include a non-trivial path are left untouched.

func logsExporterOptions(spec clrkv1alpha1.OTLPLogsSinkSpec) []otlploghttp.Option {
	opts := []otlploghttp.Option{otlploghttp.WithEndpointURL(endpointForSignal(spec.Endpoint, "/v1/logs"))}
	if len(spec.Headers) > 0 {
		opts = append(opts, otlploghttp.WithHeaders(spec.Headers))
	}
	return opts
}

func tracesExporterOptions(spec clrkv1alpha1.OTLPLogsSinkSpec) []otlptracehttp.Option {
	opts := []otlptracehttp.Option{otlptracehttp.WithEndpointURL(endpointForSignal(spec.Endpoint, "/v1/traces"))}
	if len(spec.Headers) > 0 {
		opts = append(opts, otlptracehttp.WithHeaders(spec.Headers))
	}
	return opts
}

// endpointForSignal appends the signal-specific path when the
// configured endpoint has no path or only the root `/`. Required
// because otlploghttp/otlptracehttp WithEndpointURL uses the URL's
// Path verbatim — a bare `http://host:4318` ends up POSTing to `/`,
// which collectors return 404 for.
func endpointForSignal(endpoint, signalPath string) string {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return endpoint
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = signalPath
		return u.String()
	}
	return endpoint
}

// derived caches the request/response fields parsed once per Emit so
// both the span and log paths read them without re-touching the
// header maps.
type derived struct {
	method    string
	authority string
	scheme    string
	path      string
	host      string
	port      int
	urlFull   string
	status    int
}

func newDerived(r Record) derived {
	method := r.RequestHeaders[":method"]
	authority := r.RequestHeaders[":authority"]
	scheme := r.RequestHeaders[":scheme"]
	if scheme == "" {
		scheme = "https"
	}
	path := r.RequestHeaders[":path"]
	host, port := splitAuthority(authority)
	return derived{
		method:    method,
		authority: authority,
		scheme:    scheme,
		path:      path,
		host:      host,
		port:      port,
		urlFull:   buildURL(scheme, authority, path),
		status:    parseStatus(r.ResponseHeaders[":status"]),
	}
}

func (s *otlpSink) Emit(r Record) {
	d := newDerived(r)
	span := s.emitSpan(r, d)
	s.emitLog(r, d, span.SpanContext())
	span.End(oteltrace.WithTimestamp(spanEnd(r)))
}

// EmitL4 emits one log + one span for a single TCP/TLS connection
// observed through the L4 ext_proc filter. Span name is "egress.tcp"
// — there's no HTTP method, no GenAI operation; the connection itself
// is the unit. Agent identity attributes are shared with the HTTP
// path so dashboards filtering by agent.* see both transports.
func (s *otlpSink) EmitL4(r L4Record) {
	start := r.Timestamp
	if start.IsZero() {
		start = time.Now()
	}
	end := r.EndAt
	if end.IsZero() || end.Before(start) {
		end = start
	}

	attrs := []attribute.KeyValue{
		attribute.String(attrAgentKind, r.AgentKind),
		attribute.String(attrAgentNamespace, r.AgentNamespace),
		attribute.String(attrAgentName, r.AgentName),
		attribute.String(attrAgentUID, r.AgentUID),
		attribute.String(attrAgentRevision, r.AgentRevision),
		attribute.String(attrInvocationID, r.InvocationID),
		attribute.Int64(attrL4BytesUpstream, r.BytesUpstream),
		attribute.Int(attrDurationMs, int(end.Sub(start)/time.Millisecond)),
	}
	_, span := s.tracer.Start(context.Background(), "egress.tcp",
		oteltrace.WithTimestamp(start),
		oteltrace.WithSpanKind(oteltrace.SpanKindServer),
		oteltrace.WithAttributes(attrs...),
	)
	span.End(oteltrace.WithTimestamp(end))

	var rec otellog.Record
	rec.SetTimestamp(start)
	rec.SetSeverity(otellog.SeverityInfo)
	rec.SetBody(otellog.StringValue(fmt.Sprintf("egress.tcp agent=%s/%s bytes_up=%dB dur=%dms",
		r.AgentNamespace, r.AgentName, r.BytesUpstream, int(end.Sub(start)/time.Millisecond))))
	logAttrs := []otellog.KeyValue{
		otellog.String(attrAgentKind, r.AgentKind),
		otellog.String(attrAgentNamespace, r.AgentNamespace),
		otellog.String(attrAgentName, r.AgentName),
		otellog.String(attrAgentUID, r.AgentUID),
		otellog.String(attrAgentRevision, r.AgentRevision),
		otellog.String(attrInvocationID, r.InvocationID),
		otellog.Int64(attrL4BytesUpstream, r.BytesUpstream),
		otellog.Int(attrDurationMs, int(end.Sub(start)/time.Millisecond)),
	}
	sc := span.SpanContext()
	if sc.HasTraceID() {
		logAttrs = append(logAttrs,
			otellog.String(attrTraceID, sc.TraceID().String()),
			otellog.String(attrSpanID, sc.SpanID().String()),
		)
	}
	rec.AddAttributes(logAttrs...)
	s.logger.Emit(context.Background(), rec)
}

// emitSpan returns the un-Ended span; caller stamps end after emitLog
// so the back-link log carries the matching trace_id/span_id.
func (s *otlpSink) emitSpan(r Record, d derived) oteltrace.Span {
	parentCtx := s.extractParent(context.Background(), r.RequestHeaders)
	spanName := spanNameFor(r, d)

	attrs := append(agentAttrs(r),
		semconv.HTTPRequestMethodKey.String(d.method),
		semconv.ServerAddress(d.host),
		semconv.ServerPort(d.port),
		semconv.URLScheme(d.scheme),
		semconv.URLPath(d.path),
		semconv.URLFull(d.urlFull),
		semconv.HTTPResponseStatusCode(d.status),
		attribute.Int(attrReqBytes, len(r.RequestBody)),
		attribute.Int(attrRespBytes, len(r.ResponseBody)),
		attribute.Bool(attrReqTruncated, r.RequestTruncated),
		attribute.Bool(attrRespTruncated, r.ResponseTruncated),
	)
	if r.RequestBodyRewritten {
		attrs = append(attrs, attribute.Bool(attrBodyReqRewritten, true))
	}
	attrs = appendGenAIAttrs(attrs, r)
	attrs = appendAPRAttrs(attrs, r)

	_, span := s.tracer.Start(parentCtx, spanName,
		oteltrace.WithTimestamp(spanStart(r)),
		oteltrace.WithSpanKind(oteltrace.SpanKindServer),
		oteltrace.WithAttributes(attrs...),
	)

	// Header events flatten the captured map onto the event with one
	// `http.<dir>.header.<lowercased-name>` attribute per header. Multi-
	// value headers were already collapsed upstream (`headersToMap`); we
	// can't preserve them as a list because no log/trace backend agrees
	// on a list-of-string attribute kind.
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

	if d.status >= 400 {
		span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", d.status))
	}
	return span
}

func (s *otlpSink) emitLog(r Record, d derived, sc oteltrace.SpanContext) {
	var rec otellog.Record
	rec.SetTimestamp(r.Timestamp)
	rec.SetSeverity(severityFor(d.status))
	rec.SetBody(otellog.StringValue(summaryLine(r, d)))

	attrs := make([]otellog.KeyValue, 0, 16)
	attrs = appendLogKVs(attrs, agentAttrs(r))
	attrs = append(attrs,
		otellog.String(string(semconv.HTTPRequestMethodKey), d.method),
		otellog.String(string(semconv.ServerAddressKey), d.authority),
		otellog.String(string(semconv.URLPathKey), d.path),
		otellog.Int(string(semconv.HTTPResponseStatusCodeKey), d.status),
		otellog.Int(attrReqBytes, len(r.RequestBody)),
		otellog.Int(attrRespBytes, len(r.ResponseBody)),
		otellog.Bool(attrReqTruncated, r.RequestTruncated),
		otellog.Bool(attrRespTruncated, r.ResponseTruncated),
	)
	if dur := durationMillis(r); dur >= 0 {
		attrs = append(attrs, otellog.Int(attrDurationMs, dur))
	}
	if r.RequestBodyRewritten {
		attrs = append(attrs, otellog.Bool(attrBodyReqRewritten, true))
	}
	if sc.HasTraceID() {
		attrs = append(attrs,
			otellog.String(attrTraceID, sc.TraceID().String()),
			otellog.String(attrSpanID, sc.SpanID().String()),
		)
	}
	attrs = appendLogKVs(attrs, genAIAttrs(r))
	attrs = appendLogKVs(attrs, aprAttrs(r))
	rec.AddAttributes(attrs...)
	s.logger.Emit(context.Background(), rec)
}

// genAIAttrs returns the gen_ai.* attribute slice for this record.
// Empty when the host wasn't a known AI provider. UsageVisible is
// only published as `clrk.body.usage_visible=false` when the response
// was truncated AND the parser couldn't find usage — those are the
// cases where bumping CaptureBody.MaxBytes would help.
func genAIAttrs(r Record) []attribute.KeyValue {
	if r.Provider == nil {
		return nil
	}
	p := r.Provider
	out := []attribute.KeyValue{
		attribute.String(attrGenAISystem, p.System),
	}
	if p.Operation != "" {
		out = append(out, attribute.String(attrGenAIOperationName, p.Operation))
	}
	if p.RequestModel != "" {
		out = append(out, attribute.String(attrGenAIRequestModel, p.RequestModel))
	}
	if p.ResponseModel != "" {
		out = append(out, attribute.String(attrGenAIResponseModel, p.ResponseModel))
	}
	if p.InputTokens > 0 {
		out = append(out, attribute.Int64(attrGenAIInputTokens, p.InputTokens))
	}
	if p.OutputTokens > 0 {
		out = append(out, attribute.Int64(attrGenAIOutputTokens, p.OutputTokens))
	}
	if p.StreamResponse {
		out = append(out, attribute.Bool(attrGenAIStream, true))
	}
	// Surface the visibility-of-usage signal only when it's actionable:
	// usage hidden behind truncation. Any other "absent" state (error
	// response, stream) doesn't benefit from raising the byte cap.
	if r.ResponseTruncated && !p.UsageVisible && !p.StreamResponse {
		out = append(out, attribute.Bool(attrBodyUsageVisible, false))
	}
	return out
}

// appendGenAIAttrs is the slice-append form genAIAttrs callers use to
// avoid one allocation when the record has provider info.
func appendGenAIAttrs(dst []attribute.KeyValue, r Record) []attribute.KeyValue {
	return append(dst, genAIAttrs(r)...)
}

func aprAttrs(r Record) []attribute.KeyValue {
	if r.MatchedRouteName == "" {
		return nil
	}
	out := []attribute.KeyValue{
		attribute.Bool(attrAPRRouteMatched, true),
		attribute.String(attrAPRRouteNamespace, r.MatchedRouteNamespace),
		attribute.String(attrAPRRouteName, r.MatchedRouteName),
	}
	if r.BudgetDenied {
		out = append(out,
			attribute.Bool(attrBudgetDenied, true),
			attribute.Int64(attrBudgetDailyUsed, r.BudgetDailyUsed),
			attribute.Int64(attrBudgetDailyMax, r.BudgetDailyMax),
		)
	}
	return out
}

func appendAPRAttrs(dst []attribute.KeyValue, r Record) []attribute.KeyValue {
	return append(dst, aprAttrs(r)...)
}

// extractParent threads any inbound W3C traceparent through as the
// span's parent. We never rewrite traceparent on the wire — the
// upstream sees whatever the agent emitted unchanged.
func (s *otlpSink) extractParent(ctx context.Context, headers map[string]string) context.Context {
	if _, ok := headers["traceparent"]; !ok {
		return ctx
	}
	return s.propagator.Extract(ctx, propagation.MapCarrier(headers))
}

// agentAttrs returns the identity sub-slice common to span + log.
func agentAttrs(r Record) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String(attrAgentKind, r.AgentKind),
		attribute.String(attrAgentNamespace, r.AgentNamespace),
		attribute.String(attrAgentName, r.AgentName),
		attribute.String(attrAgentUID, r.AgentUID),
		attribute.String(attrAgentRevision, r.AgentRevision),
		attribute.String(attrInvocationID, r.InvocationID),
	}
}

// appendLogKVs translates attribute.KeyValue (the trace SDK kind) into
// otellog.KeyValue (the log SDK kind) so we don't maintain two parallel
// agent-attr arrays.
func appendLogKVs(dst []otellog.KeyValue, src []attribute.KeyValue) []otellog.KeyValue {
	for _, kv := range src {
		switch kv.Value.Type() {
		case attribute.STRING:
			dst = append(dst, otellog.String(string(kv.Key), kv.Value.AsString()))
		case attribute.INT64:
			dst = append(dst, otellog.Int64(string(kv.Key), kv.Value.AsInt64()))
		case attribute.BOOL:
			dst = append(dst, otellog.Bool(string(kv.Key), kv.Value.AsBool()))
		default:
			dst = append(dst, otellog.String(string(kv.Key), kv.Value.Emit()))
		}
	}
	return dst
}

func spanStart(r Record) time.Time {
	if !r.RequestHeadersAt.IsZero() {
		return r.RequestHeadersAt
	}
	return r.Timestamp
}

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

// headerAttrs produces one attribute per header, prefixed for direction.
// Header keys are pre-lowercased by headersToMap, so we don't re-lower
// them here (and isSensitiveHeader operates on the same lowercase form).
func headerAttrs(prefix string, headers map[string]string) []attribute.KeyValue {
	if len(headers) == 0 {
		return nil
	}
	out := make([]attribute.KeyValue, 0, len(headers))
	for k, v := range headers {
		if isSensitiveHeader(k) {
			v = "[redacted]"
		}
		out = append(out, attribute.String(prefix+k, v))
	}
	return out
}

func bodyAttrs(body []byte, truncated bool) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int(attrBodyBytes, len(body)),
		attribute.Bool(attrBodyTruncated, truncated),
		attribute.String(attrBodyB64, base64.StdEncoding.EncodeToString(body)),
	}
}

// sensitiveHeaders is the conservative redact list. Keys are lowercase
// to match headersToMap. Extend cautiously — over-redacting silently
// hides telemetry operators may need.
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

func isSensitiveHeader(name string) bool {
	_, ok := sensitiveHeaders[name]
	return ok
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

func summaryLine(r Record, d derived) string {
	durTxt := "?"
	if dur := durationMillis(r); dur >= 0 {
		durTxt = strconv.Itoa(dur) + "ms"
	}
	base := fmt.Sprintf("%s %s%s %d %s req=%dB resp=%dB",
		d.method, d.authority, d.path, d.status, durTxt, len(r.RequestBody), len(r.ResponseBody))
	if r.Provider != nil {
		// Acceptance criterion line:
		//   provider=anthropic model=claude-3-5-sonnet input_tokens=42 output_tokens=128
		base += fmt.Sprintf(" provider=%s", r.Provider.System)
		if r.Provider.RequestModel != "" {
			base += fmt.Sprintf(" model=%s", r.Provider.RequestModel)
		}
		if r.Provider.InputTokens > 0 {
			base += fmt.Sprintf(" input_tokens=%d", r.Provider.InputTokens)
		}
		if r.Provider.OutputTokens > 0 {
			base += fmt.Sprintf(" output_tokens=%d", r.Provider.OutputTokens)
		}
	}
	if r.MatchedRouteName != "" {
		base += fmt.Sprintf(" route=%s/%s", r.MatchedRouteNamespace, r.MatchedRouteName)
	}
	return base
}

// spanNameFor follows OTel GenAI conventions when the request matched
// a known provider: "<operation> <model>" (e.g. "chat gpt-4o"). Falls
// back to the HTTP-style "<METHOD> <host>" otherwise.
func spanNameFor(r Record, d derived) string {
	if r.Provider != nil && r.Provider.Operation != "" {
		if r.Provider.RequestModel != "" {
			return r.Provider.Operation + " " + r.Provider.RequestModel
		}
		return r.Provider.Operation
	}
	method := d.method
	if method == "" {
		method = "REQUEST"
	}
	host := d.host
	if host == "" {
		host = d.path
	}
	if host == "" {
		return method
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

// splitAuthority returns the host and port portions of an HTTP/2
// :authority. Port 0 indicates no port was set (or it was malformed).
// Uses net.SplitHostPort so IPv6 literals like `[::1]:443` parse
// correctly.
func splitAuthority(authority string) (string, int) {
	if authority == "" {
		return "", 0
	}
	host, portStr, err := net.SplitHostPort(authority)
	if err != nil {
		// No port present — common for HTTP/2 :authority. Strip IPv6
		// brackets if any and return port 0.
		return strings.Trim(authority, "[]"), 0
	}
	port, _ := strconv.Atoi(portStr)
	return host, port
}

func parseStatus(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
