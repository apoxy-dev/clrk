package extproc

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	oteltrace "go.opentelemetry.io/otel/trace"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/extproc/tracectx"
	"github.com/apoxy-dev/clrk/internal/otelemit"
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
	logger otellog.Logger
	tracer oteltrace.Tracer
}

func newOTLPSink(ctx context.Context, spec clrkv1alpha1.OTLPLogsSinkSpec) (Sink, func(context.Context) error, error) {
	// POD_NAME / POD_NAMESPACE come from the controller-manager
	// Deployment via Downward API. Empty when unset (e.g. `go run`); the
	// resource just omits the semconv attr in that case.
	podName := os.Getenv("POD_NAME")
	podNS := os.Getenv("POD_NAMESPACE")
	attrs := []attribute.KeyValue{
		semconv.ServiceName("clrk"),
		attribute.String(otelemit.AttrComponent, "extproc"),
	}
	if podName != "" {
		attrs = append(attrs, semconv.ServiceInstanceID(podName), semconv.K8SPodName(podName))
	}
	if podNS != "" {
		attrs = append(attrs, semconv.K8SNamespaceName(podNS))
	}
	res := resource.NewWithAttributes(semconv.SchemaURL, attrs...)

	em, err := otelemit.New(ctx, spec, "github.com/apoxy-dev/clrk/internal/extproc", res)
	if err != nil {
		return nil, nil, fmt.Errorf("construct otlp emitter: %w", err)
	}
	sink := &otlpSink{
		logger: em.Logger(),
		tracer: em.Tracer(),
	}
	return sink, em.Close, nil
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

	attrs := make([]attribute.KeyValue, 0, 9)
	attrs = append(attrs,
		attribute.String(otelemit.AttrAgentKind, r.AgentKind),
		attribute.String(otelemit.AttrAgentNamespace, r.AgentNamespace),
		attribute.String(otelemit.AttrAgentName, r.AgentName),
		attribute.String(otelemit.AttrAgentUID, r.AgentUID),
		attribute.String(otelemit.AttrAgentRevision, r.AgentRevision),
		attribute.String(otelemit.AttrInvocationID, r.InvocationID),
		attribute.Int64(otelemit.AttrL4BytesUpstream, r.BytesUpstream),
		attribute.Int(otelemit.AttrDurationMs, int(end.Sub(start)/time.Millisecond)),
	)
	if r.DstName != "" {
		attrs = append(attrs, attribute.String(otelemit.AttrL4DstName, r.DstName))
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
	dst := r.DstName
	if dst == "" {
		dst = "-"
	}
	rec.SetBody(otellog.StringValue(fmt.Sprintf("egress.tcp agent=%s/%s dst=%s bytes_up=%dB dur=%dms",
		r.AgentNamespace, r.AgentName, dst, r.BytesUpstream, int(end.Sub(start)/time.Millisecond))))
	// 9 base attrs + DstName + 2 trace IDs = up to 12.
	logAttrs := make([]otellog.KeyValue, 0, 12)
	logAttrs = append(logAttrs,
		otellog.String(otelemit.AttrAgentKind, r.AgentKind),
		otellog.String(otelemit.AttrAgentNamespace, r.AgentNamespace),
		otellog.String(otelemit.AttrAgentName, r.AgentName),
		otellog.String(otelemit.AttrAgentUID, r.AgentUID),
		otellog.String(otelemit.AttrAgentRevision, r.AgentRevision),
		otellog.String(otelemit.AttrInvocationID, r.InvocationID),
		otellog.Int64(otelemit.AttrL4BytesUpstream, r.BytesUpstream),
		otellog.Int(otelemit.AttrDurationMs, int(end.Sub(start)/time.Millisecond)),
	)
	if r.DstName != "" {
		logAttrs = append(logAttrs, otellog.String(otelemit.AttrL4DstName, r.DstName))
	}
	sc := span.SpanContext()
	if sc.HasTraceID() {
		logAttrs = append(logAttrs,
			otellog.String(otelemit.AttrTraceID, sc.TraceID().String()),
			otellog.String(otelemit.AttrSpanID, sc.SpanID().String()),
		)
	}
	rec.AddAttributes(logAttrs...)
	s.logger.Emit(context.Background(), rec)
}

// emitSpan returns the un-Ended span; caller stamps end after emitLog
// so the back-link log carries the matching trace_id/span_id.
func (s *otlpSink) emitSpan(r Record, d derived) oteltrace.Span {
	parentCtx := tracectx.Extract(context.Background(), r.RequestHeaders)
	spanName := spanNameFor(r, d)

	attrs := append(agentAttrs(r),
		semconv.HTTPRequestMethodKey.String(d.method),
		semconv.ServerAddress(d.host),
		semconv.ServerPort(d.port),
		semconv.URLScheme(d.scheme),
		semconv.URLPath(d.path),
		semconv.URLFull(d.urlFull),
		semconv.HTTPResponseStatusCode(d.status),
		attribute.Int(otelemit.AttrReqBytes, len(r.RequestBody)),
		attribute.Int(otelemit.AttrRespBytes, len(r.ResponseBody)),
		attribute.Int(otelemit.AttrRespChunks, r.ResponseBodyChunks),
		attribute.Bool(otelemit.AttrReqTruncated, r.RequestTruncated),
		attribute.Bool(otelemit.AttrRespTruncated, r.ResponseTruncated),
	)
	if r.RequestBodyRewritten {
		attrs = append(attrs, attribute.Bool(otelemit.AttrBodyReqRewritten, true))
	}
	attrs = appendGenAIAttrs(attrs, r)
	attrs = appendAPRAttrs(attrs, r)
	attrs = appendMCPAttrs(attrs, r)

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
		otellog.Int(otelemit.AttrReqBytes, len(r.RequestBody)),
		otellog.Int(otelemit.AttrRespBytes, len(r.ResponseBody)),
		otellog.Int(otelemit.AttrRespChunks, r.ResponseBodyChunks),
		otellog.Bool(otelemit.AttrReqTruncated, r.RequestTruncated),
		otellog.Bool(otelemit.AttrRespTruncated, r.ResponseTruncated),
	)
	if dur := durationMillis(r); dur >= 0 {
		attrs = append(attrs, otellog.Int(otelemit.AttrDurationMs, dur))
	}
	if r.RequestBodyRewritten {
		attrs = append(attrs, otellog.Bool(otelemit.AttrBodyReqRewritten, true))
	}
	if sc.HasTraceID() {
		attrs = append(attrs,
			otellog.String(otelemit.AttrTraceID, sc.TraceID().String()),
			otellog.String(otelemit.AttrSpanID, sc.SpanID().String()),
		)
	}
	attrs = appendLogKVs(attrs, genAIAttrs(r))
	attrs = appendLogKVs(attrs, aprAttrs(r))
	attrs = appendLogKVs(attrs, mcpAttrs(r))
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
		attribute.String(otelemit.AttrGenAISystem, p.System),
	}
	if p.Operation != "" {
		out = append(out, attribute.String(otelemit.AttrGenAIOperationName, p.Operation))
	}
	if p.RequestModel != "" {
		out = append(out, attribute.String(otelemit.AttrGenAIRequestModel, p.RequestModel))
	}
	if p.ResponseModel != "" {
		out = append(out, attribute.String(otelemit.AttrGenAIResponseModel, p.ResponseModel))
	}
	if p.InputTokens > 0 {
		out = append(out, attribute.Int64(otelemit.AttrGenAIInputTokens, p.InputTokens))
	}
	if p.OutputTokens > 0 {
		out = append(out, attribute.Int64(otelemit.AttrGenAIOutputTokens, p.OutputTokens))
	}
	if p.StreamResponse {
		out = append(out, attribute.Bool(otelemit.AttrGenAIStream, true))
	}
	// Surface the visibility-of-usage signal only when it's actionable:
	// usage hidden behind truncation. Any other "absent" state (error
	// response, stream) doesn't benefit from raising the byte cap.
	if r.ResponseTruncated && !p.UsageVisible && !p.StreamResponse {
		out = append(out, attribute.Bool(otelemit.AttrBodyUsageVisible, false))
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
		attribute.Bool(otelemit.AttrAPRRouteMatched, true),
		attribute.String(otelemit.AttrAPRRouteNamespace, r.MatchedRouteNamespace),
		attribute.String(otelemit.AttrAPRRouteName, r.MatchedRouteName),
	}
	if r.BudgetDenied {
		out = append(out,
			attribute.Bool(otelemit.AttrBudgetDenied, true),
			attribute.Int64(otelemit.AttrBudgetDailyUsed, r.BudgetDailyUsed),
			attribute.Int64(otelemit.AttrBudgetDailyMax, r.BudgetDailyMax),
		)
	}
	return out
}

func appendAPRAttrs(dst []attribute.KeyValue, r Record) []attribute.KeyValue {
	return append(dst, aprAttrs(r)...)
}

// mcpAttrs returns the mcp.* + clrk.mcproute.* attribute slice for
// this record. Empty when the request wasn't parsed as MCP and no
// MCPRoute matched. Both sub-groups can be present independently: a
// request that parsed as MCP but didn't match any rule emits mcp.*
// without clrk.mcproute.*; a matched route emits both.
func mcpAttrs(r Record) []attribute.KeyValue {
	if r.MCP == nil && r.MatchedMCPRouteName == "" {
		return nil
	}
	out := make([]attribute.KeyValue, 0, 7)
	if r.MCP != nil {
		out = append(out, attribute.String(otelemit.AttrMCPMethod, r.MCP.Method))
		if r.MCP.ToolName != "" {
			out = append(out, attribute.String(otelemit.AttrMCPToolName, r.MCP.ToolName))
		}
		if r.MCP.ResourceURI != "" {
			out = append(out, attribute.String(otelemit.AttrMCPResourceURI, r.MCP.ResourceURI))
		}
	}
	if r.MatchedMCPRouteName != "" {
		out = append(out,
			attribute.Bool(otelemit.AttrMCPRouteMatched, true),
			attribute.String(otelemit.AttrMCPRouteNamespace, r.MatchedMCPRouteNamespace),
			attribute.String(otelemit.AttrMCPRouteName, r.MatchedMCPRouteName),
		)
	}
	if r.MCPToolPolicyDecision != "" {
		out = append(out, attribute.String(otelemit.AttrMCPToolPolicyDecision, r.MCPToolPolicyDecision))
	}
	return out
}

func appendMCPAttrs(dst []attribute.KeyValue, r Record) []attribute.KeyValue {
	return append(dst, mcpAttrs(r)...)
}

// agentAttrs returns the identity sub-slice common to span + log.
func agentAttrs(r Record) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String(otelemit.AttrAgentKind, r.AgentKind),
		attribute.String(otelemit.AttrAgentNamespace, r.AgentNamespace),
		attribute.String(otelemit.AttrAgentName, r.AgentName),
		attribute.String(otelemit.AttrAgentUID, r.AgentUID),
		attribute.String(otelemit.AttrAgentRevision, r.AgentRevision),
		attribute.String(otelemit.AttrInvocationID, r.InvocationID),
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
// them here (and tracectx.IsSensitiveHeader operates on the same
// lowercase form).
func headerAttrs(prefix string, headers map[string]string) []attribute.KeyValue {
	if len(headers) == 0 {
		return nil
	}
	out := make([]attribute.KeyValue, 0, len(headers))
	for k, v := range headers {
		if tracectx.IsSensitiveHeader(k) {
			v = "[redacted]"
		}
		out = append(out, attribute.String(prefix+k, v))
	}
	return out
}

func bodyAttrs(body []byte, truncated bool) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int(otelemit.AttrBodyBytes, len(body)),
		attribute.Bool(otelemit.AttrBodyTruncated, truncated),
		attribute.String(otelemit.AttrBodyB64, base64.StdEncoding.EncodeToString(body)),
	}
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
	if r.MCP != nil {
		base += fmt.Sprintf(" mcp_method=%s", r.MCP.Method)
		if r.MCP.ToolName != "" {
			base += fmt.Sprintf(" mcp_tool=%s", r.MCP.ToolName)
		}
	}
	if r.MatchedMCPRouteName != "" {
		base += fmt.Sprintf(" mcp_route=%s/%s", r.MatchedMCPRouteNamespace, r.MatchedMCPRouteName)
	}
	if r.MCPToolPolicyDecision != "" {
		base += fmt.Sprintf(" mcp_decision=%s", r.MCPToolPolicyDecision)
	}
	return base
}

// spanNameFor follows OTel GenAI conventions when the request matched
// a known provider: "<operation> <model>" (e.g. "chat gpt-4o"). For
// MCP traffic uses "mcp <method> <tool>" (e.g. "mcp tools/call
// search_web") so trace UI groups by tool. Falls back to the HTTP-
// style "<METHOD> <host>" otherwise.
func spanNameFor(r Record, d derived) string {
	if r.Provider != nil && r.Provider.Operation != "" {
		if r.Provider.RequestModel != "" {
			return r.Provider.Operation + " " + r.Provider.RequestModel
		}
		return r.Provider.Operation
	}
	if r.MCP != nil && r.MCP.Method != "" {
		name := "mcp " + r.MCP.Method
		if r.MCP.ToolName != "" {
			name += " " + r.MCP.ToolName
		}
		return name
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
