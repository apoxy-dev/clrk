package telemetry

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/apoxy-dev/clrk/internal/chwriter"
	"github.com/apoxy-dev/clrk/internal/otelemit"
)

// traceRow is one scanned otel_traces row, including the per-span Events
// and Links Nested arrays (parallel arrays sharing one length each).
// EGRef/ServiceName live inside resourceAttrs, as for logs.
type traceRow struct {
	timestamp     time.Time
	traceID       string
	spanID        string
	parentSpanID  string
	traceState    string
	spanName      string
	spanKind      string
	scopeName     string
	scopeVersion  string
	duration      uint64
	statusCode    string
	statusMessage string
	resourceAttrs map[string]string
	spanAttrs     map[string]string

	eventsTimestamp []time.Time
	eventsName      []string
	eventsAttrs     []map[string]string
	linksTraceID    []string
	linksSpanID     []string
	linksTraceState []string
	linksAttrs      []map[string]string
}

// traceSelectColumns is the otel_traces projection. The Events.* /
// Links.* dotted names select the Nested subcolumns as parallel arrays,
// matching the chwriter INSERT input names.
const traceSelectColumns = "Timestamp, TraceId, SpanId, ParentSpanId, TraceState, SpanName, SpanKind, " +
	"ScopeName, ScopeVersion, Duration, StatusCode, StatusMessage, ResourceAttributes, SpanAttributes, " +
	"Events.Timestamp, Events.Name, Events.Attributes, Links.TraceId, Links.SpanId, Links.TraceState, Links.Attributes"

// scanTraceRows runs body and scans every returned row. Reader column
// types mirror chwriter's tracesBlock exactly (incl. the Array/Map/
// DateTime64 nested columns) so ch-go's type check passes without CASTs.
func scanTraceRows(ctx context.Context, pool Doer, body string) ([]traceRow, error) {
	var (
		ts          = new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano)
		traceID     proto.ColStr
		spanID      proto.ColStr
		parentSpan  proto.ColStr
		traceState  proto.ColStr
		spanName    = new(proto.ColStr).LowCardinality()
		spanKind    = new(proto.ColStr).LowCardinality()
		scName      = new(proto.ColStr).LowCardinality()
		scVer       = new(proto.ColStr).LowCardinality()
		duration    proto.ColUInt64
		statusCode  = new(proto.ColStr).LowCardinality()
		statusMsg   proto.ColStr
		resAttr     = proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr))
		spanAttr    = proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr))
		evTimestamp = new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano).Array()
		evName      = new(proto.ColStr).LowCardinality().Array()
		evAttr      = proto.NewArray[map[string]string](proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr)))
		lnTraceID   = new(proto.ColStr).Array()
		lnSpanID    = new(proto.ColStr).Array()
		lnState     = new(proto.ColStr).Array()
		lnAttr      = proto.NewArray[map[string]string](proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr)))
	)
	var rows []traceRow
	if err := pool.Do(ctx, ch.Query{
		Body: body,
		Result: proto.Results{
			{Name: "Timestamp", Data: ts},
			{Name: "TraceId", Data: &traceID},
			{Name: "SpanId", Data: &spanID},
			{Name: "ParentSpanId", Data: &parentSpan},
			{Name: "TraceState", Data: &traceState},
			{Name: "SpanName", Data: spanName},
			{Name: "SpanKind", Data: spanKind},
			{Name: "ScopeName", Data: scName},
			{Name: "ScopeVersion", Data: scVer},
			{Name: "Duration", Data: &duration},
			{Name: "StatusCode", Data: statusCode},
			{Name: "StatusMessage", Data: &statusMsg},
			{Name: "ResourceAttributes", Data: resAttr},
			{Name: "SpanAttributes", Data: spanAttr},
			{Name: "Events.Timestamp", Data: evTimestamp},
			{Name: "Events.Name", Data: evName},
			{Name: "Events.Attributes", Data: evAttr},
			{Name: "Links.TraceId", Data: lnTraceID},
			{Name: "Links.SpanId", Data: lnSpanID},
			{Name: "Links.TraceState", Data: lnState},
			{Name: "Links.Attributes", Data: lnAttr},
		},
		// OnResult drains every received block. ClickHouse splits a
		// result into multiple blocks when the SELECT reads many parts
		// at once -- the steady state here, since chwriter's concurrent
		// inserts leave a stream of unmerged parts. Without an OnResult,
		// ch-go's default handler accepts only the first block and fails
		// the second with "no OnResult provided" (an intermittent 500
		// that tracks insert/merge pressure). The Result columns are
		// reset per block, so each callback sees just that block's rows;
		// copy them out before the next block overwrites.
		OnResult: func(context.Context, proto.Block) error {
			n := ts.Rows()
			for i := 0; i < n; i++ {
				rows = append(rows, traceRow{
					timestamp:       ts.Row(i),
					traceID:         traceID.Row(i),
					spanID:          spanID.Row(i),
					parentSpanID:    parentSpan.Row(i),
					traceState:      traceState.Row(i),
					spanName:        spanName.Row(i),
					spanKind:        spanKind.Row(i),
					scopeName:       scName.Row(i),
					scopeVersion:    scVer.Row(i),
					duration:        duration.Row(i),
					statusCode:      statusCode.Row(i),
					statusMessage:   statusMsg.Row(i),
					resourceAttrs:   resAttr.Row(i),
					spanAttrs:       spanAttr.Row(i),
					eventsTimestamp: evTimestamp.Row(i),
					eventsName:      evName.Row(i),
					eventsAttrs:     evAttr.Row(i),
					linksTraceID:    lnTraceID.Row(i),
					linksSpanID:     lnSpanID.Row(i),
					linksTraceState: lnState.Row(i),
					linksAttrs:      lnAttr.Row(i),
				})
			}
			return nil
		},
	}); err != nil {
		return nil, err
	}
	return rows, nil
}

// queryTracesPage runs one paged, newest-first read and reconstructs a
// TracesData plus the next continue token.
func (c *Connecter) queryTracesPage(ctx context.Context, sc scope, f filters) (*tracepb.TracesData, string, error) {
	snap, cur, err := pageBound(f, time.Now())
	if err != nil {
		return nil, "", apierrFromCursor(err)
	}
	clauses := scopeAndFilterClauses(sc, f, "SpanAttributes")
	clauses = append(clauses, "Timestamp <= "+dt64Nano(snap))
	if cur != nil {
		clauses = append(clauses, keysetClause(cur))
	}
	limit := f.effectiveLimit()
	body := fmt.Sprintf("SELECT %s FROM %s.%s%s ORDER BY Timestamp DESC, TraceId DESC, SpanId DESC LIMIT %d",
		traceSelectColumns, chwriter.Database, chwriter.TracesTable, whereOf(clauses), limit+1)

	rows, err := scanTraceRows(ctx, c.deps.Pool, body)
	if err != nil {
		return nil, "", err
	}
	more := len(rows) > limit
	if more {
		rows = rows[:limit]
	}
	data := reconstructTraces(rows)
	next := ""
	if more && len(rows) > 0 {
		last := rows[len(rows)-1]
		next = encodeCursor(pageCursor{Snap: snap, TS: last.timestamp.UnixNano(), TID: last.traceID, SID: last.spanID})
	}
	return data, next, nil
}

// followTraces fetches one ascending chunk of spans strictly newer than
// since and returns the reconstructed TracesData plus the new watermark.
func (c *Connecter) followTraces(ctx context.Context, sc scope, f filters, since time.Time) (*tracepb.TracesData, time.Time, error) {
	clauses := scopeAndFilterClauses(sc, f, "SpanAttributes")
	clauses = append(clauses, "Timestamp > "+dt64Nano(since.UnixNano()))
	body := fmt.Sprintf("SELECT %s FROM %s.%s%s ORDER BY Timestamp ASC, TraceId ASC, SpanId ASC LIMIT %d",
		traceSelectColumns, chwriter.Database, chwriter.TracesTable, whereOf(clauses), followChunkRows)

	rows, err := scanTraceRows(ctx, c.deps.Pool, body)
	if err != nil {
		return nil, since, err
	}
	if len(rows) == 0 {
		return nil, since, nil
	}
	return reconstructTraces(rows), rows[len(rows)-1].timestamp, nil
}

// reconstructTraces groups flattened span rows back into the OTLP
// nesting: rows sharing a Resource form one ResourceSpans; within it,
// rows sharing a Scope (name+version; the traces schema has no scope
// schema URL / attributes) form one ScopeSpans; each row is one Span,
// with Events / Links rebuilt from the parallel Nested arrays.
func reconstructTraces(rows []traceRow) *tracepb.TracesData {
	data := &tracepb.TracesData{}
	type scopeKey struct{ name, version string }
	resByKey := map[string]*tracepb.ResourceSpans{}
	scopeByKey := map[string]map[scopeKey]*tracepb.ScopeSpans{}

	for _, r := range rows {
		rKey := mapGroupKey(r.resourceAttrs)
		rs, ok := resByKey[rKey]
		if !ok {
			rs = &tracepb.ResourceSpans{
				Resource: &resourcepb.Resource{Attributes: otelemit.AttrsToKV(r.resourceAttrs)},
			}
			resByKey[rKey] = rs
			scopeByKey[rKey] = map[scopeKey]*tracepb.ScopeSpans{}
			data.ResourceSpans = append(data.ResourceSpans, rs)
		}
		sk := scopeKey{r.scopeName, r.scopeVersion}
		ss, ok := scopeByKey[rKey][sk]
		if !ok {
			ss = &tracepb.ScopeSpans{
				Scope: &commonpb.InstrumentationScope{Name: r.scopeName, Version: r.scopeVersion},
			}
			scopeByKey[rKey][sk] = ss
			rs.ScopeSpans = append(rs.ScopeSpans, ss)
		}
		ss.Spans = append(ss.Spans, buildSpan(r))
	}
	return data
}

// buildSpan reconstructs one Span from a row, zipping the parallel Event
// and Link arrays index-wise (guarding against any length skew rather
// than trusting the Nested invariant blindly).
func buildSpan(r traceRow) *tracepb.Span {
	span := &tracepb.Span{
		TraceId:           hexDecode(r.traceID),
		SpanId:            hexDecode(r.spanID),
		TraceState:        r.traceState,
		ParentSpanId:      hexDecode(r.parentSpanID),
		Name:              r.spanName,
		Kind:              spanKindFromString(r.spanKind),
		StartTimeUnixNano: uint64(r.timestamp.UnixNano()),
		EndTimeUnixNano:   uint64(r.timestamp.UnixNano()) + r.duration,
		Attributes:        otelemit.AttrsToKV(r.spanAttrs),
	}
	if code := statusCodeFromString(r.statusCode); code != tracepb.Status_STATUS_CODE_UNSET || r.statusMessage != "" {
		span.Status = &tracepb.Status{Code: code, Message: r.statusMessage}
	}
	for i := 0; i < len(r.eventsName); i++ {
		ev := &tracepb.Span_Event{Name: r.eventsName[i]}
		if i < len(r.eventsTimestamp) {
			ev.TimeUnixNano = uint64(r.eventsTimestamp[i].UnixNano())
		}
		if i < len(r.eventsAttrs) {
			ev.Attributes = otelemit.AttrsToKV(r.eventsAttrs[i])
		}
		span.Events = append(span.Events, ev)
	}
	for i := 0; i < len(r.linksTraceID); i++ {
		ln := &tracepb.Span_Link{TraceId: hexDecode(r.linksTraceID[i])}
		if i < len(r.linksSpanID) {
			ln.SpanId = hexDecode(r.linksSpanID[i])
		}
		if i < len(r.linksTraceState) {
			ln.TraceState = r.linksTraceState[i]
		}
		if i < len(r.linksAttrs) {
			ln.Attributes = otelemit.AttrsToKV(r.linksAttrs[i])
		}
		span.Links = append(span.Links, ln)
	}
	return span
}

// spanKindFromString inverts chwriter's spanKindStr (the otel-collector
// display form).
func spanKindFromString(s string) tracepb.Span_SpanKind {
	switch s {
	case "Internal":
		return tracepb.Span_SPAN_KIND_INTERNAL
	case "Server":
		return tracepb.Span_SPAN_KIND_SERVER
	case "Client":
		return tracepb.Span_SPAN_KIND_CLIENT
	case "Producer":
		return tracepb.Span_SPAN_KIND_PRODUCER
	case "Consumer":
		return tracepb.Span_SPAN_KIND_CONSUMER
	default:
		return tracepb.Span_SPAN_KIND_UNSPECIFIED
	}
}

// statusCodeFromString inverts chwriter's statusCodeStr.
func statusCodeFromString(s string) tracepb.Status_StatusCode {
	switch s {
	case "Ok":
		return tracepb.Status_STATUS_CODE_OK
	case "Error":
		return tracepb.Status_STATUS_CODE_ERROR
	default:
		return tracepb.Status_STATUS_CODE_UNSET
	}
}
