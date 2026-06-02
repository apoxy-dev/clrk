package telemetry

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/apoxy-dev/clrk/internal/chwriter"
	"github.com/apoxy-dev/clrk/internal/otelemit"
)

// logRow is one scanned otel_logs row. EGRef/ServiceName are not scanned
// separately: they live inside resourceAttrs (keys clrk.egress_gateway /
// service.name), and reconstruction rebuilds the Resource from that map.
type logRow struct {
	timestamp         time.Time
	traceID           string
	spanID            string
	traceFlags        uint8
	severityText      string
	severityNumber    uint8
	body              string
	resourceSchemaURL string
	resourceAttrs     map[string]string
	scopeSchemaURL    string
	scopeName         string
	scopeVersion      string
	scopeAttrs        map[string]string
	logAttrs          map[string]string
}

// logSelectColumns is the otel_logs projection, in proto.Results order.
const logSelectColumns = "Timestamp, TraceId, SpanId, TraceFlags, SeverityText, SeverityNumber, " +
	"Body, ResourceSchemaUrl, ResourceAttributes, ScopeSchemaUrl, ScopeName, ScopeVersion, " +
	"ScopeAttributes, LogAttributes"

// scanLogRows runs body and scans every returned row into a []logRow.
// Reader column types mirror chwriter's logsBlock exactly so ch-go's
// type check passes without any CAST (LowCardinality / Map / DateTime64
// read back into the same column kinds the writer inserted).
func scanLogRows(ctx context.Context, pool Doer, body string) ([]logRow, error) {
	var (
		ts      = new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano)
		traceID proto.ColStr
		spanID  proto.ColStr
		tflags  proto.ColUInt8
		sevText = new(proto.ColStr).LowCardinality()
		sevNum  proto.ColUInt8
		body0   proto.ColStr
		resURL  = new(proto.ColStr).LowCardinality()
		resAttr = proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr))
		scURL   = new(proto.ColStr).LowCardinality()
		scName  = new(proto.ColStr).LowCardinality()
		scVer   = new(proto.ColStr).LowCardinality()
		scAttr  = proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr))
		logAttr = proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr))
	)
	var rows []logRow
	if err := pool.Do(ctx, ch.Query{
		Body: body,
		Result: proto.Results{
			{Name: "Timestamp", Data: ts},
			{Name: "TraceId", Data: &traceID},
			{Name: "SpanId", Data: &spanID},
			{Name: "TraceFlags", Data: &tflags},
			{Name: "SeverityText", Data: sevText},
			{Name: "SeverityNumber", Data: &sevNum},
			{Name: "Body", Data: &body0},
			{Name: "ResourceSchemaUrl", Data: resURL},
			{Name: "ResourceAttributes", Data: resAttr},
			{Name: "ScopeSchemaUrl", Data: scURL},
			{Name: "ScopeName", Data: scName},
			{Name: "ScopeVersion", Data: scVer},
			{Name: "ScopeAttributes", Data: scAttr},
			{Name: "LogAttributes", Data: logAttr},
		},
		// OnResult drains every received block. ClickHouse splits a
		// result into multiple blocks when the SELECT reads many parts
		// at once -- which is the steady state here, since chwriter's
		// concurrent inserts leave a stream of unmerged parts. Without an
		// OnResult, ch-go's default handler accepts only the first block
		// and fails the second with "no OnResult provided" (an
		// intermittent 500 that tracks insert/merge pressure). The Result
		// columns are reset per block, so each callback sees just that
		// block's rows; copy them out before the next block overwrites.
		OnResult: func(context.Context, proto.Block) error {
			n := ts.Rows()
			for i := 0; i < n; i++ {
				rows = append(rows, logRow{
					timestamp:         ts.Row(i),
					traceID:           traceID.Row(i),
					spanID:            spanID.Row(i),
					traceFlags:        tflags.Row(i),
					severityText:      sevText.Row(i),
					severityNumber:    sevNum.Row(i),
					body:              body0.Row(i),
					resourceSchemaURL: resURL.Row(i),
					resourceAttrs:     resAttr.Row(i),
					scopeSchemaURL:    scURL.Row(i),
					scopeName:         scName.Row(i),
					scopeVersion:      scVer.Row(i),
					scopeAttrs:        scAttr.Row(i),
					logAttrs:          logAttr.Row(i),
				})
			}
			return nil
		},
	}); err != nil {
		return nil, err
	}
	return rows, nil
}

// queryLogsPage runs one paged, newest-first read and reconstructs a
// LogsData plus the next continue token.
func (c *Connecter) queryLogsPage(ctx context.Context, sc scope, f filters) (*logspb.LogsData, string, error) {
	snap, cur, err := pageBound(f, time.Now())
	if err != nil {
		return nil, "", apierrFromCursor(err)
	}
	clauses := scopeAndFilterClauses(sc, f, "LogAttributes")
	if f.iostream != "" {
		clauses = append(clauses, "IoStream = "+sqlString(f.iostream))
	}
	clauses = append(clauses, "Timestamp <= "+dt64Nano(snap))
	if cur != nil {
		clauses = append(clauses, keysetClause(cur))
	}
	limit := f.effectiveLimit()
	body := fmt.Sprintf("SELECT %s FROM %s.%s%s ORDER BY Timestamp DESC, TraceId DESC, SpanId DESC LIMIT %d",
		logSelectColumns, chwriter.Database, chwriter.LogsTable, whereOf(clauses), limit+1)

	rows, err := scanLogRows(ctx, c.deps.Pool, body)
	if err != nil {
		return nil, "", err
	}
	more := len(rows) > limit
	if more {
		rows = rows[:limit]
	}
	data := reconstructLogs(rows)
	next := ""
	if more && len(rows) > 0 {
		last := rows[len(rows)-1]
		next = encodeCursor(pageCursor{Snap: snap, TS: last.timestamp.UnixNano(), TID: last.traceID, SID: last.spanID})
	}
	return data, next, nil
}

// followLogs fetches one ascending chunk of log rows strictly newer than
// since and returns the reconstructed LogsData plus the new watermark.
func (c *Connecter) followLogs(ctx context.Context, sc scope, f filters, since time.Time) (*logspb.LogsData, time.Time, error) {
	clauses := scopeAndFilterClauses(sc, f, "LogAttributes")
	if f.iostream != "" {
		clauses = append(clauses, "IoStream = "+sqlString(f.iostream))
	}
	clauses = append(clauses, "Timestamp > "+dt64Nano(since.UnixNano()))
	body := fmt.Sprintf("SELECT %s FROM %s.%s%s ORDER BY Timestamp ASC, TraceId ASC, SpanId ASC LIMIT %d",
		logSelectColumns, chwriter.Database, chwriter.LogsTable, whereOf(clauses), followChunkRows)

	rows, err := scanLogRows(ctx, c.deps.Pool, body)
	if err != nil {
		return nil, since, err
	}
	if len(rows) == 0 {
		return nil, since, nil
	}
	return reconstructLogs(rows), rows[len(rows)-1].timestamp, nil
}

// reconstructLogs groups flattened rows back into the OTLP nesting:
// rows sharing a Resource (schema URL + attributes) form one
// ResourceLogs; within it, rows sharing a Scope form one ScopeLogs; each
// row is one LogRecord. Group order follows first appearance so output
// is deterministic given the row order.
func reconstructLogs(rows []logRow) *logspb.LogsData {
	data := &logspb.LogsData{}
	type scopeKey struct{ schemaURL, name, version, attrs string }
	resByKey := map[string]*logspb.ResourceLogs{}
	scopeByKey := map[string]map[scopeKey]*logspb.ScopeLogs{}

	for _, r := range rows {
		rKey := r.resourceSchemaURL + "\x00" + mapGroupKey(r.resourceAttrs)
		rl, ok := resByKey[rKey]
		if !ok {
			rl = &logspb.ResourceLogs{
				Resource:  &resourcepb.Resource{Attributes: otelemit.AttrsToKV(r.resourceAttrs)},
				SchemaUrl: r.resourceSchemaURL,
			}
			resByKey[rKey] = rl
			scopeByKey[rKey] = map[scopeKey]*logspb.ScopeLogs{}
			data.ResourceLogs = append(data.ResourceLogs, rl)
		}
		sk := scopeKey{r.scopeSchemaURL, r.scopeName, r.scopeVersion, mapGroupKey(r.scopeAttrs)}
		sl, ok := scopeByKey[rKey][sk]
		if !ok {
			sl = &logspb.ScopeLogs{
				Scope: &commonpb.InstrumentationScope{
					Name:       r.scopeName,
					Version:    r.scopeVersion,
					Attributes: otelemit.AttrsToKV(r.scopeAttrs),
				},
				SchemaUrl: r.scopeSchemaURL,
			}
			scopeByKey[rKey][sk] = sl
			rl.ScopeLogs = append(rl.ScopeLogs, sl)
		}
		lr := &logspb.LogRecord{
			TimeUnixNano:   uint64(r.timestamp.UnixNano()),
			SeverityNumber: logspb.SeverityNumber(r.severityNumber),
			SeverityText:   r.severityText,
			Flags:          uint32(r.traceFlags),
			TraceId:        hexDecode(r.traceID),
			SpanId:         hexDecode(r.spanID),
			Attributes:     otelemit.AttrsToKV(r.logAttrs),
		}
		if r.body != "" {
			lr.Body = &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: r.body}}
		}
		sl.LogRecords = append(sl.LogRecords, lr)
	}
	return data
}
