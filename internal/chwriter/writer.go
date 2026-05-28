package chwriter

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/apoxy-dev/clrk/internal/otelemit"
)

// insertTmpl is the body of every INSERT we issue. ch-go expects a
// "INSERT INTO <db>.<table> VALUES" terminator with the columnar
// block delivered separately via Query.Input.
const insertTmpl = "INSERT INTO %s.%s VALUES"

// Defaults. The flush thresholds are conservative: 1s keeps recent
// records queryable while keeping the part count low, 10k rows keeps
// memory bounded under a burst, and the ~8MiB byte estimate caps
// outsize Body strings before they wreck the writer's working set.
const (
	DefaultAddress    = "127.0.0.1:9000"
	DefaultFlushEvery = 1 * time.Second
	DefaultFlushRows  = 10_000
	DefaultFlushBytes = 8 * 1024 * 1024

	// dialTimeout caps the time spent waiting for CH on Run startup.
	// The supervisor's Ready() polls /ping separately, so when Run
	// reaches Dial CH should be up; this is a safety belt.
	dialTimeout = 5 * time.Second

	// channel buffer sizes: large enough to absorb a few seconds of
	// receive at typical agent QPS without dropping, small enough that
	// drop+count kicks in promptly when CH is wedged.
	logsChanBuf   = 256
	tracesChanBuf = 256
)

// Writer pumps decoded OTLP batches into the embedded ClickHouse
// engine over a single connection per signal. Run blocks; Enqueue is
// non-blocking and drops on overflow.
type Writer struct {
	address    string
	database   string
	ttlDays    int
	flushEvery time.Duration
	flushRows  int
	flushBytes int

	logsCh   chan []*logspb.ResourceLogs
	tracesCh chan []*tracepb.ResourceSpans

	logsDropped   atomic.Uint64
	tracesDropped atomic.Uint64
}

// Option configures a Writer.
type Option func(*Writer)

func WithAddress(addr string) Option        { return func(w *Writer) { w.address = addr } }
func WithDatabase(name string) Option       { return func(w *Writer) { w.database = name } }
func WithTTLDays(days int) Option           { return func(w *Writer) { w.ttlDays = days } }
func WithFlushEvery(d time.Duration) Option { return func(w *Writer) { w.flushEvery = d } }
func WithFlushRows(n int) Option            { return func(w *Writer) { w.flushRows = n } }
func WithFlushBytes(n int) Option           { return func(w *Writer) { w.flushBytes = n } }

// New returns a Writer configured for the embedded engine's loopback
// port with the documented flush defaults. Callers override via opts.
func New(opts ...Option) *Writer {
	w := &Writer{
		address:    DefaultAddress,
		database:   Database,
		ttlDays:    7,
		flushEvery: DefaultFlushEvery,
		flushRows:  DefaultFlushRows,
		flushBytes: DefaultFlushBytes,
		logsCh:     make(chan []*logspb.ResourceLogs, logsChanBuf),
		tracesCh:   make(chan []*tracepb.ResourceSpans, tracesChanBuf),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// EnqueueLogs hands a decoded batch of ResourceLogs to the writer.
// Non-blocking: when the channel is full, the batch is dropped and
// LogsDropped() bumps. Callers must not mutate rls afterwards.
func (w *Writer) EnqueueLogs(rls []*logspb.ResourceLogs) {
	if len(rls) == 0 {
		return
	}
	select {
	case w.logsCh <- rls:
	default:
		w.logsDropped.Add(1)
	}
}

// EnqueueTraces is the trace analog of EnqueueLogs.
func (w *Writer) EnqueueTraces(rss []*tracepb.ResourceSpans) {
	if len(rss) == 0 {
		return
	}
	select {
	case w.tracesCh <- rss:
	default:
		w.tracesDropped.Add(1)
	}
}

// LogsDropped returns the cumulative drop count for the logs channel.
func (w *Writer) LogsDropped() uint64 { return w.logsDropped.Load() }

// TracesDropped returns the cumulative drop count for the traces channel.
func (w *Writer) TracesDropped() uint64 { return w.tracesDropped.Load() }

// Run dials CH, creates tables on first start, then pumps incoming
// batches into INSERTs until ctx is done. Returns nil on clean
// shutdown or the first unrecoverable error from CH.
func (w *Writer) Run(ctx context.Context) error {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	ddlClient, err := ch.Dial(dialCtx, ch.Options{Address: w.address, Database: w.database})
	if err != nil {
		return fmt.Errorf("dial clickhouse %s: %w", w.address, err)
	}
	if err := ddlClient.Do(ctx, ch.Query{Body: renderCreateLogsTable(w.ttlDays)}); err != nil {
		_ = ddlClient.Close()
		return fmt.Errorf("create %s: %w", LogsTable, err)
	}
	if err := ddlClient.Do(ctx, ch.Query{Body: renderCreateTracesTable(w.ttlDays)}); err != nil {
		_ = ddlClient.Close()
		return fmt.Errorf("create %s: %w", TracesTable, err)
	}
	_ = ddlClient.Close()

	logsClient, err := ch.Dial(ctx, ch.Options{Address: w.address, Database: w.database})
	if err != nil {
		return fmt.Errorf("dial logs client: %w", err)
	}
	tracesClient, err := ch.Dial(ctx, ch.Options{Address: w.address, Database: w.database})
	if err != nil {
		_ = logsClient.Close()
		return fmt.Errorf("dial traces client: %w", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	errCh := make(chan error, 2)
	go func() {
		defer wg.Done()
		defer logsClient.Close()
		if err := w.runLogs(ctx, logsClient); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("logs pump: %w", err)
		}
	}()
	go func() {
		defer wg.Done()
		defer tracesClient.Close()
		if err := w.runTraces(ctx, tracesClient); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("traces pump: %w", err)
		}
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

// logsBlock holds the column set for one in-flight otel_logs batch.
// Fields mirror the schema in schema.go column-for-column.
type logsBlock struct {
	timestamp          *proto.ColDateTime64
	egRef              *proto.ColLowCardinality[string]
	traceID            *proto.ColStr
	spanID             *proto.ColStr
	severityText       *proto.ColLowCardinality[string]
	severityNumber     *proto.ColUInt8
	serviceName        *proto.ColLowCardinality[string]
	body               *proto.ColStr
	scopeName          *proto.ColLowCardinality[string]
	resourceAttributes *proto.ColMap[string, string]
	logAttributes      *proto.ColMap[string, string]
	rows               int
	bytes              int
}

func newLogsBlock() *logsBlock {
	return &logsBlock{
		timestamp:          new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		egRef:              new(proto.ColStr).LowCardinality(),
		traceID:            new(proto.ColStr),
		spanID:             new(proto.ColStr),
		severityText:       new(proto.ColStr).LowCardinality(),
		severityNumber:     new(proto.ColUInt8),
		serviceName:        new(proto.ColStr).LowCardinality(),
		body:               new(proto.ColStr),
		scopeName:          new(proto.ColStr).LowCardinality(),
		resourceAttributes: proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr)),
		logAttributes:      proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr)),
	}
}

func (b *logsBlock) input() proto.Input {
	return proto.Input{
		{Name: "Timestamp", Data: b.timestamp},
		{Name: "EGRef", Data: b.egRef},
		{Name: "TraceId", Data: b.traceID},
		{Name: "SpanId", Data: b.spanID},
		{Name: "SeverityText", Data: b.severityText},
		{Name: "SeverityNumber", Data: b.severityNumber},
		{Name: "ServiceName", Data: b.serviceName},
		{Name: "Body", Data: b.body},
		{Name: "ScopeName", Data: b.scopeName},
		{Name: "ResourceAttributes", Data: b.resourceAttributes},
		{Name: "LogAttributes", Data: b.logAttributes},
	}
}

// tracesBlock holds the column set for one in-flight otel_traces batch.
type tracesBlock struct {
	timestamp          *proto.ColDateTime64
	egRef              *proto.ColLowCardinality[string]
	traceID            *proto.ColStr
	spanID             *proto.ColStr
	parentSpanID       *proto.ColStr
	spanName           *proto.ColLowCardinality[string]
	spanKind           *proto.ColLowCardinality[string]
	serviceName        *proto.ColLowCardinality[string]
	duration           *proto.ColUInt64
	statusCode         *proto.ColLowCardinality[string]
	statusMessage      *proto.ColStr
	scopeName          *proto.ColLowCardinality[string]
	resourceAttributes *proto.ColMap[string, string]
	spanAttributes     *proto.ColMap[string, string]
	rows               int
	bytes              int
}

func newTracesBlock() *tracesBlock {
	return &tracesBlock{
		timestamp:          new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		egRef:              new(proto.ColStr).LowCardinality(),
		traceID:            new(proto.ColStr),
		spanID:             new(proto.ColStr),
		parentSpanID:       new(proto.ColStr),
		spanName:           new(proto.ColStr).LowCardinality(),
		spanKind:           new(proto.ColStr).LowCardinality(),
		serviceName:        new(proto.ColStr).LowCardinality(),
		duration:           new(proto.ColUInt64),
		statusCode:         new(proto.ColStr).LowCardinality(),
		statusMessage:      new(proto.ColStr),
		scopeName:          new(proto.ColStr).LowCardinality(),
		resourceAttributes: proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr)),
		spanAttributes:     proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr)),
	}
}

func (b *tracesBlock) input() proto.Input {
	return proto.Input{
		{Name: "Timestamp", Data: b.timestamp},
		{Name: "EGRef", Data: b.egRef},
		{Name: "TraceId", Data: b.traceID},
		{Name: "SpanId", Data: b.spanID},
		{Name: "ParentSpanId", Data: b.parentSpanID},
		{Name: "SpanName", Data: b.spanName},
		{Name: "SpanKind", Data: b.spanKind},
		{Name: "ServiceName", Data: b.serviceName},
		{Name: "Duration", Data: b.duration},
		{Name: "StatusCode", Data: b.statusCode},
		{Name: "StatusMessage", Data: b.statusMessage},
		{Name: "ScopeName", Data: b.scopeName},
		{Name: "ResourceAttributes", Data: b.resourceAttributes},
		{Name: "SpanAttributes", Data: b.spanAttributes},
	}
}

func (w *Writer) runLogs(ctx context.Context, client *ch.Client) error {
	block := newLogsBlock()
	ticker := time.NewTicker(w.flushEvery)
	defer ticker.Stop()
	body := fmt.Sprintf(insertTmpl, w.database, LogsTable)
	flush := func(reason string) {
		if block.rows == 0 {
			return
		}
		insertCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := client.Do(insertCtx, ch.Query{Body: body, Input: block.input()})
		cancel()
		if err != nil {
			slog.Error("Logs INSERT failed", "rows", block.rows, "reason", reason, "err", err)
		} else {
			slog.Debug("Logs INSERT", "rows", block.rows, "reason", reason)
		}
		block = newLogsBlock()
	}
	for {
		select {
		case <-ctx.Done():
			flush("shutdown")
			return ctx.Err()
		case <-ticker.C:
			flush("tick")
		case rls := <-w.logsCh:
			w.appendLogs(block, rls)
			if block.rows >= w.flushRows || block.bytes >= w.flushBytes {
				flush("size")
			}
		}
	}
}

func (w *Writer) runTraces(ctx context.Context, client *ch.Client) error {
	block := newTracesBlock()
	ticker := time.NewTicker(w.flushEvery)
	defer ticker.Stop()
	body := fmt.Sprintf(insertTmpl, w.database, TracesTable)
	flush := func(reason string) {
		if block.rows == 0 {
			return
		}
		insertCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := client.Do(insertCtx, ch.Query{Body: body, Input: block.input()})
		cancel()
		if err != nil {
			slog.Error("Traces INSERT failed", "rows", block.rows, "reason", reason, "err", err)
		} else {
			slog.Debug("Traces INSERT", "rows", block.rows, "reason", reason)
		}
		block = newTracesBlock()
	}
	for {
		select {
		case <-ctx.Done():
			flush("shutdown")
			return ctx.Err()
		case <-ticker.C:
			flush("tick")
		case rss := <-w.tracesCh:
			w.appendTraces(block, rss)
			if block.rows >= w.flushRows || block.bytes >= w.flushBytes {
				flush("size")
			}
		}
	}
}

// appendLogs projects each LogRecord in rls into block's columns.
// resourceAttrs are flattened once per ResourceLogs since every record
// underneath inherits them.
func (w *Writer) appendLogs(block *logsBlock, rls []*logspb.ResourceLogs) {
	for _, rl := range rls {
		resAttrs := otelemit.FlattenAttrs(rl.GetResource().GetAttributes())
		egRef := resAttrs[otelemit.AttrEgressGateway]
		serviceName := resAttrs[string(semconv.ServiceNameKey)]
		for _, sl := range rl.GetScopeLogs() {
			scopeName := sl.GetScope().GetName()
			for _, lr := range sl.GetLogRecords() {
				logAttrs := otelemit.FlattenAttrs(lr.GetAttributes())
				body := ""
				if b := lr.GetBody(); b != nil {
					body = otelemit.AnyValueString(b)
				}
				var ts time.Time
				if tsNano := int64(lr.GetTimeUnixNano()); tsNano == 0 {
					ts = time.Now()
				} else {
					ts = time.Unix(0, tsNano)
				}
				block.timestamp.Append(ts)
				block.egRef.Append(egRef)
				block.traceID.Append(hexOrEmpty(lr.GetTraceId()))
				block.spanID.Append(hexOrEmpty(lr.GetSpanId()))
				block.severityText.Append(lr.GetSeverityText())
				block.severityNumber.Append(uint8(lr.GetSeverityNumber()))
				block.serviceName.Append(serviceName)
				block.body.Append(body)
				block.scopeName.Append(scopeName)
				block.resourceAttributes.Append(resAttrs)
				block.logAttributes.Append(logAttrs)
				block.rows++
				block.bytes += approxLogBytes(body, logAttrs, resAttrs)
			}
		}
	}
}

// appendTraces projects each Span in rss into block's columns.
func (w *Writer) appendTraces(block *tracesBlock, rss []*tracepb.ResourceSpans) {
	for _, rs := range rss {
		resAttrs := otelemit.FlattenAttrs(rs.GetResource().GetAttributes())
		egRef := resAttrs[otelemit.AttrEgressGateway]
		serviceName := resAttrs[string(semconv.ServiceNameKey)]
		for _, ss := range rs.GetScopeSpans() {
			scopeName := ss.GetScope().GetName()
			for _, sp := range ss.GetSpans() {
				spanAttrs := otelemit.FlattenAttrs(sp.GetAttributes())
				var start time.Time
				if startNano := int64(sp.GetStartTimeUnixNano()); startNano == 0 {
					start = time.Now()
				} else {
					start = time.Unix(0, startNano)
				}
				duration := uint64(0)
				if end := sp.GetEndTimeUnixNano(); end > sp.GetStartTimeUnixNano() {
					duration = end - sp.GetStartTimeUnixNano()
				}
				statusCode := ""
				statusMsg := ""
				if st := sp.GetStatus(); st != nil {
					statusCode = st.GetCode().String()
					statusMsg = st.GetMessage()
				}
				block.timestamp.Append(start)
				block.egRef.Append(egRef)
				block.traceID.Append(hexOrEmpty(sp.GetTraceId()))
				block.spanID.Append(hexOrEmpty(sp.GetSpanId()))
				block.parentSpanID.Append(hexOrEmpty(sp.GetParentSpanId()))
				block.spanName.Append(sp.GetName())
				block.spanKind.Append(sp.GetKind().String())
				block.serviceName.Append(serviceName)
				block.duration.Append(duration)
				block.statusCode.Append(statusCode)
				block.statusMessage.Append(statusMsg)
				block.scopeName.Append(scopeName)
				block.resourceAttributes.Append(resAttrs)
				block.spanAttributes.Append(spanAttrs)
				block.rows++
				block.bytes += approxSpanBytes(sp.GetName(), spanAttrs, resAttrs)
			}
		}
	}
}

func hexOrEmpty(id []byte) string {
	if len(id) == 0 {
		return ""
	}
	return hex.EncodeToString(id)
}

// approxLogBytes is a coarse upper bound on the bytes a single log row
// adds to the in-memory block. We don't need precision — this only
// gates "flush early when body strings are huge" so a noisy capture
// doesn't blow the writer's working set.
func approxLogBytes(body string, logAttrs, resAttrs map[string]string) int {
	return len(body) + attrsBytes(logAttrs) + attrsBytes(resAttrs) + 64
}

func approxSpanBytes(name string, spanAttrs, resAttrs map[string]string) int {
	return len(name) + attrsBytes(spanAttrs) + attrsBytes(resAttrs) + 96
}

func attrsBytes(m map[string]string) int {
	n := 0
	for k, v := range m {
		n += len(k) + len(v) + 2
	}
	return n
}
