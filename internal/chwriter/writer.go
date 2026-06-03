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

	// insertTimeout bounds a single INSERT attempt. A broken socket errors
	// fast; this only bites a connected-but-slow CH (merge backpressure).
	insertTimeout = 30 * time.Second

	// A failed INSERT is held and retried (through reconnects) until it
	// commits, so a CH restart costs zero already-buffered rows. Backoff is
	// capped exactly like the invocation materializer's drain loop.
	flushRetryInitial = 250 * time.Millisecond
	flushRetryMax     = 5 * time.Second

	// shutdownFlushTimeout bounds the final drain on ctx cancel. It runs on
	// a detached context so the last buffered block can still commit (and
	// reconnect if needed) after the parent ctx is already done.
	shutdownFlushTimeout = 10 * time.Second

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

// dial opens a fresh ch-go client to the configured engine, bounded by
// dialTimeout. Shared by Run's startup dials and the pumps' reconnect
// path so connection options stay identical across both.
func (w *Writer) dial(ctx context.Context) (*ch.Client, error) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	return ch.Dial(dialCtx, ch.Options{Address: w.address, Database: w.database})
}

// recoverConn is called on a pump's INSERT-error path to keep one
// transient connection failure from permanently wedging telemetry. A
// broken pipe leaves ch-go's client in a half-dead state: Do returns an
// error but IsClosed() stays false, so every later INSERT keeps failing
// on the same dead socket until the pod restarts. We disambiguate with a
// Ping probe — if the probe succeeds the failure was server-side (a
// rejected batch) and the same client is kept; if it fails the socket is
// dead and we dial a replacement. The old client is closed only after a
// replacement is in hand, so a failed re-dial leaves the pump retrying on
// the next flush rather than holding nothing. During shutdown we skip the
// churn entirely.
func (w *Writer) recoverConn(ctx context.Context, client *ch.Client) *ch.Client {
	if ctx.Err() != nil {
		return client
	}
	pingCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	perr := client.Ping(pingCtx)
	cancel()
	if perr == nil {
		return client
	}
	fresh, err := w.dial(ctx)
	if err != nil {
		slog.Error("ClickHouse reconnect failed; will retry on next flush", "addr", w.address, "err", err)
		return client
	}
	_ = client.Close()
	slog.Warn("Reconnected to ClickHouse after connection failure", "addr", w.address, "probe_err", perr)
	return fresh
}

// insertWithRetry sends one columnar block, holding it and retrying
// through reconnects until it commits or ctx is cancelled. The block is
// NEVER discarded on a transient failure: ch-go reads (does not consume)
// the Input columns, so a retry re-sends identical data, and recoverConn
// re-dials a dead socket between attempts. This mirrors the invocation
// materializer's hold-and-retry drain so a CH restart or network blip
// costs zero already-buffered rows (at-least-once: a failure after CH
// committed but before we read the ack can re-send a block, so a rare
// duplicate is possible — acceptable for append-only telemetry). Returns
// the (possibly reconnected) client and whether the block committed; a
// false result means only that ctx was cancelled mid-retry. Backoff is
// capped exactly like consumer.drain.
func (w *Writer) insertWithRetry(ctx context.Context, client *ch.Client, body string, in proto.Input, rows int, reason string) (*ch.Client, bool) {
	backoff := flushRetryInitial
	for {
		insertCtx, cancel := context.WithTimeout(ctx, insertTimeout)
		err := client.Do(insertCtx, ch.Query{Body: body, Input: in})
		cancel()
		if err == nil {
			slog.Debug("INSERT committed", "rows", rows, "reason", reason)
			return client, true
		}
		if ctx.Err() != nil {
			// Cancelled mid-retry: hold the block. On a live ctx this is the
			// shutdown drain giving up after shutdownFlushTimeout; the rows
			// are lost only because chwriter has no durable spool.
			slog.Error("INSERT abandoned (context done)", "rows", rows, "reason", reason, "err", err)
			return client, false
		}
		slog.Error("INSERT failed; holding batch and retrying", "rows", rows, "reason", reason, "err", err)
		client = w.recoverConn(ctx, client)
		select {
		case <-ctx.Done():
			return client, false
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > flushRetryMax {
			backoff = flushRetryMax
		}
	}
}

// Run dials CH, creates tables on first start, then pumps incoming
// batches into INSERTs until ctx is done. Returns nil on clean
// shutdown or the first unrecoverable error from CH.
func (w *Writer) Run(ctx context.Context) error {
	ddlClient, err := w.dial(ctx)
	if err != nil {
		return fmt.Errorf("dial clickhouse %s: %w", w.address, err)
	}
	if err := ensureSchema(ctx, ddlClient, w.ttlDays); err != nil {
		_ = ddlClient.Close()
		return err
	}
	_ = ddlClient.Close()

	logsClient, err := w.dial(ctx)
	if err != nil {
		return fmt.Errorf("dial logs client: %w", err)
	}
	tracesClient, err := w.dial(ctx)
	if err != nil {
		_ = logsClient.Close()
		return fmt.Errorf("dial traces client: %w", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	errCh := make(chan error, 2)
	go func() {
		defer wg.Done()
		// The pump owns closing its client: a reconnect swaps the client
		// out from under us, so closing here in Run would close the stale
		// pointer and leak the reconnected one.
		if err := w.runLogs(ctx, logsClient); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("logs pump: %w", err)
		}
	}()
	go func() {
		defer wg.Done()
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
// Fields mirror the schema in schema.go column-for-column. MATERIALIZED
// columns (Component/InvocationId/Agent/IoStream) are computed by
// ClickHouse from the maps and never appear here.
type logsBlock struct {
	timestamp          *proto.ColDateTime64
	egRef              *proto.ColLowCardinality[string]
	traceID            *proto.ColStr
	spanID             *proto.ColStr
	traceFlags         *proto.ColUInt8
	severityText       *proto.ColLowCardinality[string]
	severityNumber     *proto.ColUInt8
	serviceName        *proto.ColLowCardinality[string]
	body               *proto.ColStr
	resourceSchemaURL  *proto.ColLowCardinality[string]
	resourceAttributes *proto.ColMap[string, string]
	scopeSchemaURL     *proto.ColLowCardinality[string]
	scopeName          *proto.ColLowCardinality[string]
	scopeVersion       *proto.ColLowCardinality[string]
	scopeAttributes    *proto.ColMap[string, string]
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
		traceFlags:         new(proto.ColUInt8),
		severityText:       new(proto.ColStr).LowCardinality(),
		severityNumber:     new(proto.ColUInt8),
		serviceName:        new(proto.ColStr).LowCardinality(),
		body:               new(proto.ColStr),
		resourceSchemaURL:  new(proto.ColStr).LowCardinality(),
		resourceAttributes: proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr)),
		scopeSchemaURL:     new(proto.ColStr).LowCardinality(),
		scopeName:          new(proto.ColStr).LowCardinality(),
		scopeVersion:       new(proto.ColStr).LowCardinality(),
		scopeAttributes:    proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr)),
		logAttributes:      proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr)),
	}
}

func (b *logsBlock) input() proto.Input {
	return proto.Input{
		{Name: "Timestamp", Data: b.timestamp},
		{Name: "EGRef", Data: b.egRef},
		{Name: "TraceId", Data: b.traceID},
		{Name: "SpanId", Data: b.spanID},
		{Name: "TraceFlags", Data: b.traceFlags},
		{Name: "SeverityText", Data: b.severityText},
		{Name: "SeverityNumber", Data: b.severityNumber},
		{Name: "ServiceName", Data: b.serviceName},
		{Name: "Body", Data: b.body},
		{Name: "ResourceSchemaUrl", Data: b.resourceSchemaURL},
		{Name: "ResourceAttributes", Data: b.resourceAttributes},
		{Name: "ScopeSchemaUrl", Data: b.scopeSchemaURL},
		{Name: "ScopeName", Data: b.scopeName},
		{Name: "ScopeVersion", Data: b.scopeVersion},
		{Name: "ScopeAttributes", Data: b.scopeAttributes},
		{Name: "LogAttributes", Data: b.logAttributes},
	}
}

// tracesBlock holds the column set for one in-flight otel_traces batch.
// Field order mirrors the schema in schema.go column-for-column. The
// trailing array columns back the Events / Links Nested columns; they
// are inserted via dotted names (Events.Timestamp, Links.TraceId, ...)
// in input(). MATERIALIZED columns (Component/InvocationId/Agent) are
// computed by ClickHouse from the maps and never appear here.
type tracesBlock struct {
	timestamp          *proto.ColDateTime64
	egRef              *proto.ColLowCardinality[string]
	traceID            *proto.ColStr
	spanID             *proto.ColStr
	parentSpanID       *proto.ColStr
	traceState         *proto.ColStr
	spanName           *proto.ColLowCardinality[string]
	spanKind           *proto.ColLowCardinality[string]
	serviceName        *proto.ColLowCardinality[string]
	scopeName          *proto.ColLowCardinality[string]
	scopeVersion       *proto.ColLowCardinality[string]
	duration           *proto.ColUInt64
	statusCode         *proto.ColLowCardinality[string]
	statusMessage      *proto.ColStr
	resourceAttributes *proto.ColMap[string, string]
	spanAttributes     *proto.ColMap[string, string]

	eventsTimestamp  *proto.ColArr[time.Time]
	eventsName       *proto.ColArr[string]
	eventsAttributes *proto.ColArr[map[string]string]
	linksTraceID     *proto.ColArr[string]
	linksSpanID      *proto.ColArr[string]
	linksTraceState  *proto.ColArr[string]
	linksAttributes  *proto.ColArr[map[string]string]

	rows  int
	bytes int
}

func newTracesBlock() *tracesBlock {
	return &tracesBlock{
		timestamp:          new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		egRef:              new(proto.ColStr).LowCardinality(),
		traceID:            new(proto.ColStr),
		spanID:             new(proto.ColStr),
		parentSpanID:       new(proto.ColStr),
		traceState:         new(proto.ColStr),
		spanName:           new(proto.ColStr).LowCardinality(),
		spanKind:           new(proto.ColStr).LowCardinality(),
		serviceName:        new(proto.ColStr).LowCardinality(),
		scopeName:          new(proto.ColStr).LowCardinality(),
		scopeVersion:       new(proto.ColStr).LowCardinality(),
		duration:           new(proto.ColUInt64),
		statusCode:         new(proto.ColStr).LowCardinality(),
		statusMessage:      new(proto.ColStr),
		resourceAttributes: proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr)),
		spanAttributes:     proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr)),
		// DateTime64 precision must be set BEFORE .Array(); the inner
		// column panics on append otherwise.
		eventsTimestamp:  new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano).Array(),
		eventsName:       new(proto.ColStr).LowCardinality().Array(),
		eventsAttributes: proto.NewArray[map[string]string](proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr))),
		linksTraceID:     new(proto.ColStr).Array(),
		linksSpanID:      new(proto.ColStr).Array(),
		linksTraceState:  new(proto.ColStr).Array(),
		linksAttributes:  proto.NewArray[map[string]string](proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr))),
	}
}

func (b *tracesBlock) input() proto.Input {
	return proto.Input{
		{Name: "Timestamp", Data: b.timestamp},
		{Name: "EGRef", Data: b.egRef},
		{Name: "TraceId", Data: b.traceID},
		{Name: "SpanId", Data: b.spanID},
		{Name: "ParentSpanId", Data: b.parentSpanID},
		{Name: "TraceState", Data: b.traceState},
		{Name: "SpanName", Data: b.spanName},
		{Name: "SpanKind", Data: b.spanKind},
		{Name: "ServiceName", Data: b.serviceName},
		{Name: "ScopeName", Data: b.scopeName},
		{Name: "ScopeVersion", Data: b.scopeVersion},
		{Name: "Duration", Data: b.duration},
		{Name: "StatusCode", Data: b.statusCode},
		{Name: "StatusMessage", Data: b.statusMessage},
		{Name: "ResourceAttributes", Data: b.resourceAttributes},
		{Name: "SpanAttributes", Data: b.spanAttributes},
		{Name: "Events.Timestamp", Data: b.eventsTimestamp},
		{Name: "Events.Name", Data: b.eventsName},
		{Name: "Events.Attributes", Data: b.eventsAttributes},
		{Name: "Links.TraceId", Data: b.linksTraceID},
		{Name: "Links.SpanId", Data: b.linksSpanID},
		{Name: "Links.TraceState", Data: b.linksTraceState},
		{Name: "Links.Attributes", Data: b.linksAttributes},
	}
}

func (w *Writer) runLogs(ctx context.Context, client *ch.Client) error {
	// Closure over client so a reconnect-swapped client is the one closed.
	defer func() { _ = client.Close() }()
	block := newLogsBlock()
	ticker := time.NewTicker(w.flushEvery)
	defer ticker.Stop()
	body := fmt.Sprintf(insertTmpl, w.database, LogsTable)
	// flush sends the current block under fctx (the normal path passes the
	// pump ctx so a retry blocks until CH recovers; shutdown passes a
	// detached, bounded ctx). The block is reset only on a committed insert,
	// so a transient failure holds the rows and the next flush re-sends them.
	flush := func(fctx context.Context, reason string) {
		if block.rows == 0 {
			return
		}
		var ok bool
		if client, ok = w.insertWithRetry(fctx, client, body, block.input(), block.rows, reason); ok {
			block = newLogsBlock()
		}
	}
	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.Background(), shutdownFlushTimeout)
			flush(fctx, "shutdown")
			cancel()
			return ctx.Err()
		case <-ticker.C:
			flush(ctx, "tick")
		case rls := <-w.logsCh:
			w.appendLogs(block, rls)
			if block.rows >= w.flushRows || block.bytes >= w.flushBytes {
				flush(ctx, "size")
			}
		}
	}
}

func (w *Writer) runTraces(ctx context.Context, client *ch.Client) error {
	// Closure over client so a reconnect-swapped client is the one closed.
	defer func() { _ = client.Close() }()
	block := newTracesBlock()
	ticker := time.NewTicker(w.flushEvery)
	defer ticker.Stop()
	body := fmt.Sprintf(insertTmpl, w.database, TracesTable)
	// See runLogs: the block is held and re-sent until it commits.
	flush := func(fctx context.Context, reason string) {
		if block.rows == 0 {
			return
		}
		var ok bool
		if client, ok = w.insertWithRetry(fctx, client, body, block.input(), block.rows, reason); ok {
			block = newTracesBlock()
		}
	}
	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.Background(), shutdownFlushTimeout)
			flush(fctx, "shutdown")
			cancel()
			return ctx.Err()
		case <-ticker.C:
			flush(ctx, "tick")
		case rss := <-w.tracesCh:
			w.appendTraces(block, rss)
			if block.rows >= w.flushRows || block.bytes >= w.flushBytes {
				flush(ctx, "size")
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
		resourceSchemaURL := rl.GetSchemaUrl()
		for _, sl := range rl.GetScopeLogs() {
			scopeSchemaURL := sl.GetSchemaUrl()
			scopeName := sl.GetScope().GetName()
			scopeVersion := sl.GetScope().GetVersion()
			scopeAttrs := otelemit.FlattenAttrs(sl.GetScope().GetAttributes())
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
				block.traceFlags.Append(uint8(lr.GetFlags()))
				block.severityText.Append(lr.GetSeverityText())
				block.severityNumber.Append(uint8(lr.GetSeverityNumber()))
				block.serviceName.Append(serviceName)
				block.body.Append(body)
				block.resourceSchemaURL.Append(resourceSchemaURL)
				block.resourceAttributes.Append(resAttrs)
				block.scopeSchemaURL.Append(scopeSchemaURL)
				block.scopeName.Append(scopeName)
				block.scopeVersion.Append(scopeVersion)
				block.scopeAttributes.Append(scopeAttrs)
				block.logAttributes.Append(logAttrs)
				block.rows++
				block.bytes += approxLogBytes(body, logAttrs, resAttrs)
			}
		}
	}
}

// appendTraces projects each Span in rss into block's columns. Span
// Events (the egress sink's request/response header+body captures) are
// persisted into the Events Nested column as parallel arrays; Links are
// written as empty arrays since no producer emits them today.
func (w *Writer) appendTraces(block *tracesBlock, rss []*tracepb.ResourceSpans) {
	for _, rs := range rss {
		resAttrs := otelemit.FlattenAttrs(rs.GetResource().GetAttributes())
		egRef := resAttrs[otelemit.AttrEgressGateway]
		serviceName := resAttrs[string(semconv.ServiceNameKey)]
		for _, ss := range rs.GetScopeSpans() {
			scopeName := ss.GetScope().GetName()
			scopeVersion := ss.GetScope().GetVersion()
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

				// Events: one parallel slice element per event on this span;
				// all three sub-arrays share the same length (driven by the
				// same sp.GetEvents() loop), satisfying the Nested invariant.
				evTimes := make([]time.Time, 0, len(sp.GetEvents()))
				evNames := make([]string, 0, len(sp.GetEvents()))
				evAttrs := make([]map[string]string, 0, len(sp.GetEvents()))
				for _, ev := range sp.GetEvents() {
					evTimes = append(evTimes, time.Unix(0, int64(ev.GetTimeUnixNano())))
					evNames = append(evNames, ev.GetName())
					evAttrs = append(evAttrs, otelemit.FlattenAttrs(ev.GetAttributes()))
				}

				block.timestamp.Append(start)
				block.egRef.Append(egRef)
				block.traceID.Append(hexOrEmpty(sp.GetTraceId()))
				block.spanID.Append(hexOrEmpty(sp.GetSpanId()))
				block.parentSpanID.Append(hexOrEmpty(sp.GetParentSpanId()))
				block.traceState.Append(sp.GetTraceState())
				block.spanName.Append(sp.GetName())
				block.spanKind.Append(spanKindStr(sp.GetKind()))
				block.serviceName.Append(serviceName)
				block.scopeName.Append(scopeName)
				block.scopeVersion.Append(scopeVersion)
				block.duration.Append(duration)
				block.statusCode.Append(statusCodeStr(sp.GetStatus().GetCode()))
				block.statusMessage.Append(sp.GetStatus().GetMessage())
				block.resourceAttributes.Append(resAttrs)
				block.spanAttributes.Append(spanAttrs)
				block.eventsTimestamp.Append(evTimes)
				block.eventsName.Append(evNames)
				block.eventsAttributes.Append(evAttrs)
				// Links are never emitted today; one empty array element per
				// row keeps the Nested sub-arrays aligned and the rows
				// round-trippable. A future link populator must hexOrEmpty()
				// the link's []byte trace/span ids before appending.
				block.linksTraceID.Append(nil)
				block.linksSpanID.Append(nil)
				block.linksTraceState.Append(nil)
				block.linksAttributes.Append(nil)
				block.rows++
				block.bytes += approxSpanBytes(sp.GetName(), spanAttrs, resAttrs)
				// Account for event bytes too: the body events carry base64
				// request/response payloads that can dwarf the span itself,
				// so they must gate the byte-based flush. Reuse the flattened
				// maps to avoid a second AnyValueString pass.
				for i := range evAttrs {
					block.bytes += len(evNames[i]) + 16 + attrsBytes(evAttrs[i])
				}
			}
		}
	}
}

// spanKindStr maps an OTLP span kind to the otel-collector-contrib
// stored form ("Client", "Server", ...) so off-the-shelf dashboards'
// SpanKind filters match. The read API reverses this mapping when
// reconstructing trace.proto.
func spanKindStr(k tracepb.Span_SpanKind) string {
	switch k {
	case tracepb.Span_SPAN_KIND_INTERNAL:
		return "Internal"
	case tracepb.Span_SPAN_KIND_SERVER:
		return "Server"
	case tracepb.Span_SPAN_KIND_CLIENT:
		return "Client"
	case tracepb.Span_SPAN_KIND_PRODUCER:
		return "Producer"
	case tracepb.Span_SPAN_KIND_CONSUMER:
		return "Consumer"
	default:
		return "Unspecified"
	}
}

// statusCodeStr maps an OTLP status code to the otel-collector-contrib
// stored form ("Unset", "Ok", "Error"). A nil span status decodes to
// STATUS_CODE_UNSET via the proto getter, yielding "Unset".
func statusCodeStr(c tracepb.Status_StatusCode) string {
	switch c {
	case tracepb.Status_STATUS_CODE_OK:
		return "Ok"
	case tracepb.Status_STATUS_CODE_ERROR:
		return "Error"
	default:
		return "Unset"
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
