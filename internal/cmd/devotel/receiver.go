// Package devotel implements an in-process OTLP/HTTP receiver that
// `clrk dev` runs to capture egress ext_proc telemetry without having
// to bundle a real otel-collector container. Decoded LogRecord and Span
// values are pushed into channels the dev TUI consumes.
package devotel

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/apoxy-dev/clrk/internal/otelemit"
	"github.com/apoxy-dev/clrk/internal/otelreceiver"
)

// LogRecord is the receiver's flattened view of one OTLP log record.
// Resource + scope attributes are merged into Attributes for renderers
// that don't care about the hierarchy.
type LogRecord struct {
	Time       time.Time
	Severity   string
	Body       string
	Attributes map[string]string
	TraceID    string
	SpanID    string
}

// Span is the receiver's flattened view of one OTLP span. Events are
// preserved verbatim; renderers can choose to expand them on a
// drill-down view.
type Span struct {
	Time       time.Time
	Duration   time.Duration
	TraceID    string
	SpanID     string
	ParentSpan string
	Name       string
	Status     string
	StatusMsg  string
	Attributes map[string]string
	Events     []SpanEvent
}

type SpanEvent struct {
	Time       time.Time
	Name       string
	Attributes map[string]string
}

// Receiver is a long-lived OTLP/HTTP listener bound to a single
// host:port. Logs() and Traces() return channels callers consume
// (e.g. the dev TUI). The channels are buffered so a slow consumer
// doesn't backpressure the data plane — overflow is dropped with a
// counted log line.
type Receiver struct {
	srv  *http.Server
	addr string

	logsCh  chan LogRecord
	spansCh chan Span

	mu          sync.Mutex
	dropped     uint64
	lastDropLog time.Time
}

// Start binds an OTLP/HTTP listener on addr and returns a Receiver
// that fans incoming logs/traces into Logs()/Traces(). Cancelling
// ctx triggers graceful shutdown.
func Start(ctx context.Context, addr string) (*Receiver, error) {
	r := &Receiver{
		logsCh:  make(chan LogRecord, 1024),
		spansCh: make(chan Span, 1024),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/logs", r.handleLogs)
	mux.HandleFunc("/v1/traces", r.handleTraces)
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		slog.Warn("devotel receiver got unknown path", "path", req.URL.Path, "method", req.Method)
		http.NotFound(w, req)
	})

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	r.addr = lis.Addr().String()
	r.srv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := r.srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("devotel receiver exited", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.srv.Shutdown(shutCtx)
	}()
	return r, nil
}

// Addr returns the bound address (host:port). Useful when Start was
// called with `:0` to pick a random free port.
func (r *Receiver) Addr() string { return r.addr }

// Logs returns the receive channel for decoded log records.
func (r *Receiver) Logs() <-chan LogRecord { return r.logsCh }

// Traces returns the receive channel for decoded spans.
func (r *Receiver) Traces() <-chan Span { return r.spansCh }

func (r *Receiver) handleLogs(w http.ResponseWriter, req *http.Request) {
	body, err := otelreceiver.ReadBody(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var msg collogspb.ExportLogsServiceRequest
	if err := proto.Unmarshal(body, &msg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	for _, rl := range msg.GetResourceLogs() {
		resAttrs := otelemit.FlattenAttrs(rl.GetResource().GetAttributes())
		for _, sl := range rl.GetScopeLogs() {
			for _, lr := range sl.GetLogRecords() {
				r.dispatchLog(decodeLog(lr, resAttrs))
			}
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(otelreceiver.EmptyLogsResp)
}

func (r *Receiver) handleTraces(w http.ResponseWriter, req *http.Request) {
	body, err := otelreceiver.ReadBody(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var msg coltracepb.ExportTraceServiceRequest
	if err := proto.Unmarshal(body, &msg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	for _, rs := range msg.GetResourceSpans() {
		resAttrs := otelemit.FlattenAttrs(rs.GetResource().GetAttributes())
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				r.dispatchSpan(decodeSpan(sp, resAttrs))
			}
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(otelreceiver.EmptyTracesResp)
}

func (r *Receiver) dispatchLog(rec LogRecord) {
	select {
	case r.logsCh <- rec:
	default:
		r.recordDrop("logs")
	}
}

func (r *Receiver) dispatchSpan(sp Span) {
	select {
	case r.spansCh <- sp:
	default:
		r.recordDrop("traces")
	}
}

// recordDrop counts overflow drops. We cap log noise at one line per
// 5s so a stuck consumer doesn't fill the cli pane.
func (r *Receiver) recordDrop(signal string) {
	r.mu.Lock()
	r.dropped++
	now := time.Now()
	if now.Sub(r.lastDropLog) > 5*time.Second {
		dropped := r.dropped
		r.dropped = 0
		r.lastDropLog = now
		r.mu.Unlock()
		slog.Warn("devotel dropped records", "signal", signal, "count", dropped)
		return
	}
	r.mu.Unlock()
}

func decodeLog(lr *logspb.LogRecord, resAttrs map[string]string) LogRecord {
	attrs := otelemit.MergeAttrs(resAttrs, otelemit.FlattenAttrs(lr.GetAttributes()))
	traceID := ""
	if id := lr.GetTraceId(); len(id) > 0 {
		traceID = hex.EncodeToString(id)
	}
	spanID := ""
	if id := lr.GetSpanId(); len(id) > 0 {
		spanID = hex.EncodeToString(id)
	}
	body := ""
	if b := lr.GetBody(); b != nil {
		body = otelemit.AnyValueString(b)
	}
	t := time.Unix(0, int64(lr.GetTimeUnixNano()))
	if t.IsZero() || lr.GetTimeUnixNano() == 0 {
		t = time.Now()
	}
	return LogRecord{
		Time:       t,
		Severity:   lr.GetSeverityNumber().String(),
		Body:       body,
		Attributes: attrs,
		TraceID:    traceID,
		SpanID:     spanID,
	}
}

func decodeSpan(sp *tracepb.Span, resAttrs map[string]string) Span {
	attrs := otelemit.MergeAttrs(resAttrs, otelemit.FlattenAttrs(sp.GetAttributes()))
	out := Span{
		Time:       time.Unix(0, int64(sp.GetStartTimeUnixNano())),
		Duration:   time.Duration(int64(sp.GetEndTimeUnixNano()) - int64(sp.GetStartTimeUnixNano())),
		TraceID:    hex.EncodeToString(sp.GetTraceId()),
		SpanID:     hex.EncodeToString(sp.GetSpanId()),
		ParentSpan: hex.EncodeToString(sp.GetParentSpanId()),
		Name:       sp.GetName(),
		Attributes: attrs,
	}
	if st := sp.GetStatus(); st != nil {
		out.Status = st.GetCode().String()
		out.StatusMsg = st.GetMessage()
	}
	for _, ev := range sp.GetEvents() {
		out.Events = append(out.Events, SpanEvent{
			Time:       time.Unix(0, int64(ev.GetTimeUnixNano())),
			Name:       ev.GetName(),
			Attributes: otelemit.FlattenAttrs(ev.GetAttributes()),
		})
	}
	return out
}
