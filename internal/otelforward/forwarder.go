// Package otelforward re-exports OTLP/HTTP signal bytes that the cm
// OTLP receiver captured to a customer-configured external endpoint.
// One Forwarder per EgressGateway; each owns a bounded channel and a
// background goroutine that POSTs raw protobuf bytes to the
// customer's endpoint with optional auth headers.
//
// Best-effort by design: cm always persists the captured signal to
// the embedded ClickHouse (internal/chwriter) which is the durable
// copy, so a wedged customer endpoint must never back-pressure the
// receiver.
package otelforward

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/otelemit"
)

const (
	// chanBuf sizes the per-direction outbound queue. Two seconds of
	// receive at typical agent QPS without dropping; drop+count kicks
	// in promptly when the customer endpoint is wedged.
	chanBuf = 256

	// httpTimeout is the per-POST upper bound. Customer endpoints are
	// typically off-cluster (vendor SaaS); 10s is generous but caps
	// goroutine pile-ups when the endpoint is slow rather than dead.
	httpTimeout = 10 * time.Second
)

// Forwarder ships raw OTLP/HTTP protobuf bytes to a single customer
// endpoint. Construct with New; run the pump with Run; enqueue
// payloads with EnqueueLogs / EnqueueTraces. Close drains for the
// passed deadline then exits.
type Forwarder struct {
	egRef     string
	logsURL   string
	tracesURL string
	headers   map[string]string

	client *http.Client

	logsCh   chan []byte
	tracesCh chan []byte

	logsDropped   atomic.Uint64
	tracesDropped atomic.Uint64

	done chan struct{}
}

// NewForwarder builds a Forwarder for the given EG and OTLP spec.
// Returns nil when spec.Endpoint is empty — callers should not
// register a noop forwarder; absence in the registry is the noop
// signal.
func NewForwarder(egRef string, spec clrkv1alpha1.OTLPLogsSinkSpec) *Forwarder {
	if spec.Endpoint == "" {
		return nil
	}
	headers := make(map[string]string, len(spec.Headers))
	for k, v := range spec.Headers {
		headers[k] = v
	}
	return &Forwarder{
		egRef:     egRef,
		logsURL:   otelemit.EndpointForSignal(spec.Endpoint, "/v1/logs"),
		tracesURL: otelemit.EndpointForSignal(spec.Endpoint, "/v1/traces"),
		headers:   headers,
		client:    &http.Client{Timeout: httpTimeout},
		logsCh:    make(chan []byte, chanBuf),
		tracesCh:  make(chan []byte, chanBuf),
		done:      make(chan struct{}),
	}
}

// EGRef returns "ns/name" of the EgressGateway this Forwarder serves.
func (f *Forwarder) EGRef() string { return f.egRef }

// EnqueueLogs hands a raw OTLP/HTTP-protobuf logs payload to the
// forwarder. Non-blocking; drops on overflow.
func (f *Forwarder) EnqueueLogs(body []byte) {
	select {
	case f.logsCh <- body:
	default:
		f.logsDropped.Add(1)
	}
}

// EnqueueTraces is the trace analog of EnqueueLogs.
func (f *Forwarder) EnqueueTraces(body []byte) {
	select {
	case f.tracesCh <- body:
	default:
		f.tracesDropped.Add(1)
	}
}

// LogsDropped / TracesDropped expose cumulative drop counters for
// the admin surface and tests.
func (f *Forwarder) LogsDropped() uint64   { return f.logsDropped.Load() }
func (f *Forwarder) TracesDropped() uint64 { return f.tracesDropped.Load() }

// Run pumps the queues until ctx is done or Close is called. Blocks.
func (f *Forwarder) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); f.pump(ctx, f.logsCh, f.logsURL, "logs") }()
	go func() { defer wg.Done(); f.pump(ctx, f.tracesCh, f.tracesURL, "traces") }()
	wg.Wait()
	close(f.done)
}

// Close waits up to the passed deadline for the pump goroutine to
// exit (it terminates when its Run ctx is cancelled). Returns
// context.DeadlineExceeded if the drain timer elapsed first.
func (f *Forwarder) Close(ctx context.Context) error {
	select {
	case <-f.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *Forwarder) pump(ctx context.Context, ch chan []byte, url, label string) {
	for {
		select {
		case <-ctx.Done():
			return
		case body := <-ch:
			if err := f.post(ctx, url, body); err != nil {
				slog.Warn("OTLP forward failed",
					"eg", f.egRef, "signal", label, "err", err)
			}
		}
	}
}

func (f *Forwarder) post(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	for k, v := range f.headers {
		req.Header.Set(k, v)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return fmt.Errorf("post %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("post %s: status %d", url, resp.StatusCode)
	}
	return nil
}
