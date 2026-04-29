package extproc

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egidentity"
)

// sinkShutdownTimeout bounds how long we wait for a replaced sink's
// background workers to flush during cache rebuild. OTLP exporters use
// it as a hard deadline.
const sinkShutdownTimeout = 5 * time.Second

// defaultIncludedContentTypes is the body-capture content-type allow-list
// applied when OTLP.CaptureBody.IncludeContentTypes is empty. Matched by
// case-insensitive prefix on the request/response content-type header.
var defaultIncludedContentTypes = []string{
	"application/json",
	"application/x-ndjson",
	"text/event-stream",
}

// egSink is a per-EgressGateway capture configuration: the resolved Sink
// (OTLP or fallback slog), the body-capture cap, the content-type
// allow-list, plus a snapshot of the spec fields we actually consume so
// we can decide whether the EG changed in a way that warrants a rebuild.
type egSink struct {
	sink            Sink
	maxCaptureBytes int
	includedTypes   []string
	specSnapshot    *clrkv1alpha1.OTLPLogsSinkSpec
	shutdown        func(context.Context) error
}

// sinkRegistry caches one egSink per EgressGateway, keyed by ns/name.
// Process() calls get(ctx) once at stream start; the registry resolves
// the calling Envoy back to its EG, fetches the EG from the
// controller-runtime cache, and returns the cached sink (rebuilding it
// if EG.ResourceVersion has changed).
type sinkRegistry struct {
	client client.Client

	mu sync.Mutex
	by map[types.NamespacedName]*egSink
}

func newSinkRegistry(c client.Client) *sinkRegistry {
	return &sinkRegistry{
		client: c,
		by:     make(map[types.NamespacedName]*egSink),
	}
}

// get resolves the EgressGateway from ctx (stamped by the egidentity
// gRPC interceptor off the incoming HTTP/2 :authority header) and
// returns the (possibly newly-built) cached egSink. Returns an error
// when no EG identity was attached or the controller-runtime client
// isn't wired — callers fall back to slogSink in that case.
//
// Cache invalidation is keyed on the OTLP spec snapshot, not
// EG.ResourceVersion. The EG controller writes status frequently
// (cert rotation, condition flips); rebuilding the OTLP exporter pair
// on every status write would churn batcher goroutines for no signal
// change.
func (r *sinkRegistry) get(ctx context.Context) (*egSink, error) {
	if r.client == nil {
		return nil, fmt.Errorf("no kube client configured")
	}
	key, err := egidentity.MustFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve EgressGateway: %w", err)
	}

	var eg clrkv1alpha1.EgressGateway
	if err := r.client.Get(ctx, key, &eg); err != nil {
		return nil, fmt.Errorf("get EgressGateway %s: %w", key, err)
	}

	r.mu.Lock()
	if hit, ok := r.by[key]; ok && reflect.DeepEqual(hit.specSnapshot, eg.Spec.OTLP) {
		r.mu.Unlock()
		return hit, nil
	}
	prev := r.by[key]
	r.mu.Unlock()

	// Build the new sink before tearing down the old one — if the rebuild
	// errors (bad endpoint, etc.) the previous sink stays live so we
	// keep emitting somewhere instead of dropping records on the floor.
	built, err := buildEgSink(ctx, &eg)
	if err != nil {
		return nil, fmt.Errorf("build sink: %w", err)
	}

	r.mu.Lock()
	r.by[key] = built
	r.mu.Unlock()

	// Shutdown the predecessor in the background — it owns an OTLP
	// exporter goroutine that needs to flush. Caller doesn't wait.
	if prev != nil && prev.shutdown != nil {
		go func(sd func(context.Context) error) {
			shutCtx, cancel := context.WithTimeout(context.Background(), sinkShutdownTimeout)
			defer cancel()
			_ = sd(shutCtx)
		}(prev.shutdown)
	}
	return built, nil
}

// shutdownAll tears down every cached sink. Best-effort: errors are
// logged via slog and don't block teardown of the rest.
func (r *sinkRegistry) shutdownAll(ctx context.Context) {
	r.mu.Lock()
	sinks := r.by
	r.by = make(map[types.NamespacedName]*egSink)
	r.mu.Unlock()
	for _, s := range sinks {
		if s.shutdown == nil {
			continue
		}
		_ = s.shutdown(ctx)
	}
}

// buildEgSink builds a fresh egSink for the given EG spec. Picks OTLP
// when an endpoint is configured, falls back to slogSink otherwise.
func buildEgSink(ctx context.Context, eg *clrkv1alpha1.EgressGateway) (*egSink, error) {
	maxBytes := captureMaxBytesDefault
	var included []string
	if eg.Spec.OTLP != nil && eg.Spec.OTLP.CaptureBody != nil {
		cb := eg.Spec.OTLP.CaptureBody
		if cb.MaxBytes != nil && *cb.MaxBytes > 0 {
			maxBytes = int(*cb.MaxBytes)
		}
		if len(cb.IncludeContentTypes) > 0 {
			included = lowerAll(cb.IncludeContentTypes)
		}
	}
	if len(included) == 0 {
		included = defaultIncludedContentTypes
	}

	out := &egSink{
		maxCaptureBytes: maxBytes,
		includedTypes:   included,
		specSnapshot:    eg.Spec.OTLP.DeepCopy(),
	}

	if eg.Spec.OTLP != nil && eg.Spec.OTLP.Endpoint != "" {
		sink, shutdown, err := newOTLPSink(ctx, *eg.Spec.OTLP)
		if err != nil {
			return nil, fmt.Errorf("otlp sink: %w", err)
		}
		out.sink = sink
		out.shutdown = shutdown
		return out, nil
	}

	out.sink = slogSink{}
	return out, nil
}

func lowerAll(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = strings.ToLower(strings.TrimSpace(s))
	}
	return out
}

// contentTypeIncluded reports whether the given Content-Type header
// matches any prefix in includedTypes. An empty includedTypes slice
// means "capture everything" — used when EG resolution fell back to
// slogSink and we never derived a per-EG allow-list.
func contentTypeIncluded(contentType string, includedTypes []string) bool {
	if len(includedTypes) == 0 {
		return true
	}
	ct := strings.ToLower(contentType)
	for _, t := range includedTypes {
		if strings.HasPrefix(ct, t) {
			return true
		}
	}
	return false
}
