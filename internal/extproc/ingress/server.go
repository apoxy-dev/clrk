// Package ingress implements the TaskAgent ingress ext_proc filter.
// It runs in controller-manager, is wired by a per-TaskAgent
// EnvoyExtensionPolicy onto the per-TaskAgent HTTPRoute, and rewrites
// `:authority` on each inbound request to the worker pod IP picked by
// WorkerHealthChecker. The HTTPRoute backend is a per-TaskAgent
// `Backend` with `spec.type: DynamicResolver`, so EG generates a
// dynamic_forward_proxy cluster that dials whatever host this filter
// sets on the request.
package ingress

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/cloudevents"
	"github.com/apoxy-dev/clrk/internal/extproc/invocationctx"
	"github.com/apoxy-dev/clrk/internal/extproc/tracectx"
	"github.com/apoxy-dev/clrk/internal/healthcheck"
	"github.com/apoxy-dev/clrk/internal/ports"
)

// Picker is the subset of healthcheck.WorkerHealthChecker the
// server depends on. Defined here so unit tests can supply a fake.
type Picker interface {
	Pick(pool types.NamespacedName, ns, agent, revision string, maxConcurrent uint32, tieBreaker string) (healthcheck.PickResult, bool)
}

// Server implements envoy.service.ext_proc.v3.ExternalProcessor for
// inbound TaskAgent traffic. One stream per HTTP transaction; we only
// act on RequestHeaders and continue everything else untouched.
type Server struct {
	extprocv3.UnimplementedExternalProcessorServer

	client client.Client
	picker Picker

	// invocations carries the inbound W3C parent context to the egress
	// ext_proc, keyed by the per-request invocation-id we stamp on
	// the request to the worker. Nil-safe — when omitted (tests, or
	// before the controller-manager wires it), trace propagation is a
	// no-op and downstream egress spans start as roots.
	invocations *invocationctx.Store
}

// New constructs an ingress ext_proc server.
func New(c client.Client, picker Picker, invocations *invocationctx.Store) *Server {
	return &Server{client: c, picker: picker, invocations: invocations}
}

// Process handles one ext_proc stream. The only message we act on is
// RequestHeaders — we use it to pick a worker and rewrite :authority
// for the dynamic_forward_proxy cluster downstream. Body/trailer
// phases are continued unchanged.
func (s *Server) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	ctx := stream.Context()

	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		switch m := req.Request.(type) {
		case *extprocv3.ProcessingRequest_RequestHeaders:
			resp := s.handleRequestHeaders(ctx, m.RequestHeaders)
			if err := stream.Send(resp); err != nil {
				return err
			}
		default:
			if err := stream.Send(continueResponse()); err != nil {
				return err
			}
		}
	}
}

// handleRequestHeaders performs the worker pick and builds a
// CommonResponse rewriting :authority. On any error reachable from
// the agent's perspective we return an ImmediateResponse with the
// right HTTP status; on internal errors we return a continue and let
// the request fall through to the static fallback path (whatever
// EG's DFP cluster does with a missing host).
func (s *Server) handleRequestHeaders(ctx context.Context, in *extprocv3.HttpHeaders) *extprocv3.ProcessingResponse {
	hdrs := headersToMap(in.Headers)
	taHdr := hdrs[strings.ToLower(ports.HeaderTaskAgent)]
	if taHdr == "" {
		return immediateResponse(typev3.StatusCode_BadRequest, "clrk: missing "+ports.HeaderTaskAgent+" header")
	}
	ns, name, ok := strings.Cut(taHdr, "/")
	if !ok || ns == "" || name == "" {
		return immediateResponse(typev3.StatusCode_BadRequest, "clrk: invalid "+ports.HeaderTaskAgent+" (want ns/name)")
	}

	var ta clrkv1alpha1.TaskAgent
	if err := s.client.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &ta); err != nil {
		if apierrors.IsNotFound(err) {
			return immediateResponse(typev3.StatusCode_NotFound, "clrk: TaskAgent not found")
		}
		slog.Error("ingress ext_proc: TaskAgent lookup failed", "ns", ns, "name", name, "err", err)
		return immediateResponse(typev3.StatusCode_InternalServerError, "clrk: TaskAgent lookup failed")
	}

	if ta.Status.LatestReadyRevisionName == "" {
		slog.Warn("ingress ext_proc: TaskAgent has no ready revision", "ns", ns, "name", name)
		return immediateResponse(typev3.StatusCode_ServiceUnavailable, "clrk: TaskAgent has no ready revision")
	}

	pool := types.NamespacedName{Namespace: ns, Name: ta.Spec.WorkerPoolRef}
	maxConcurrent := uint32(0)
	if ta.Spec.MaxConcurrent != nil && *ta.Spec.MaxConcurrent > 0 {
		maxConcurrent = uint32(*ta.Spec.MaxConcurrent)
	}

	tieBreaker := hdrs[strings.ToLower(ports.HeaderExecutionID)]
	if tieBreaker == "" {
		// Fall back to the request-id header Envoy auto-injects so we
		// still spread thundering-herd across workers when the caller
		// didn't supply an execution-id.
		tieBreaker = hdrs["x-request-id"]
	}

	pick, ok := s.picker.Pick(pool, ns, name, ta.Status.LatestReadyRevisionName, maxConcurrent, tieBreaker)
	if !ok {
		if pick.AlreadyAtCap {
			slog.Warn("ingress ext_proc: TaskAgent at MaxConcurrent", "ns", ns, "name", name)
			return immediateResponse(typev3.StatusCode_TooManyRequests, "clrk: TaskAgent at MaxConcurrent across the cluster")
		}
		slog.Warn("ingress ext_proc: no ready worker", "ns", ns, "name", name, "pool", pool, "revision", ta.Status.LatestReadyRevisionName)
		return immediateResponse(typev3.StatusCode_ServiceUnavailable, "clrk: no ready worker for TaskAgent")
	}

	// Rewrite :authority so EG's dynamic_forward_proxy cluster dials
	// the picked pod IP:port. Also stamp the chosen endpoint into a
	// header for telemetry / observability.
	setHeaders := []*corev3.HeaderValueOption{
		{
			Header: &corev3.HeaderValue{
				Key:      ":authority",
				RawValue: []byte(pick.Addr),
			},
		},
		{
			Header: &corev3.HeaderValue{
				Key:      "host",
				RawValue: []byte(pick.Addr),
			},
		},
		{
			Header: &corev3.HeaderValue{
				Key:      ports.HeaderWorkerEndpoint,
				RawValue: []byte(pick.Addr),
			},
		},
	}

	// Per-invocation id: a fresh UUID, decoupled from any caller-set
	// idempotency key. This is what flows through PROXY v2 TLVs to the
	// egress ext_proc as invocation.id and is the lookup key for the
	// invocationctx store. The dispatcher reads HeaderInvocationID and
	// pins it into the sandbox's IdentityDialer before Start.
	invocationID := uuid.NewString()
	setHeaders = append(setHeaders, &corev3.HeaderValueOption{
		Header: &corev3.HeaderValue{
			Key:      ports.HeaderInvocationID,
			RawValue: []byte(invocationID),
		},
	})

	// Capture (or synthesize) the inbound W3C trace parent and stash
	// it under the invocation-id for the egress ext_proc to recover.
	// Synthesizing a sampled root for un-instrumented callers is
	// deliberate: without it the agent's outbound LLM/MCP calls
	// produce orphan egress spans the operator can't correlate to any
	// inbound request. The synthesized root is sampled (`01`) so
	// downstream sampling decisions consistently keep the chain.
	parent := resolveOrSynthesizeParent(hdrs)
	if s.invocations != nil {
		s.invocations.Put(invocationID, parent)
	}
	traceparent, tracestate := tracectx.Inject(parent)
	if traceparent != "" {
		setHeaders = append(setHeaders, &corev3.HeaderValueOption{
			Header: &corev3.HeaderValue{
				Key:      tracectx.HeaderTraceparent,
				RawValue: []byte(traceparent),
			},
		})
	}
	if tracestate != "" {
		setHeaders = append(setHeaders, &corev3.HeaderValueOption{
			Header: &corev3.HeaderValue{
				Key:      tracectx.HeaderTracestate,
				RawValue: []byte(tracestate),
			},
		})
	}

	// Stamp CloudEvents binary-mode `ce-*` headers. The worker
	// dispatcher reads them off the request to construct the
	// envelope (Stdin mode) or serve via /v1/event (Metadata mode).
	// Existing ce-* headers from the caller win — passThrough is
	// extracted from the request's existing ce-* headers.
	passThrough := cloudevents.CEHeaderIterMap(cloudevents.HeaderMap(hdrs))
	ceAttrs := cloudevents.AttrsFromHeaders(cloudevents.HeaderMap(hdrs), &ta, passThrough)
	for k, v := range ceAttrs {
		setHeaders = append(setHeaders, &corev3.HeaderValueOption{
			Header: &corev3.HeaderValue{
				Key:      "ce-" + k,
				RawValue: []byte(v),
			},
		})
	}

	mutation := &extprocv3.HeaderMutation{SetHeaders: setHeaders}

	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_RequestHeaders{
			RequestHeaders: &extprocv3.HeadersResponse{
				Response: &extprocv3.CommonResponse{
					Status:         extprocv3.CommonResponse_CONTINUE,
					HeaderMutation: mutation,
				},
			},
		},
	}
}

// resolveOrSynthesizeParent returns the inbound request's W3C trace
// parent if present, or a freshly minted sampled root otherwise.
// Synthesis means the trace exists end-to-end even when the caller
// isn't OTel-aware — the egress ext_proc will continue this id on
// outbound LLM/MCP calls regardless.
func resolveOrSynthesizeParent(hdrs map[string]string) trace.SpanContext {
	if ctx := tracectx.Extract(context.Background(), hdrs); ctx != nil {
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			return sc
		}
	}
	var traceID trace.TraceID
	var spanID trace.SpanID
	_, _ = rand.Read(traceID[:])
	_, _ = rand.Read(spanID[:])
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
}

func continueResponse() *extprocv3.ProcessingResponse {
	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_RequestHeaders{
			RequestHeaders: &extprocv3.HeadersResponse{
				Response: &extprocv3.CommonResponse{Status: extprocv3.CommonResponse_CONTINUE},
			},
		},
	}
}

func immediateResponse(code typev3.StatusCode, body string) *extprocv3.ProcessingResponse {
	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_ImmediateResponse{
			ImmediateResponse: &extprocv3.ImmediateResponse{
				Status: &typev3.HttpStatus{Code: code},
				Body:   []byte(body),
			},
		},
	}
}

func headersToMap(h *corev3.HeaderMap) map[string]string {
	out := make(map[string]string, 8)
	if h == nil {
		return out
	}
	for _, e := range h.Headers {
		k := strings.ToLower(e.Key)
		if v := e.RawValue; len(v) > 0 {
			out[k] = string(v)
		} else {
			out[k] = e.Value
		}
	}
	return out
}
