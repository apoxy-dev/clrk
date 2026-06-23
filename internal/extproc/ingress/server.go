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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/cloudevents"
	"github.com/apoxy-dev/clrk/internal/extproc/capture"
	"github.com/apoxy-dev/clrk/internal/extproc/invocationctx"
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
	"github.com/apoxy-dev/clrk/internal/extproc/tracectx"
	"github.com/apoxy-dev/clrk/internal/healthcheck"
	"github.com/apoxy-dev/clrk/internal/invevent"
	"github.com/apoxy-dev/clrk/internal/otelemit"
	"github.com/apoxy-dev/clrk/internal/ports"
)

// Picker is the subset of healthcheck.WorkerHealthChecker the
// server depends on. Defined here so unit tests can supply a fake.
type Picker interface {
	Pick(pool types.NamespacedName, ns, agent, revision string, maxConcurrent uint32, tieBreaker string) (healthcheck.PickResult, bool)
}

// emitterCell wraps the Emitter interface so we can store it in an
// atomic.Pointer (Go atomics cannot hold interface types directly).
type emitterCell struct {
	em otelemit.Emitter
}

// Server implements envoy.service.ext_proc.v3.ExternalProcessor for
// inbound TaskAgent traffic. One stream per HTTP transaction; we act on
// RequestHeaders (worker pick + :authority rewrite) and observe
// RequestBody (the trigger payload, captured onto the dispatch span),
// continuing everything else untouched.
type Server struct {
	extprocv3.UnimplementedExternalProcessorServer

	client client.Client
	picker Picker

	// invocations carries the ingress.dispatch span's SpanContext to
	// the egress ext_proc, keyed by the per-request invocation-id we
	// stamp on the request to the worker. Nil-safe — when omitted
	// (tests, or before the controller-manager wires it), trace
	// propagation is a no-op and downstream egress spans start as roots.
	invocations *invocationctx.Store

	// emitter holds the live OTLP emitter, swappable atomically via
	// SwapEmitter for graceful shutdown (cm replaces the live emitter
	// with otelemit.Noop() before closing the old one). Always non-nil
	// — New seeds it with the caller-supplied emitter (or
	// otelemit.Noop() if nil) so Process can Load unconditionally.
	emitter atomic.Pointer[emitterCell]

	// invPub publishes Invocation lifecycle birth events (Dispatched /
	// Rejected) to the JetStream INVOCATIONS stream. Nil until the
	// controller-manager wires it via RunInvocationPublisher once the
	// embedded NATS server is ready; nil-safe so dispatch works
	// unchanged when NATS is disabled or still starting. See
	// invpublish.go.
	invPub   atomic.Pointer[invevent.Publisher]
	invQueue chan *clrkv1alpha1.Invocation
}

// New constructs an ingress ext_proc server. A nil Emitter falls back
// to otelemit.Noop() so callers can wire telemetry lazily.
func New(c client.Client, picker Picker, invocations *invocationctx.Store, em otelemit.Emitter) *Server {
	if em == nil {
		em = otelemit.Noop()
	}
	s := &Server{
		client:      c,
		picker:      picker,
		invocations: invocations,
		invQueue:    make(chan *clrkv1alpha1.Invocation, invQueueSize),
	}
	s.emitter.Store(&emitterCell{em: em})
	return s
}

// SwapEmitter atomically replaces the live emitter and returns the
// displaced one. The caller owns the returned emitter — it must be
// Closed (typically in a background goroutine with a deadline) to
// release the OTLP exporter's batcher and connection. Passing nil is
// equivalent to passing otelemit.Noop().
func (s *Server) SwapEmitter(em otelemit.Emitter) otelemit.Emitter {
	if em == nil {
		em = otelemit.Noop()
	}
	prev := s.emitter.Swap(&emitterCell{em: em})
	if prev == nil {
		return nil
	}
	return prev.em
}

// Process handles one ext_proc stream. RequestHeaders drives the worker
// pick and :authority rewrite for the dynamic_forward_proxy cluster
// downstream. RequestBody is observed — not mutated — to capture the
// trigger payload onto the dispatch span (see below). Trailer phases are
// continued unchanged.
//
// The ingress.dispatch span is initialized lazily on the first
// RequestHeaders message (we have no parent context to root it under
// until then). It ends when the stream exits — EOF, error, or
// ImmediateResponse-induced close — so the span's wall-clock duration
// reflects the entire dispatch transaction.
//
// Request-body capture: the ingress EnvoyExtensionPolicy already runs
// RequestBodyMode=STREAMED (taskagent_ingress_controller.go) so the
// dispatcher can pump the sandbox's stdin without waiting on full-body
// buffering. That mode means Envoy already streams every request-body
// chunk to this server; they were previously dropped. We accumulate them
// (keep-first-N, bounded by captureMaxBytesDefault, gated on the
// content-type allow-list) and emit one http.request.body span event at
// end-of-stream using the same clrk.body.* contract the egress sink emits
// (otelemit.BodyEventAttrs), so the console renders the root span's
// request body with no front-end change. Capture is observe-only: every
// chunk is forwarded unchanged, preserving the no-buffering dispatch path.
func (s *Server) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	ctx := stream.Context()
	var span trace.Span
	// Per-stream request-body capture state, populated from RequestHeaders
	// (which always precedes RequestBody) and accumulated across chunks.
	var (
		captureBody  bool
		reqEncoding  string
		reqBody      []byte
		reqBytesLeft = capture.MaxBytesDefault
		reqTruncated bool
		reqBodyAt    time.Time
		bodyEmitted  bool
	)
	defer func() {
		if span == nil {
			return
		}
		// Flush a partially-captured body if the stream ended without an
		// end-of-stream chunk (e.g. the client aborted mid-upload), matching
		// the egress sink which emits whatever it captured at stream end. A
		// stream that reached EOS already emitted (bodyEmitted), so this
		// never double-emits.
		if captureBody && !bodyEmitted && len(reqBody) > 0 {
			emitRequestBodyEvent(span, reqBody, reqTruncated, reqEncoding, reqBodyAt)
		}
		span.End()
	}()

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
			hdrs := headersToMap(m.RequestHeaders.Headers)
			if span == nil {
				// Load the live emitter for this stream's whole lifetime
				// (a subsequent SwapEmitter must not retroactively reroute
				// in-flight spans — the displaced emitter's Close drains
				// them). The same tracer is reused for any later
				// RequestHeaders message on this stream.
				tracer := s.emitter.Load().em.Tracer()
				parent := resolveOrSynthesizeParent(hdrs)
				parentCtx := trace.ContextWithSpanContext(ctx, parent)
				_, span = tracer.Start(parentCtx, otelemit.SpanNameIngressDispatch,
					trace.WithSpanKind(trace.SpanKindServer))
			}
			// Gate request-body capture on the content-type allow-list
			// (the same primitive the egress sink uses) and stash the
			// content-encoding so the body event can undo/label it at
			// end-of-stream.
			captureBody = capture.ContentTypeIncluded(hdrs["content-type"], ingressIncludedContentTypes)
			reqEncoding = hdrs["content-encoding"]
			resp := s.handleRequestHeaders(ctx, hdrs, span)
			if err := stream.Send(resp); err != nil {
				return err
			}
		case *extprocv3.ProcessingRequest_RequestBody:
			// Observe-only: accumulate the trigger payload (bounded,
			// keep-first-N) and emit the body span event once the stream
			// ends, then always forward the chunk unchanged.
			if captureBody && span != nil && !bodyEmitted {
				chunk := m.RequestBody.GetBody()
				if reqBodyAt.IsZero() && len(chunk) > 0 {
					reqBodyAt = time.Now()
				}
				var trunc bool
				reqBody, trunc = capture.AppendBounded(reqBody, chunk, &reqBytesLeft)
				reqTruncated = reqTruncated || trunc
				if m.RequestBody.GetEndOfStream() {
					emitRequestBodyEvent(span, reqBody, reqTruncated, reqEncoding, reqBodyAt)
					bodyEmitted = true
				}
			}
			if err := stream.Send(bodyContinue()); err != nil {
				return err
			}
		default:
			// Phases the ingress filter doesn't act on. Reply with a
			// CONTINUE of the MATCHING type -- a phase-mismatched response
			// (e.g. a RequestHeaders-typed reply to a ResponseBody) would
			// reset the stream. Under the ingress ProcessingMode (response +
			// trailer modes SKIP'd) none of these arrive today; this keeps
			// the filter correct if a future policy enables them.
			if resp := continueUnchanged(req); resp != nil {
				if err := stream.Send(resp); err != nil {
					return err
				}
			}
		}
	}
}

// stampOutcome records the dispatch decision on the span and flips
// span status to Error for any non-2xx outcome. Centralized so every
// return path in handleRequestHeaders stamps the same shape.
func stampOutcome(span trace.Span, outcome string, status int) {
	span.SetAttributes(
		attribute.String(otelemit.AttrIngressOutcome, outcome),
		semconv.HTTPResponseStatusCode(status),
	)
	if status >= 400 {
		// error.type (OTel HTTP server span semconv) — the clrk outcome
		// enum is low-cardinality and predictable, exactly what the attr
		// wants. Pairs with the span status flip below.
		span.SetAttributes(semconv.ErrorTypeKey.String(outcome))
		span.SetStatus(codes.Error, outcome)
	}
}

// stampHTTPServerAttrs records the OTel HTTP server span semconv
// attributes derivable from the inbound request onto the dispatch span:
// the request line (method/scheme/path/query/url), the server authority
// the client addressed, the originating client address, the user agent,
// and the declared request body size. Stamped for every outcome -- including
// rejections -- so an operator sees what was requested even on a 400/404.
// http.response.status_code / error.type are stamped per-outcome by
// stampOutcome. The span keeps its wire-frozen ingress.dispatch name (the
// read model identifies the root span by it) rather than the semconv
// "{method} {route}" form.
//
// :authority is read here, before handleRequestHeaders rewrites it to the
// picked worker, so server.address is the gateway host the client hit --
// not the internal pod endpoint.
func stampHTTPServerAttrs(span trace.Span, hdrs map[string]string) {
	method := hdrs[":method"]
	scheme := hdrs[":scheme"]
	if scheme == "" {
		scheme = "https"
	}
	path, query := capture.SplitPathQuery(hdrs[":path"])
	authority := hdrs[":authority"]
	host, port := capture.SplitAuthority(authority)

	attrs := make([]attribute.KeyValue, 0, 10)
	if method != "" {
		attrs = append(attrs, semconv.HTTPRequestMethodKey.String(method))
	}
	attrs = append(attrs, semconv.URLScheme(scheme))
	if path != "" {
		attrs = append(attrs, semconv.URLPath(path))
	}
	if query != "" {
		attrs = append(attrs, semconv.URLQuery(query))
	}
	if authority != "" {
		attrs = append(attrs, semconv.ServerAddress(host), semconv.URLFull(capture.BuildURL(scheme, authority, path, query)))
		if port > 0 {
			attrs = append(attrs, semconv.ServerPort(port))
		}
	}
	if ca := clientAddress(hdrs); ca != "" {
		attrs = append(attrs, semconv.ClientAddress(ca))
	}
	if ua := hdrs["user-agent"]; ua != "" {
		attrs = append(attrs, semconv.UserAgentOriginal(ua))
	}
	if n, ok := contentLength(hdrs["content-length"]); ok {
		attrs = append(attrs, semconv.HTTPRequestBodySize(n))
	}
	span.SetAttributes(attrs...)
}

// handleRequestHeaders performs the worker pick and builds a
// CommonResponse rewriting :authority. On any error reachable from
// the agent's perspective we return an ImmediateResponse with the
// right HTTP status; on internal errors we return a continue and let
// the request fall through to the static fallback path (whatever
// EG's DFP cluster does with a missing host).
//
// The span argument is the ingress.dispatch span; this method stamps
// outcome / status / agent-identity attributes on it before each
// return.
func (s *Server) handleRequestHeaders(ctx context.Context, hdrs map[string]string, span trace.Span) *extprocv3.ProcessingResponse {
	// Stamp the standard HTTP server span attributes + the request-header
	// event first, so they land on every outcome including the early
	// rejections below.
	stampHTTPServerAttrs(span, hdrs)
	emitRequestHeadersEvent(span, hdrs)

	taHdr := hdrs[strings.ToLower(ports.HeaderTaskAgent)]
	if taHdr == "" {
		stampOutcome(span, otelemit.IngressOutcomeBadRequest, 400)
		return immediateResponse(typev3.StatusCode_BadRequest, "clrk: missing "+ports.HeaderTaskAgent+" header")
	}
	ns, name, ok := strings.Cut(taHdr, "/")
	if !ok || ns == "" || name == "" {
		stampOutcome(span, otelemit.IngressOutcomeBadRequest, 400)
		return immediateResponse(typev3.StatusCode_BadRequest, "clrk: invalid "+ports.HeaderTaskAgent+" (want ns/name)")
	}

	// Stamp both the ingress-specific clrk.taskagent.* attrs and the
	// canonical agent.* attrs. Ingress only ever dispatches TaskAgents,
	// so the kind is fixed. The agent.* set is what the chwriter
	// materialized Agent column reads, so without it agent-keyed reads
	// would silently miss the ingress.dispatch span (APO-717).
	span.SetAttributes(
		attribute.String(otelemit.AttrTaskAgentNamespace, ns),
		attribute.String(otelemit.AttrTaskAgentName, name),
		attribute.String(otelemit.AttrAgentKind, clrkv1alpha1.AgentKindTask),
		attribute.String(otelemit.AttrAgentNamespace, ns),
		attribute.String(otelemit.AttrAgentName, name),
	)

	var ta clrkv1alpha1.TaskAgent
	if err := s.client.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &ta); err != nil {
		if apierrors.IsNotFound(err) {
			stampOutcome(span, otelemit.IngressOutcomeNotFound, 404)
			return immediateResponse(typev3.StatusCode_NotFound, "clrk: TaskAgent not found")
		}
		slog.Error("ingress ext_proc: TaskAgent lookup failed", "ns", ns, "name", name, "err", err)
		stampOutcome(span, otelemit.IngressOutcomeInternal, 500)
		return immediateResponse(typev3.StatusCode_InternalServerError, "clrk: TaskAgent lookup failed")
	}

	// Resolve the per-invocation id as soon as the parent TaskAgent is
	// resolved, so the Dispatched (accept) and Rejected (capacity /
	// readiness denial) lifecycle events below all carry the same id.
	// This is what flows through PROXY v2 TLVs to the egress ext_proc as
	// invocation.id, the lookup key for the invocationctx store, and (on
	// accept) the X-Clrk-Invocation-Id the dispatcher pins into the
	// sandbox before Start. Denials reachable before this point (bad
	// header, TaskAgent not found, lookup error) have no resolvable
	// parent agent, so they record no Invocation.
	//
	// Honor a caller-supplied X-Clrk-Invocation-ID when it is a canonical
	// UUID -- the run-task CLI mints one and sends it so it can follow its
	// own run to a terminal phase and derive an exit code without the id
	// ever being echoed back. The id becomes the Invocation's
	// metadata.name verbatim (invpublish.go), so only the canonical form
	// uuid.NewString emits is honored; any other shape (non-UUID, braces,
	// uppercase) falls back to a freshly minted id rather than being
	// rejected, so a malformed header can never deny an otherwise valid
	// request.
	invocationID := canonicalInvocationID(hdrs[strings.ToLower(ports.HeaderInvocationID)])
	if invocationID == "" {
		invocationID = uuid.NewString()
	}
	span.SetAttributes(attribute.String(otelemit.AttrInvocationID, invocationID))

	// Classify the trigger from the inbound header so the Dispatched and
	// Rejected events ingress emits carry the right source (the run-task
	// CLI sets X-Clrk-Trigger: cli; the cron invoker sets cron). Without
	// this a Rejected CLI/cron invocation -- which produces no later
	// worker events to overwrite it in the argMax read model -- would be
	// mislabeled HTTP. Defaults to HTTP for ordinary ingress traffic.
	trigger := clrkv1alpha1.TriggerTypeFromHeaderValue(hdrs[strings.ToLower(ports.HeaderTrigger)])

	if ta.Status.LatestReadyRevisionName == "" {
		slog.Warn("ingress ext_proc: TaskAgent has no ready revision", "ns", ns, "name", name)
		stampOutcome(span, otelemit.IngressOutcomeNoReadyRevision, 503)
		s.emitInvocation(ns, name, invocationID, trigger, clrkv1alpha1.InvocationPhaseRejected)
		return immediateResponse(typev3.StatusCode_ServiceUnavailable, "clrk: TaskAgent has no ready revision")
	}

	span.SetAttributes(
		attribute.String(otelemit.AttrTaskAgentRevision, ta.Status.LatestReadyRevisionName),
		attribute.String(otelemit.AttrWorkerPool, ta.Spec.WorkerPoolRef),
	)

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
			stampOutcome(span, otelemit.IngressOutcomeAtMaxConcurrent, 429)
			s.emitInvocation(ns, name, invocationID, trigger, clrkv1alpha1.InvocationPhaseRejected)
			return immediateResponse(typev3.StatusCode_TooManyRequests, "clrk: TaskAgent at MaxConcurrent across the cluster")
		}
		slog.Warn("ingress ext_proc: no ready worker", "ns", ns, "name", name, "pool", pool, "revision", ta.Status.LatestReadyRevisionName)
		stampOutcome(span, otelemit.IngressOutcomeNoReadyWorker, 503)
		s.emitInvocation(ns, name, invocationID, trigger, clrkv1alpha1.InvocationPhaseRejected)
		return immediateResponse(typev3.StatusCode_ServiceUnavailable, "clrk: no ready worker for TaskAgent")
	}

	span.SetAttributes(attribute.String(otelemit.AttrWorkerAddr, pick.Addr))

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

	// Stamp the per-invocation id (minted above when the parent
	// resolved) onto the request: the dispatcher reads HeaderInvocationID
	// and pins it into the sandbox's IdentityDialer before Start, and it
	// flows through PROXY v2 TLVs to the egress ext_proc as invocation.id.
	setHeaders = append(setHeaders, &corev3.HeaderValueOption{
		Header: &corev3.HeaderValue{
			Key:      ports.HeaderInvocationID,
			RawValue: []byte(invocationID),
		},
	})

	// Re-chain: store the ingress.dispatch span's SpanContext (not the
	// raw inbound parent) so the egress ext_proc's outbound LLM/MCP
	// spans become children of ingress.dispatch. The worker forwards
	// the same traceparent, so OTel-instrumented agents echoing it
	// also chain correctly.
	ingressSC := span.SpanContext()
	if s.invocations != nil {
		s.invocations.Put(invocationID, ingressSC)
	}
	traceparent, tracestate := tracectx.Inject(ingressSC)
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

	stampOutcome(span, otelemit.IngressOutcomeOK, 200)
	s.emitInvocation(ns, name, invocationID, trigger, clrkv1alpha1.InvocationPhaseDispatched)
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
//
// The synthesized root is random, not derived from the invocation id:
// the two are deliberately separate identity spaces (see the
// invocationctx package doc for why). Seeding it from the invocation
// UUID in this no-inbound-traceparent branch -- so the two ids match in
// dev/CLI/cron -- is a deferred dev-ergonomics option tracked in
// apoxy-cloud//docs/clrk-improvements.md.
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

// canonicalInvocationID returns s when it is a canonical RFC 4122 UUID
// in the lowercase 8-4-4-4-12 hyphenated form -- exactly what
// uuid.NewString mints and a valid Kubernetes object name -- else "".
// uuid.Parse accepts several lenient encodings (urn:uuid:, braces,
// uppercase, raw hex); requiring u.String() == s rejects all of them so
// a caller-supplied id is only honored when it is safe to use verbatim
// as the Invocation's metadata.name.
func canonicalInvocationID(s string) string {
	if s == "" {
		return ""
	}
	u, err := uuid.Parse(s)
	if err != nil || u.String() != s {
		return ""
	}
	return s
}

// continueUnchanged returns a CONTINUE response whose type matches req's
// processing phase, so a phase the ingress filter doesn't act on is always
// answered with a correctly-typed reply -- a phase-mismatched response (e.g.
// a RequestHeaders-typed reply to a ResponseBody) would reset the stream.
// Returns nil for an unknown oneof (a future Envoy version), in which case
// the caller sends nothing. Under the ingress ProcessingMode only
// RequestHeaders/RequestBody are sent (both handled explicitly in Process);
// this covers any phase a future policy change might enable.
func continueUnchanged(req *extprocv3.ProcessingRequest) *extprocv3.ProcessingResponse {
	cont := &extprocv3.CommonResponse{Status: extprocv3.CommonResponse_CONTINUE}
	switch req.Request.(type) {
	case *extprocv3.ProcessingRequest_RequestHeaders:
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestHeaders{
			RequestHeaders: &extprocv3.HeadersResponse{Response: cont}}}
	case *extprocv3.ProcessingRequest_ResponseHeaders:
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseHeaders{
			ResponseHeaders: &extprocv3.HeadersResponse{Response: cont}}}
	case *extprocv3.ProcessingRequest_RequestBody:
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestBody{
			RequestBody: &extprocv3.BodyResponse{Response: cont}}}
	case *extprocv3.ProcessingRequest_ResponseBody:
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseBody{
			ResponseBody: &extprocv3.BodyResponse{Response: cont}}}
	case *extprocv3.ProcessingRequest_RequestTrailers:
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestTrailers{
			RequestTrailers: &extprocv3.TrailersResponse{}}}
	case *extprocv3.ProcessingRequest_ResponseTrailers:
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseTrailers{
			ResponseTrailers: &extprocv3.TrailersResponse{}}}
	default:
		return nil
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

// clientAddress returns the originating client IP: the first hop of
// X-Forwarded-For (Envoy appends the immediate peer to the right), else "".
func clientAddress(hdrs map[string]string) string {
	xff := hdrs["x-forwarded-for"]
	if xff == "" {
		return ""
	}
	if i := strings.IndexByte(xff, ','); i >= 0 {
		return strings.TrimSpace(xff[:i])
	}
	return strings.TrimSpace(xff)
}

// contentLength parses a Content-Length header into a non-negative size;
// ok is false when the header is absent or malformed.
func contentLength(v string) (int, bool) {
	if v == "" {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// ingressIncludedContentTypes is the content-type allow-list gating
// request-body capture, matched case-insensitively by prefix. It is the
// shared egress default set (capture.DefaultIncludedContentTypes) plus
// application/cloudevents+json -- the structured-mode CloudEvents trigger
// shape ingress accepts. Building it from the shared default keeps the base
// set from drifting; a trigger whose content-type isn't listed (e.g. a
// binary upload) is not captured, so the span never carries opaque bytes.
// The byte cap is capture.MaxBytesDefault (no per-resource CaptureBody
// override exists on the ingress path; adding one is a separate CRD change).
var ingressIncludedContentTypes = append(
	[]string{"application/cloudevents+json"}, capture.DefaultIncludedContentTypes...)

// emitRequestHeadersEvent stamps the inbound request headers onto the
// dispatch span as an http.request.headers event -- one
// http.request.header.<name> attribute per header -- mirroring the egress
// sink so the console renders the root span's request headers. Sensitive
// headers (authorization, x-api-key, cookie, ...) are redacted via the same
// tracectx.IsSensitiveHeader set the egress path uses, so an inbound
// credential never reaches OTLP raw.
func emitRequestHeadersEvent(span trace.Span, hdrs map[string]string) {
	if len(hdrs) == 0 {
		return
	}
	out := make([]attribute.KeyValue, 0, len(hdrs))
	for k, v := range hdrs {
		if tracectx.IsSensitiveHeader(k) {
			v = "[redacted]"
		}
		out = append(out, attribute.String("http.request.header."+k, v))
	}
	span.AddEvent(otelemit.SpanEventHTTPRequestHeaders, trace.WithAttributes(out...))
}

// emitRequestBodyEvent stamps the captured request body onto the dispatch
// span as an http.request.body event, using the same clrk.body.* contract
// the egress sink emits (otelemit.BodyEventAttrs) so the console renders it
// with no front-end change. The body is content-decoded for the event when
// it carried a non-identity, inflatable encoding -- so clrk.body.b64 is the
// readable payload. Decode is skipped on a truncated capture: a body cut off
// at the byte cap is an incomplete compressed stream that can't be reliably
// inflated, so it ships raw, flagged by clrk.body.truncated and the
// content_encoding breadcrumb (the egress contract). The caller stamps `at`
// at first-chunk arrival and only calls this with a non-empty body, so the
// timestamp is always set.
func emitRequestBodyEvent(span trace.Span, body []byte, truncated bool, contentEncoding string, at time.Time) {
	if len(body) == 0 {
		return
	}
	enc := llmcall.NormalizeContentEncoding(contentEncoding)
	out := body
	if !truncated && enc != "" {
		if dec, ok := llmcall.DecodeBody(body, contentEncoding); ok {
			out = dec
		}
	}
	span.AddEvent(otelemit.SpanEventHTTPRequestBody,
		trace.WithTimestamp(at),
		trace.WithAttributes(otelemit.BodyEventAttrs(out, truncated, enc)...))
}

// bodyContinue is the RequestBody-phase CONTINUE reply: forward the chunk
// unchanged. The ingress filter only observes the body; it never mutates it.
func bodyContinue() *extprocv3.ProcessingResponse {
	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_RequestBody{
			RequestBody: &extprocv3.BodyResponse{
				Response: &extprocv3.CommonResponse{Status: extprocv3.CommonResponse_CONTINUE},
			},
		},
	}
}
