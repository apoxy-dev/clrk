// Package extproc implements envoy.service.ext_proc.v3.ExternalProcessor.
// Streams through an Envoy ext_proc filter deliver HTTP headers + body
// chunks (both directions) to this server; we buffer them per stream,
// emit a structured log on stream close, and hand back continue responses
// that leave traffic untouched.
//
// The EG extension (see internal/egextension) wires Envoy so that MITM
// listener traffic flows through the filter, and so that PROXY v2 TLVs
// (agent identity + invocation ID) appear as dynamic metadata under the
// clrk.apoxy.dev namespace — which we read from MetadataContext below.
package extproc

import (
	"context"
	"errors"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocfilterv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/go-logr/logr"
	"google.golang.org/protobuf/types/known/structpb"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egidentity"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
	"github.com/apoxy-dev/clrk/internal/extproc/invocationctx"
	"github.com/apoxy-dev/clrk/internal/extproc/parsers"
	"github.com/apoxy-dev/clrk/internal/extproc/tracectx"
)

// MetadataNamespace is the dynamic-metadata key carrying clrk PROXY v2 TLV
// values decoded by the proxy_protocol listener filter. Keep in sync with
// internal/egextension when it programs listener-filter rules.
const MetadataNamespace = "clrk.apoxy.dev"

// Metadata sub-keys under MetadataNamespace.
const (
	MetaAgentKind      = "agent_kind"
	MetaAgentNamespace = "agent_namespace"
	MetaAgentName      = "agent_name"
	MetaAgentUID       = "agent_uid"
	MetaAgentRevision  = "agent_revision"
	MetaInvocationID   = "invocation_id"

	// MetaDstName carries the DNS-bound destination hostname for the
	// connection — the name the sandbox's resolver answered for the
	// dst IP. Worker emits it via PROXY v2 TLVDstName; the listener
	// filter publishes it under this key. Empty when no binding
	// existed at dial time (direct-IP, DNS bypass, expired entry).
	MetaDstName = "dst_name"
)

// captureMaxBytesDefault bounds buffered request+response body bytes per
// stream when the resolved EgressGateway didn't pin a specific cap.
const captureMaxBytesDefault = 64 * 1024

// Sink receives one captured record per HTTP transaction or L4
// connection. Implementations might push to OTLP, Axiom, stdout, etc.
// Errors are logged and dropped.
type Sink interface {
	Emit(record Record)
	EmitL4(record L4Record)
}

// Record is the structured representation of one HTTP request/response
// observed through ext_proc. All fields are best-effort; Envoy may not
// deliver bodies or trailers depending on mode.
//
// Timestamps mark the wall-clock arrival of each ext_proc callback so
// the trace sink can synthesize span events with realistic timings.
// They are zero when the corresponding phase did not occur (e.g. an
// empty-body request leaves RequestBodyAt zero).
type Record struct {
	Timestamp time.Time
	EndAt     time.Time

	AgentKind      string
	AgentNamespace string
	AgentName      string
	AgentUID       string
	AgentRevision  string
	InvocationID   string

	RequestHeadersAt time.Time
	RequestHeaders   map[string]string
	RequestBodyAt    time.Time
	RequestBody      []byte
	RequestTruncated bool

	ResponseHeadersAt time.Time
	ResponseHeaders   map[string]string
	ResponseBodyAt    time.Time
	ResponseBody      []byte
	ResponseTruncated bool
	// ResponseBodyChunks counts ResponseBody ProcessingRequest messages
	// received from Envoy. Under ResponseBodyMode=BUFFERED this is 1
	// (or 0 if the upstream produced no body); under STREAMED it
	// reflects per-chunk delivery. Surfaced on OTLP so operators (and
	// integration tests) can confirm the conditional STREAMED override
	// promoted the mode for streaming traffic.
	ResponseBodyChunks int

	// RequestBodyRewritten is true when ext_proc replaced the request
	// body before forwarding (e.g. forcing OpenAI
	// stream_options.include_usage=true so streamed responses always
	// emit terminal usage). Surfaced on the OTLP record as
	// clrk.body.request_rewritten so operators can correlate
	// upstream-observed bodies with what the agent actually sent.
	RequestBodyRewritten bool

	// Provider holds parsed AI-provider facts (gen_ai.* shape) when the
	// request's :authority host matched a known provider. Nil for any
	// other host.
	Provider *parsers.ProviderInfo

	// MatchedRouteNamespace / MatchedRouteName identify the
	// AIProviderRoute that accepted this transaction; empty when no
	// route attached to the calling EG matched.
	MatchedRouteNamespace string
	MatchedRouteName      string

	// BudgetDenied is set when the matched route's TokenBudget caused
	// us to short-circuit the request with an HTTP 429. The captured
	// record will only have RequestHeaders populated in that case
	// (response phases never ran).
	BudgetDenied bool
	// BudgetDailyUsed / BudgetDailyMax are the counter snapshot at the
	// moment of the deny decision, attached for operator visibility.
	BudgetDailyUsed int64
	BudgetDailyMax  int64

	// MCP carries parsed MCP JSON-RPC envelope facts when the
	// :authority host fell under an attached MCPRoute and the request
	// body was a single (non-batch) JSON-RPC request. Nil for any
	// other traffic.
	MCP *parsers.MCPInfo

	// MatchedMCPRouteNamespace / MatchedMCPRouteName identify the
	// MCPRoute whose rule accepted this transaction; empty when no
	// rule matched.
	MatchedMCPRouteNamespace string
	MatchedMCPRouteName      string

	// MCPToolPolicyDecision is "allow" or "deny" when the matched
	// rule's ToolPolicy fired on a tools/call request; empty for
	// non-tools/call methods, no-policy rules, or unmatched traffic.
	MCPToolPolicyDecision string

	// SelectedBackendNamespace / SelectedBackendName identify the clrk
	// Backend this transaction was re-pointed to at RequestBody EOS.
	// SelectedBackendSchema is that backend's canonical gen_ai.system
	// (the wire schema the response was parsed against).
	// SelectedBackendReselected is true only when re-selection actually
	// fired. All four stay zero for single-backend / non-reselectable
	// routes, so OTLP attributes are unchanged for existing traffic.
	SelectedBackendNamespace  string
	SelectedBackendName       string
	SelectedBackendSchema     string
	SelectedBackendReselected bool
}

// ServerOption configures the ext_proc Server.
type ServerOption func(*Server)

// WithSinkOverride forces every stream to use the given sink, bypassing
// per-EgressGateway lookup. Intended for tests; production wires the
// controller-runtime client and resolves sinks per-EG.
func WithSinkOverride(sink Sink) ServerOption {
	return func(s *Server) { s.sinkOverride = sink }
}

// WithInvocationContext threads the controller-manager-local store
// that carries an invocation's W3C trace parent context from ingress
// to egress. The egress filter consults it on every stream's
// RequestHeaders phase to (a) parent the egress span on the inbound
// trace and (b) inject `traceparent` on the outbound request when
// the agent didn't set one — see the package doc on
// internal/extproc/invocationctx.
func WithInvocationContext(store *invocationctx.Store) ServerOption {
	return func(s *Server) { s.invocations = store }
}

// Server implements ExternalProcessorServer.
type Server struct {
	extprocv3.UnimplementedExternalProcessorServer

	client       client.Client
	sinkOverride Sink

	registry    *sinkRegistry
	budget      *budgetStore
	invocations *invocationctx.Store
}

// New constructs an ext_proc server. The client is used to look up
// EgressGateway state per-stream (OTLP endpoint + body capture bounds);
// pass cm.GetClient() in the controller-manager. Tests may pass nil and
// use WithSinkOverride.
func New(c client.Client, opts ...ServerOption) *Server {
	s := &Server{
		client:   c,
		registry: newSinkRegistry(c),
		budget:   newBudgetStore(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Stop releases any resources held by per-EgressGateway sinks (in
// particular, OTLP exporter background workers and pending batches).
// Safe to call multiple times.
func (s *Server) Stop(ctx context.Context) {
	if s.registry != nil {
		s.registry.shutdownAll(ctx)
	}
}

// Process handles one ext_proc stream. One stream corresponds to one HTTP
// transaction (request + response pair, assuming upstream keep-alives).
func (s *Server) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	ctx := stream.Context()
	logger := ctrllog.FromContext(ctx).WithName("extproc")

	rec := Record{Timestamp: time.Now()}

	// Resolve per-stream sink + capture bounds once. We don't redo this
	// per message — config changes between request and response of a
	// single transaction would split the record, which is worse than
	// using the start-of-stream config.
	//
	// Sink emission and registry-driven config (capture bounds, route
	// table, EG identity) are independent: a test can use
	// WithSinkOverride to capture emitted records while still letting
	// the registry resolve routes + budget against a fake client. When
	// the registry is unavailable (no client wired, EG missing), we
	// fall back to slogSink for emission and skip route/budget logic.
	var (
		sink            Sink = s.sinkOverride
		maxCaptureBytes int  = captureMaxBytesDefault
		includedTypes   []string
		routes          *routeTable
		mcpRoutes       *mcpRouteTable
		creds           *credTable
		egKey           types.NamespacedName
	)
	if s.client != nil {
		es, err := s.registry.get(ctx)
		if err != nil {
			if sink == nil {
				logger.V(1).Info("Falling back to slog sink", "reason", err.Error())
			}
		} else {
			if sink == nil {
				sink = es.sink
			}
			maxCaptureBytes = es.maxCaptureBytes
			includedTypes = es.includedTypes
			routes = es.routes
			mcpRoutes = es.mcpRoutes
			creds = es.creds
			if k, kerr := egidentity.MustFromContext(ctx); kerr == nil {
				egKey = k
			}
		}
	}
	if sink == nil {
		sink = slogSink{}
	}

	reqBytesLeft := maxCaptureBytes
	respBytesLeft := maxCaptureBytes
	// respKeepLast switches the response body capture from
	// keep-first-N (appendBounded) to keep-last-N (appendRing) once we
	// see a streamed content-type on ResponseHeaders. Streamed
	// providers report cumulative token usage in the terminal event,
	// so first-N truncation drops exactly the data we need.
	respKeepLast := false
	// streamRequested latches when the request body advertises
	// "stream": true. The filter runs with ResponseBodyMode=BUFFERED
	// by default (one ProcessingRequest covers the whole non-streaming
	// response) and we promote to STREAMED via ModeOverride on the
	// ResponseHeaders reply only when the client asked for streaming
	// AND the upstream returned 200. BUFFERED_PARTIAL request bodies
	// may arrive in multiple chunks; the OR-set below latches on the
	// first chunk that carries the probe.
	streamRequested := false

	// deferReselect latches when the header-time provisional match
	// resolves to a rule that declares clrk BackendRefs. For those
	// streams, credential injection and the final :authority repoint are
	// deferred to RequestBody EOS (after the model is known and a backend
	// is chosen), and the request body mode is promoted to BUFFERED so
	// the body-phase header mutation is honored — BUFFERED_PARTIAL
	// silently drops it (see CommonResponse.HeaderMutation). provMatch is
	// the model-blind header-time match used for the credential fallback
	// when re-selection can't run (truncated body, no candidate).
	deferReselect := false
	var provMatch *routeRule
	// finalRule is the rule that accepted this transaction at RequestBody
	// EOS (model-aware match). It is threaded through to enrichRecord and
	// budget charging instead of re-matching at emit time: a ModelRewrite
	// (and, with cross-schema translation, a schema change) alters the
	// captured body, so an emit-time re-match against the rewritten
	// system/model can miss the rule that actually routed the request —
	// losing route attribution and silently skipping the TokenBudget
	// charge.
	var finalRule *routeRule

	for {
		req, err := stream.Recv()
		if err != nil {
			rec.EndAt = time.Now()
			if errors.Is(err, io.EOF) {
				matched := enrichRecord(&rec, routes, finalRule)
				s.chargeBudget(matched, egKey, rec)
				sink.Emit(rec)
				return nil
			}
			// Client cancelled or transport error. Still emit whatever we
			// captured before the break.
			matched := enrichRecord(&rec, routes, finalRule)
			s.chargeBudget(matched, egKey, rec)
			sink.Emit(rec)
			return err
		}

		// Agent identity is attached to every message under the clrk
		// metadata namespace. It's idempotent to re-read — same values
		// every message — so we just overwrite each time.
		if mctx := req.GetMetadataContext(); mctx != nil {
			applyClrkMetadata(&rec, mctx.GetFilterMetadata())
		}

		now := time.Now()
		var resp *extprocv3.ProcessingResponse
		switch m := req.Request.(type) {
		case *extprocv3.ProcessingRequest_RequestHeaders:
			rec.RequestHeadersAt = now
			rec.RequestHeaders = headersToMap(m.RequestHeaders)
			// Pre-flight TokenBudget check: if the matched route's
			// daily counter is already over cap, return ImmediateResponse
			// 429 instead of letting the request reach the upstream. The
			// match here uses model="" because we haven't buffered the
			// body yet — model-scoped rules can't enforce pre-flight by
			// design (see routeTable.match).
			if denied, used, max := s.evaluateBudget(routes, egKey, rec.RequestHeaders); denied != "" {
				rec.BudgetDenied = true
				rec.BudgetDailyUsed = used
				rec.BudgetDailyMax = max
				rec.MatchedRouteName = denied
				if err := stream.Send(immediateResponse(typev3.StatusCode_TooManyRequests, "clrk: token budget exceeded for route "+denied)); err != nil {
					rec.EndAt = time.Now()
					enrichRecord(&rec, routes, finalRule)
					sink.Emit(rec)
					return err
				}
				// Don't break — keep the stream up so Envoy can drain;
				// the next Recv() will return EOF and we emit the record
				// from the EOF branch above.
				continue
			}
			resp = headersContinue(true)
			// Force port 443 onto :authority when none was supplied. The
			// dynamic_forward_proxy filter parses host:port off the
			// authority and defaults to :80, so HTTPS requests from the
			// sandbox (which omit the implicit :443) get plaintext-
			// dialed to 80 and rejected by hosts behind Cloudflare.
			// Done here, before the dfp HTTP filter, so dfp parses the
			// rewritten value. Filter order is ext_proc → dfp → router.
			mut := authorityPortMutation(rec.RequestHeaders[":authority"])
			// Inject the inbound W3C trace parent on the outbound
			// request when the agent didn't set one. The lookup is
			// keyed by invocation.id (carried via PROXY v2 TLV →
			// dynamic metadata → rec.InvocationID). Honors any
			// agent-set traceparent so deliberately instrumented
			// agents continue to drive their own context.
			mut = applyTraceparentInjection(mut, s.invocations, rec.InvocationID, rec.RequestHeaders)
			// Decide whether this transaction routes through a
			// re-selectable rule (a model-blind match that declares clrk
			// BackendRefs). If so, defer credential injection + the final
			// :authority repoint to RequestBody EOS — after the model is
			// known and a backend is chosen — and promote the request
			// body mode to BUFFERED so the body-phase header mutation
			// takes effect. Otherwise inject credentials now, exactly as
			// before: API keys never live inside the agent, the proxy
			// adds them on the way out (match uses model="" because the
			// body isn't buffered yet — same constraint as the budget
			// pre-flight).
			provMatch = provisionalMatch(routes, rec.RequestHeaders)
			deferReselect = provMatch != nil && len(provMatch.backends) > 0
			if deferReselect {
				resp.ModeOverride = &extprocfilterv3.ProcessingMode{
					RequestBodyMode: extprocfilterv3.ProcessingMode_BUFFERED,
				}
			} else if injs := lookupCreds(creds, routes, rec.RequestHeaders); len(injs) > 0 {
				if collisions := redactInjected(rec.RequestHeaders, injs); len(collisions) > 0 {
					logger.Info("Agent supplied a header that policy is configured to inject; proxy overwriting",
						"policies", collisions)
				}
				mut = applyInjections(mut, injs)
			}
			if mut != nil {
				resp.GetRequestHeaders().GetResponse().HeaderMutation = mut
			}
			// Apply content-type body gate per-direction. If the request
			// content-type isn't in the included set, drop request body
			// capture (set bytesLeft to 0). Headers stay either way.
			if !contentTypeIncluded(rec.RequestHeaders["content-type"], includedTypes) {
				reqBytesLeft = 0
			}
		case *extprocv3.ProcessingRequest_ResponseHeaders:
			rec.ResponseHeadersAt = now
			rec.ResponseHeaders = headersToMap(m.ResponseHeaders)
			resp = headersContinue(false)
			// Promote response-body mode to STREAMED when the response
			// will actually stream AND the upstream returned a 200. Two
			// triggers: the client requested streaming (request body
			// "stream":true — OpenAI/Anthropic), OR the upstream returned
			// a streamed content-type. The latter catches MCP "Streamable
			// HTTP" SSE, which is negotiated via Accept: text/event-stream
			// and never sets a request-body stream flag — without it that
			// traffic would buffer the whole SSE response before releasing
			// it downstream. Error responses stay BUFFERED so the capture
			// path gets the whole error body in one ProcessingRequest.
			respIsStreaming := isStreamingContentType(rec.ResponseHeaders["content-type"])
			if (streamRequested || respIsStreaming) && rec.ResponseHeaders[":status"] == "200" {
				resp.ModeOverride = &extprocfilterv3.ProcessingMode{
					ResponseBodyMode: extprocfilterv3.ProcessingMode_STREAMED,
				}
			}
			if !contentTypeIncluded(rec.ResponseHeaders["content-type"], includedTypes) {
				respBytesLeft = 0
			}
			respKeepLast = respIsStreaming
		case *extprocv3.ProcessingRequest_RequestBody:
			chunk := m.RequestBody.GetBody()
			body, trunc := appendBounded(rec.RequestBody, chunk, &reqBytesLeft)
			if rec.RequestBodyAt.IsZero() {
				rec.RequestBodyAt = now
			}
			rec.RequestBody = body
			rec.RequestTruncated = rec.RequestTruncated || trunc
			// Probe each chunk so multi-chunk BUFFERED_PARTIAL deliveries
			// latch as soon as "stream":true appears. Once true, we leave
			// it.
			if !streamRequested {
				streamRequested = parsers.BodyAdvertisesStream(chunk)
			}
			// On the terminal body chunk, run the deferred backend
			// re-selection (reselectable streams) or the existing
			// include-usage rewrite + MCP enforcement (everything else).
			// BUFFERED(_PARTIAL) delivers the whole body in one
			// ProcessingRequest under the buffer limit; multi-chunk only
			// happens when the body exceeds it.
			if m.RequestBody.GetEndOfStream() {
				host, _ := splitHostPort(rec.RequestHeaders[":authority"])
				reqPath := rec.RequestHeaders[":path"]

				if deferReselect {
					// A truncated body can't be decoded for the model or
					// safely re-serialized: fall back to the provisional
					// match — inject its route-wide credentials so the
					// request still authenticates, leave :authority on the
					// header-time host, do not re-select.
					if rec.RequestTruncated {
						if mut := injectFallbackCreds(creds, provMatch, rec.RequestHeaders, logger); mut != nil {
							resp = bodyContinueWithHeaders(true, mut, false)
						} else {
							resp = bodyContinue(true)
						}
						break
					}

					system := parsers.SystemFor(host)
					model := parsers.RequestModel(rec.RequestBody)
					final := routes.match(system, reqPath, model)
					if final == nil {
						final = provMatch
					}
					// Pin the accepting rule now, while the body still
					// carries the agent's original system/model — emit-time
					// re-matching would run against the rewritten body.
					finalRule = final
					// Weighted/first pick over the rule's candidate
					// backendRefs (BackendRef.Weight). The candidate set was
					// already restricted to the rule provider's wire schema
					// at route-table build time (sameSchemaBackends), so this
					// cannot pick a cross-schema backend and send it a
					// mismatched body — same-schema reselection only;
					// cross-schema needs translation (APO-572). A
					// classifier-driven ExtensionRef selector is APO-480 and
					// will swap a different backendSelector in here.
					chosen, ok := staticSelector{}.Select(final.backends, selectorInput{
						provider:     system,
						model:        model,
						path:         reqPath,
						invocationID: rec.InvocationID,
						reqHeaders:   rec.RequestHeaders,
						reqBody:      rec.RequestBody,
					})
					if !ok {
						// No candidate resolved (e.g. all refs dangling).
						// Fall back to route-wide creds + original host.
						if mut := injectFallbackCreds(creds, final, rec.RequestHeaders, logger); mut != nil {
							resp = bodyContinueWithHeaders(true, mut, false)
						} else {
							resp = bodyContinue(true)
						}
						break
					}
					if chosen.refuse {
						// Resolved but unservable (InferencePool). Fail
						// cleanly rather than mis-route to the agent's
						// original SaaS host.
						if err := stream.Send(immediateResponse(typev3.StatusCode_NotImplemented, "clrk: "+chosen.unsupportedReason)); err != nil {
							rec.EndAt = time.Now()
							enrichRecord(&rec, routes, finalRule)
							sink.Emit(rec)
							return err
						}
						// Keep the stream up; EOF emits the record.
						continue
					}

					// Repoint :authority to the selected backend and inject
					// its credentials (route-wide + the CIP whose parentRef
					// sectionName names this backend + gateway), in the
					// backend's scheme.
					mut := setHeaderMut(nil, ":authority", net.JoinHostPort(chosen.host, strconv.Itoa(chosen.port)))
					if injs := creds.lookupForBackend(final, chosen.name); len(injs) > 0 {
						if collisions := redactInjected(rec.RequestHeaders, injs); len(collisions) > 0 {
							logger.Info("Agent supplied a header that policy is configured to inject; proxy overwriting",
								"policies", collisions)
						}
						mut = applyInjections(mut, injs)
					}
					rec.SelectedBackendNamespace = chosen.namespace
					rec.SelectedBackendName = chosen.name
					rec.SelectedBackendSchema = chosen.system
					rec.SelectedBackendReselected = true

					// Apply per-backend body rewrites (model remap +
					// include-usage in the backend's schema) and emit the
					// repoint with ClearRouteCache so DFP re-derives the
					// upstream host + SNI from the new :authority.
					if newBody, changed := rewriteRequestBody(rec.RequestBody, reqPath, model, chosen); changed {
						rec.RequestBody = newBody
						rec.RequestBodyRewritten = true
						resp = bodyMutationWithHeaders(true, newBody, mut, true)
					} else {
						resp = bodyContinueWithHeaders(true, mut, true)
					}
					break
				}

				// Non-reselectable stream: today's path, unchanged. Skip
				// when truncation already kicked in — partial JSON can't
				// be safely re-serialized or policy-gated.
				if !rec.RequestTruncated {
					if newBody, mut := parsers.EnsureIncludeUsage(host, reqPath, rec.RequestBody); mut {
						// Update the captured copy so OTLP shows what the
						// upstream actually received, not what the agent
						// originally sent.
						rec.RequestBody = newBody
						rec.RequestBodyRewritten = true
						resp = bodyMutation(true, newBody)
						break
					}
					// MCP enforcement. Cheap host + content-type gate so
					// non-MCP traffic pays nothing for the JSON-RPC parse.
					// The include-usage rewrite (above) and an MCP deny
					// are mutually exclusive — the former only fires for
					// OpenAI-shaped requests, the latter only when a host
					// falls under an MCPRoute.
					if mcpCandidate(mcpRoutes, host, rec.RequestHeaders["content-type"]) {
						res := mcpRoutes.evaluate(host, parsers.Input{
							ReqBody:      rec.RequestBody,
							ReqTruncated: rec.RequestTruncated,
							ReqHeaders:   rec.RequestHeaders,
						})
						stampMCPResult(&rec, res)
						if res.decision == mcpDecisionDeny {
							if err := stream.Send(immediateResponse(typev3.StatusCode_Forbidden, res.denyDetail)); err != nil {
								rec.EndAt = time.Now()
								enrichRecord(&rec, routes, finalRule)
								sink.Emit(rec)
								return err
							}
							// Keep the stream up; EOF emits the record.
							continue
						}
					}
				}
			}
			resp = bodyContinue(true)
		case *extprocv3.ProcessingRequest_ResponseBody:
			rec.ResponseBodyChunks++
			var (
				body  []byte
				trunc bool
			)
			if respKeepLast {
				body, trunc = appendRing(rec.ResponseBody, m.ResponseBody.GetBody(), maxCaptureBytes)
			} else {
				body, trunc = appendBounded(rec.ResponseBody, m.ResponseBody.GetBody(), &respBytesLeft)
			}
			if rec.ResponseBodyAt.IsZero() {
				rec.ResponseBodyAt = now
			}
			rec.ResponseBody = body
			rec.ResponseTruncated = rec.ResponseTruncated || trunc
			resp = bodyContinue(false)
		case *extprocv3.ProcessingRequest_RequestTrailers:
			resp = trailersContinue(true)
		case *extprocv3.ProcessingRequest_ResponseTrailers:
			resp = trailersContinue(false)
		default:
			// Unknown oneof branch (e.g. new Envoy version). Skip.
			continue
		}

		if resp == nil {
			continue
		}
		if err := stream.Send(resp); err != nil {
			rec.EndAt = time.Now()
			matched := enrichRecord(&rec, routes, finalRule)
			s.chargeBudget(matched, egKey, rec)
			sink.Emit(rec)
			return err
		}
	}
}

// enrichRecord runs the provider parser on the captured headers/bodies
// and identifies the AIProviderRoute that accepted the transaction.
// Returns the matched routeRule (for downstream budget charging) or
// nil. Both outcomes are no-ops when the request didn't reach a known
// provider host or when no APR is attached to the calling EG.
//
// matched, when non-nil, is the rule pinned at RequestBody EOS and is
// trusted over a fresh routes.match: per-backend rewrites mutate the
// captured body (model remap today, schema translation with APO-742),
// so re-matching against the rewritten system/model here can miss the
// rule that actually routed the request — dropping route attribution
// and the TokenBudget charge.
//
// Called once per stream, just before sink.Emit, so partial-capture
// records (truncated bodies) still attempt parsing.
func enrichRecord(rec *Record, routes *routeTable, matched *routeRule) *routeRule {
	// Augment the MCP record with response-envelope facts (the JSON-RPC
	// error code) now that the response body is buffered. rec.MCP is set
	// only when the request parsed as a single JSON-RPC call under an
	// attached MCPRoute; deny and budget short-circuits leave no response
	// body, so ParseResponse returns nil and this is a no-op there.
	if rec.MCP != nil {
		if res := parsers.ParseResponse(parsers.Input{
			RespBody:      rec.ResponseBody,
			RespTruncated: rec.ResponseTruncated,
			RespHeaders:   rec.ResponseHeaders,
		}); res != nil && res.IsError {
			rec.MCP.IsError = true
			rec.MCP.ErrorCode = res.ErrorCode
		}
	}

	// Key the parser to the SELECTED backend's wire schema when
	// re-selection fired — the original :authority host is no longer
	// where the request went, so parsers.For(host) would parse the
	// response against the wrong schema (zeroing usage and silently
	// evading budget). Otherwise key on the original host as before.
	host, _ := splitHostPort(rec.RequestHeaders[":authority"])
	var parser parsers.Parser
	if rec.SelectedBackendSchema != "" {
		parser = parsers.ForSchema(rec.SelectedBackendSchema)
	} else {
		parser = parsers.For(host)
	}
	if parser != nil {
		rec.Provider = parser.Parse(parsers.Input{
			Method:        rec.RequestHeaders[":method"],
			Path:          rec.RequestHeaders[":path"],
			ReqHeaders:    rec.RequestHeaders,
			RespHeaders:   rec.ResponseHeaders,
			ReqBody:       rec.RequestBody,
			ReqTruncated:  rec.RequestTruncated,
			RespBody:      rec.ResponseBody,
			RespTruncated: rec.ResponseTruncated,
		})
	}
	if matched != nil {
		rec.MatchedRouteNamespace = matched.routeNamespace
		rec.MatchedRouteName = matched.routeName
		return matched
	}
	if routes == nil || rec.Provider == nil {
		return nil
	}
	rr := routes.match(rec.Provider.System, rec.RequestHeaders[":path"], rec.Provider.RequestModel)
	if rr == nil {
		return nil
	}
	rec.MatchedRouteNamespace = rr.routeNamespace
	rec.MatchedRouteName = rr.routeName
	return rr
}

// lookupCreds matches the request to an APR (if any) and returns the
// credentials that should be injected. Pure read against pre-resolved
// tables; called from the RequestHeaders branch where we do header
// mutation. Returns an empty slice when no CIPs attach to this
// transaction.
func lookupCreds(creds *credTable, routes *routeTable, headers map[string]string) []credInjection {
	if creds == nil {
		return nil
	}
	var rr *routeRule
	if routes != nil {
		host, _ := splitHostPort(headers[":authority"])
		if system := parsers.SystemFor(host); system != "" {
			rr = routes.match(system, headers[":path"], "")
		}
	}
	return creds.lookup(rr)
}

// provisionalMatch is the model-blind header-time match used to decide
// whether a stream is re-selectable (its rule declares clrk BackendRefs)
// and, on the fallback paths, which route's credentials apply. It mirrors
// the pre-flight semantics (model="") but, unlike lookupCreds, does not
// gate on a known provider host so a custom-provider rule (which matches
// any host) can still drive re-selection.
func provisionalMatch(routes *routeTable, headers map[string]string) *routeRule {
	if routes == nil {
		return nil
	}
	host, _ := splitHostPort(headers[":authority"])
	return routes.match(parsers.SystemFor(host), headers[":path"], "")
}

// injectFallbackCreds builds a header mutation injecting rr's route-wide
// (and gateway) credentials, redacting them from the captured record.
// Used on the re-selection fallback paths (truncated body, no resolvable
// candidate) so a deferred-credential request still authenticates against
// its original host. Returns nil when there is nothing to inject.
func injectFallbackCreds(creds *credTable, rr *routeRule, headers map[string]string, logger logr.Logger) *extprocv3.HeaderMutation {
	injs := creds.lookup(rr)
	if len(injs) == 0 {
		return nil
	}
	if collisions := redactInjected(headers, injs); len(collisions) > 0 {
		logger.Info("Agent supplied a header that policy is configured to inject; proxy overwriting",
			"policies", collisions)
	}
	return applyInjections(nil, injs)
}

// rewriteRequestBody applies a selected backend's per-backend request
// rewrites at RequestBody EOS: a model remap (first matching
// ModelRewrite) then include-usage keyed to the backend's wire schema.
// The backend's BodyMutation.EnsureStreamUsage gates the include-usage
// rewrite when set; when unset the schema heuristic decides (preserving
// today's behavior). Returns the possibly-rewritten body and whether
// anything changed.
func rewriteRequestBody(body []byte, reqPath, model string, b resolvedBackend) ([]byte, bool) {
	changed := false
	if to, ok := b.rewriteModel(model); ok && to != model {
		if nb, ok2 := parsers.RewriteModel(body, to); ok2 {
			body, changed = nb, true
		}
	}
	applyUsage := true
	if b.bodyMutation != nil && b.bodyMutation.EnsureStreamUsage != nil {
		applyUsage = *b.bodyMutation.EnsureStreamUsage
	}
	if applyUsage {
		if nb, ok := parsers.EnsureIncludeUsageForSystem(b.system, reqPath, body); ok {
			body, changed = nb, true
		}
	}
	return body, changed
}

// evaluateBudget runs the pre-flight TokenBudget check at request-
// headers time. Returns ("", 0, 0) when nothing should block the
// request; otherwise returns the route name plus current daily total
// and cap, and the caller is expected to short-circuit with a 429.
//
// Pre-flight matches with model="" because the request body hasn't
// been buffered yet — model-scoped rules don't enforce here (see
// routeTable.match for the rationale).
func (s *Server) evaluateBudget(routes *routeTable, eg types.NamespacedName, headers map[string]string) (route string, used, max int64) {
	if s.budget == nil || routes == nil || eg.Name == "" {
		return "", 0, 0
	}
	host, _ := splitHostPort(headers[":authority"])
	system := parsers.SystemFor(host)
	if system == "" {
		return "", 0, 0
	}
	rr := routes.match(system, headers[":path"], "")
	if rr == nil || rr.tokenBudget == nil || rr.tokenBudget.MaxTokensPerDay == nil {
		return "", 0, 0
	}
	cap := *rr.tokenBudget.MaxTokensPerDay
	if cap <= 0 {
		return "", 0, 0
	}
	bk := budgetKey{
		egNamespace:    eg.Namespace,
		egName:         eg.Name,
		routeNamespace: rr.routeNamespace,
		routeName:      rr.routeName,
	}
	if s.budget.Allow(bk, cap) {
		return "", 0, 0
	}
	return rr.routeName, s.budget.snapshot(bk), cap
}

// chargeBudget increments the daily counter for the matched route by
// the parsed input+output token total. No-op when the route has no
// TokenBudget, the parser found no usage, or the request was denied
// at pre-flight (in which case nothing reached the upstream).
func (s *Server) chargeBudget(rr *routeRule, eg types.NamespacedName, rec Record) {
	if s.budget == nil || rr == nil || rr.tokenBudget == nil {
		return
	}
	if rec.BudgetDenied || rec.Provider == nil {
		return
	}
	tokens := rec.Provider.InputTokens + rec.Provider.OutputTokens
	if tokens <= 0 {
		return
	}
	s.budget.Add(budgetKey{
		egNamespace:    eg.Namespace,
		egName:         eg.Name,
		routeNamespace: rr.routeNamespace,
		routeName:      rr.routeName,
	}, tokens)
}

// immediateResponse builds an ext_proc ImmediateResponse that
// short-circuits the upstream call with the given HTTP status and a
// small JSON body explaining the deny reason. Envoy synthesizes the
// client response from this; the client sees the status immediately.
func immediateResponse(code typev3.StatusCode, detail string) *extprocv3.ProcessingResponse {
	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_ImmediateResponse{
			ImmediateResponse: &extprocv3.ImmediateResponse{
				Status: &typev3.HttpStatus{Code: code},
				Body:   []byte(`{"error":` + jsonString(detail) + `}`),
				Headers: &extprocv3.HeaderMutation{
					SetHeaders: []*corev3.HeaderValueOption{{
						Header: &corev3.HeaderValue{Key: "content-type", Value: "application/json"},
					}},
				},
			},
		},
	}
}

// jsonString quotes s as a JSON string. Tiny helper so we don't pull
// encoding/json in just to format an error body.
func jsonString(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"', '\\':
			out = append(out, '\\', c)
		case '\n':
			out = append(out, '\\', 'n')
		case '\r':
			out = append(out, '\\', 'r')
		case '\t':
			out = append(out, '\\', 't')
		default:
			if c < 0x20 {
				continue
			}
			out = append(out, c)
		}
	}
	out = append(out, '"')
	return string(out)
}

// splitHostPort returns host without port from an HTTP/2 :authority,
// stripping IPv6 brackets. Distinct from sink_otlp.splitAuthority,
// which returns host+port; we only need the host here.
func splitHostPort(authority string) (string, string) {
	if authority == "" {
		return "", ""
	}
	host, port, err := net.SplitHostPort(authority)
	if err != nil {
		return strings.Trim(authority, "[]"), ""
	}
	return host, port
}

func headersToMap(h *extprocv3.HttpHeaders) map[string]string {
	m := make(map[string]string)
	for _, kv := range h.GetHeaders().GetHeaders() {
		// Envoy may populate either Value (string) or RawValue (bytes).
		v := kv.GetValue()
		if v == "" && len(kv.GetRawValue()) > 0 {
			v = string(kv.GetRawValue())
		}
		m[strings.ToLower(kv.GetKey())] = v
	}
	return m
}

// appendBounded appends src to dst up to *left bytes, truncating the
// remainder. Returns whether truncation occurred.
func appendBounded(dst, src []byte, left *int) ([]byte, bool) {
	if *left <= 0 {
		return dst, len(src) > 0
	}
	if len(src) <= *left {
		*left -= len(src)
		return append(dst, src...), false
	}
	dst = append(dst, src[:*left]...)
	*left = 0
	return dst, true
}

// appendRing appends src to dst and trims from the head when the
// total exceeds capBytes, keeping the last capBytes bytes. trunc is
// true when bytes were dropped (either from this call or any prior
// one — once we trim, every subsequent call inherits trunc=true via
// dst already being at capacity). Used for streaming response bodies
// where the terminal usage event lives at the tail.
func appendRing(dst, src []byte, capBytes int) ([]byte, bool) {
	if capBytes <= 0 {
		return dst, len(src) > 0
	}
	if len(src) >= capBytes {
		// New chunk alone exceeds the cap; keep only its tail.
		out := make([]byte, capBytes)
		copy(out, src[len(src)-capBytes:])
		return out, true
	}
	dst = append(dst, src...)
	if len(dst) > capBytes {
		drop := len(dst) - capBytes
		dst = dst[drop:]
		return dst, true
	}
	return dst, false
}

// isStreamingContentType reports whether ct names a streamed response
// shape that needs keep-last-N capture (terminal usage event lives at
// the tail).
func isStreamingContentType(ct string) bool {
	if ct == "" {
		return false
	}
	ct = strings.ToLower(ct)
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	switch ct {
	case "text/event-stream", "application/x-ndjson":
		return true
	}
	return false
}

func applyClrkMetadata(rec *Record, filterMeta map[string]*structpb.Struct) {
	s, ok := filterMeta[MetadataNamespace]
	if !ok {
		return
	}
	fields := s.GetFields()
	if v := fields[MetaAgentKind]; v != nil {
		rec.AgentKind = decodeAgentKind(v.GetStringValue())
	}
	if v := fields[MetaAgentNamespace]; v != nil {
		rec.AgentNamespace = v.GetStringValue()
	}
	if v := fields[MetaAgentName]; v != nil {
		rec.AgentName = v.GetStringValue()
	}
	if v := fields[MetaAgentUID]; v != nil {
		rec.AgentUID = v.GetStringValue()
	}
	if v := fields[MetaAgentRevision]; v != nil {
		rec.AgentRevision = v.GetStringValue()
	}
	if v := fields[MetaInvocationID]; v != nil {
		rec.InvocationID = v.GetStringValue()
	}
}

// decodeAgentKind translates the raw byte value the proxy_protocol
// listener filter wrote into dynamic metadata back into the human-
// readable kind string OTel consumers expect. The TLV encoding (see
// internal/egress/proxyproto/tlv.go) is one byte: 0 = DaemonAgent,
// 1 = TaskAgent. Without this translation `agent.kind` reaches OTel
// as a NUL or 0x01 byte and downstream attribution silently drops the
// record (the dev TUI's per-agent detail view, ClickHouse views, etc.).
// Pass-through any value that isn't a recognised single byte so a
// future producer that sets the string directly still works.
func decodeAgentKind(raw string) string {
	if len(raw) != 1 {
		return raw
	}
	switch raw[0] {
	case byte(proxyproto.AgentKindDaemon):
		return clrkv1alpha1.AgentKindDaemon
	case byte(proxyproto.AgentKindTask):
		return clrkv1alpha1.AgentKindTask
	}
	return raw
}

// applyTraceparentInjection mutates the outbound request to carry the
// inbound trace parent on the wire when the agent didn't set one.
// The lookup is keyed by invocation.id (carried as a PROXY v2 TLV
// from the sandbox's IdentityDialer), recovered from the
// invocationctx store the ingress filter populates. No-op when:
//
//   - the store isn't wired (tests, init order),
//   - the invocation has no recorded parent (direct-dispatcher path),
//   - the agent already set traceparent (we honor instrumented
//     callers; respect their context).
//
// The captured rec.RequestHeaders map is also updated so OTLP span
// emission later parents off the same context.
func applyTraceparentInjection(existing *extprocv3.HeaderMutation, store *invocationctx.Store, invocationID string, headers map[string]string) *extprocv3.HeaderMutation {
	if store == nil || invocationID == "" {
		return existing
	}
	if _, agentSet := headers[tracectx.HeaderTraceparent]; agentSet {
		return existing
	}
	parent, ok := store.Get(invocationID)
	if !ok || !parent.IsValid() {
		return existing
	}
	traceparent, tracestate := tracectx.Inject(parent)
	if traceparent == "" {
		return existing
	}
	existing = setHeaderMut(existing, tracectx.HeaderTraceparent, traceparent)
	headers[tracectx.HeaderTraceparent] = traceparent
	if tracestate != "" {
		existing = setHeaderMut(existing, tracectx.HeaderTracestate, tracestate)
		headers[tracectx.HeaderTracestate] = tracestate
	}
	return existing
}

// authorityPortRE matches a trailing :NN port suffix on a host. We don't
// need to handle bracketed IPv6 — agents resolve target hostnames, not
// literals — but the regex is anchored so a colon in a userinfo segment
// (which is also forbidden in :authority) doesn't false-match.
var authorityPortRE = regexp.MustCompile(`:[0-9]+$`)

// authorityPortMutation returns a HeaderMutation that rewrites
// :authority to host:443 when no port is set; nil otherwise. See the
// caller for context on why this is necessary.
func authorityPortMutation(authority string) *extprocv3.HeaderMutation {
	if authority == "" || authorityPortRE.MatchString(authority) {
		return nil
	}
	return &extprocv3.HeaderMutation{
		SetHeaders: []*corev3.HeaderValueOption{{
			// RawValue (not Value) — see applyInjections for the rationale.
			Header:       &corev3.HeaderValue{Key: ":authority", RawValue: []byte(authority + ":443")},
			AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
		}},
	}
}

// headersContinue / bodyContinue / trailersContinue build observability-
// mode-safe responses that tell Envoy to forward unchanged.
func headersContinue(isRequest bool) *extprocv3.ProcessingResponse {
	r := &extprocv3.HeadersResponse{Response: &extprocv3.CommonResponse{
		Status: extprocv3.CommonResponse_CONTINUE,
	}}
	if isRequest {
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestHeaders{RequestHeaders: r}}
	}
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseHeaders{ResponseHeaders: r}}
}

func bodyContinue(isRequest bool) *extprocv3.ProcessingResponse {
	r := &extprocv3.BodyResponse{Response: &extprocv3.CommonResponse{
		Status: extprocv3.CommonResponse_CONTINUE,
	}}
	if isRequest {
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestBody{RequestBody: r}}
	}
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseBody{ResponseBody: r}}
}

// bodyMutation returns a CONTINUE_AND_REPLACE body response carrying
// the new body bytes. Envoy strips the original Content-Length and
// recomputes from the mutated body before forwarding upstream.
func bodyMutation(isRequest bool, newBody []byte) *extprocv3.ProcessingResponse {
	r := &extprocv3.BodyResponse{Response: &extprocv3.CommonResponse{
		Status: extprocv3.CommonResponse_CONTINUE_AND_REPLACE,
		BodyMutation: &extprocv3.BodyMutation{
			Mutation: &extprocv3.BodyMutation_Body{Body: newBody},
		},
	}}
	if isRequest {
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestBody{RequestBody: r}}
	}
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseBody{ResponseBody: r}}
}

// bodyContinueWithHeaders is bodyContinue plus a header mutation and an
// optional route-cache clear. Used by the post-body backend re-selection
// path to repoint :authority + inject credentials at RequestBody EOS
// without replacing the body. Per the ext_proc contract a header
// mutation on a body response only takes effect when the request body
// mode is BUFFERED — which the RequestHeaders ModeOverride promotes for
// reselectable streams. clearRouteCache forces Envoy to re-derive the
// upstream (DFP host + SNI) from the mutated :authority.
func bodyContinueWithHeaders(isRequest bool, mut *extprocv3.HeaderMutation, clearRouteCache bool) *extprocv3.ProcessingResponse {
	common := &extprocv3.CommonResponse{
		Status:          extprocv3.CommonResponse_CONTINUE,
		HeaderMutation:  mut,
		ClearRouteCache: clearRouteCache,
	}
	r := &extprocv3.BodyResponse{Response: common}
	if isRequest {
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestBody{RequestBody: r}}
	}
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseBody{ResponseBody: r}}
}

// bodyMutationWithHeaders is bodyMutation (CONTINUE_AND_REPLACE with a
// new body) plus a header mutation + optional route-cache clear, for the
// re-selection path where the request body was also rewritten (e.g.
// include_usage) in the same response that repoints :authority.
func bodyMutationWithHeaders(isRequest bool, newBody []byte, mut *extprocv3.HeaderMutation, clearRouteCache bool) *extprocv3.ProcessingResponse {
	common := &extprocv3.CommonResponse{
		Status: extprocv3.CommonResponse_CONTINUE_AND_REPLACE,
		BodyMutation: &extprocv3.BodyMutation{
			Mutation: &extprocv3.BodyMutation_Body{Body: newBody},
		},
		HeaderMutation:  mut,
		ClearRouteCache: clearRouteCache,
	}
	r := &extprocv3.BodyResponse{Response: common}
	if isRequest {
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestBody{RequestBody: r}}
	}
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseBody{ResponseBody: r}}
}

func trailersContinue(isRequest bool) *extprocv3.ProcessingResponse {
	r := &extprocv3.TrailersResponse{}
	if isRequest {
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestTrailers{RequestTrailers: r}}
	}
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseTrailers{ResponseTrailers: r}}
}
