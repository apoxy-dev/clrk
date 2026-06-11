package extproc

// This file holds the downstream (listener-level) stream handler: the
// per-stream state and phase methods for the agent-side ext_proc filter
// every MITM'd HTTP transaction flows through. Each phase method takes
// the decoded message and returns the ProcessingResponse for the gRPC
// loop in Process to send (nil means "send nothing, keep receiving") —
// the methods never touch the stream, so tests can drive them directly.

import (
	"errors"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocfilterv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"

	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
	"github.com/apoxy-dev/clrk/internal/extproc/parsers"
)

// downstreamStream is the per-stream state of one downstream ext_proc
// stream (one HTTP transaction). Built by Server.Process at stream
// start from the per-EG registry config.
type downstreamStream struct {
	srv    *Server
	logger logr.Logger

	rec  Record
	sink Sink

	maxCaptureBytes int
	includedTypes   []string
	routes          *routeTable
	mcpRoutes       *mcpRouteTable
	creds           *credTable
	egKey           types.NamespacedName

	reqBytesLeft  int
	respBytesLeft int

	// respKeepLast switches the response body capture from
	// keep-first-N (appendBounded) to keep-last-N (appendRing) once we
	// see a streamed content-type on ResponseHeaders. Streamed
	// providers report cumulative token usage in the terminal event,
	// so first-N truncation drops exactly the data we need.
	respKeepLast bool

	// streamRequested latches when the request body advertises
	// "stream": true. The filter runs with ResponseBodyMode=BUFFERED
	// by default (one ProcessingRequest covers the whole non-streaming
	// response) and we promote to STREAMED via ModeOverride on the
	// ResponseHeaders reply only when the client asked for streaming
	// AND the upstream returned 200. BUFFERED_PARTIAL request bodies
	// may arrive in multiple chunks; the OR-set in onRequestBody
	// latches on the first chunk that carries the probe.
	streamRequested bool

	// deferReselect latches when the header-time provisional match
	// resolves to a rule that declares clrk BackendRefs. For those
	// streams, credential injection and the final :authority repoint are
	// deferred to RequestBody EOS (after the model is known and a backend
	// is chosen), and the request body mode is promoted to BUFFERED so
	// the body-phase header mutation is honored — BUFFERED_PARTIAL
	// silently drops it (see CommonResponse.HeaderMutation). provMatch is
	// the model-blind header-time match used for the credential fallback
	// when re-selection can't run (truncated body, no candidate).
	deferReselect bool
	provMatch     *routeRule

	// finalRule is the rule that accepted this transaction at RequestBody
	// EOS (model-aware match). It is threaded through to enrichRecord and
	// budget charging instead of re-matching at emit time: a ModelRewrite
	// (and, with cross-schema translation, a schema change) alters the
	// captured body, so an emit-time re-match against the rewritten
	// system/model can miss the rule that actually routed the request —
	// losing route attribution and silently skipping the TokenBudget
	// charge.
	finalRule *routeRule

	// fullReqBody accumulates the COMPLETE request body for reselectable
	// streams, independent of the telemetry capture cap. Re-selection
	// re-serializes the body (model remap, schema translation), which is
	// only sound from full bytes — the capture cap exists to bound what we
	// EMIT, not what we route on. Bounded by Envoy's per-stream buffer
	// (the BUFFERED mode promotion), so it cannot grow past what Envoy
	// itself was willing to hold.
	fullReqBody []byte

	// xlate is non-nil once a cross-schema-translated request was
	// committed upstream. The response phases key off it: the agent
	// speaks xlate.src's schema, the upstream answers in xlate.tgt's,
	// and the body must come back through the reverse translation.
	// fullRespBody is the response-side full-fidelity buffer, mirroring
	// fullReqBody — reverse translation must run on complete bytes, not
	// the capped capture.
	xlate        *translationState
	fullRespBody []byte
}

// handle dispatches one ProcessingRequest to its phase method and
// returns the response for the gRPC loop to send. nil means "nothing
// to send" (unknown message kinds).
func (ds *downstreamStream) handle(req *extprocv3.ProcessingRequest, now time.Time) *extprocv3.ProcessingResponse {
	switch m := req.Request.(type) {
	case *extprocv3.ProcessingRequest_RequestHeaders:
		return ds.onRequestHeaders(m.RequestHeaders, now)
	case *extprocv3.ProcessingRequest_ResponseHeaders:
		return ds.onResponseHeaders(m.ResponseHeaders, now)
	case *extprocv3.ProcessingRequest_RequestBody:
		return ds.onRequestBody(m.RequestBody, now)
	case *extprocv3.ProcessingRequest_ResponseBody:
		return ds.onResponseBody(m.ResponseBody, now)
	case *extprocv3.ProcessingRequest_RequestTrailers:
		return trailersContinue(true)
	case *extprocv3.ProcessingRequest_ResponseTrailers:
		return trailersContinue(false)
	default:
		// Unknown oneof branch (e.g. new Envoy version). Skip.
		return nil
	}
}

// finish stamps the end time, runs record enrichment + budget charging,
// and emits the record. Called exactly once per stream, when Recv or
// Send breaks the loop.
func (ds *downstreamStream) finish() {
	ds.rec.EndAt = time.Now()
	matched := enrichRecord(&ds.rec, ds.routes, ds.finalRule)
	ds.srv.chargeBudget(matched, ds.egKey, ds.rec)
	ds.sink.Emit(ds.rec)
}

func (ds *downstreamStream) onRequestHeaders(m *extprocv3.HttpHeaders, now time.Time) *extprocv3.ProcessingResponse {
	ds.rec.RequestHeadersAt = now
	ds.rec.RequestHeaders = headersToMap(m)
	// Pre-flight TokenBudget check: if the matched route's
	// daily counter is already over cap, return ImmediateResponse
	// 429 instead of letting the request reach the upstream. The
	// match here uses model="" because we haven't buffered the
	// body yet — model-scoped rules can't enforce pre-flight by
	// design (see routeTable.match).
	if denied, used, max := ds.srv.evaluateBudget(ds.routes, ds.egKey, ds.rec.RequestHeaders); denied != "" {
		ds.rec.BudgetDenied = true
		ds.rec.BudgetDailyUsed = used
		ds.rec.BudgetDailyMax = max
		ds.rec.MatchedRouteName = denied
		return immediateResponse(typev3.StatusCode_TooManyRequests, "clrk: token budget exceeded for route "+denied)
	}
	resp := headersContinue(true)
	// Force port 443 onto :authority when none was supplied. The
	// dynamic_forward_proxy filter parses host:port off the
	// authority and defaults to :80, so HTTPS requests from the
	// sandbox (which omit the implicit :443) get plaintext-
	// dialed to 80 and rejected by hosts behind Cloudflare.
	// Done here, before the dfp HTTP filter, so dfp parses the
	// rewritten value. Filter order is ext_proc → dfp → router.
	mut := authorityPortMutation(ds.rec.RequestHeaders[":authority"])
	// Inject the inbound W3C trace parent on the outbound
	// request when the agent didn't set one. The lookup is
	// keyed by invocation.id (carried via PROXY v2 TLV →
	// dynamic metadata → rec.InvocationID). Honors any
	// agent-set traceparent so deliberately instrumented
	// agents continue to drive their own context.
	mut = applyTraceparentInjection(mut, ds.srv.invocations, ds.rec.InvocationID, ds.rec.RequestHeaders)
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
	ds.provMatch = provisionalMatch(ds.routes, ds.rec.RequestHeaders)
	ds.deferReselect = ds.provMatch != nil && len(ds.provMatch.backends) > 0
	if ds.deferReselect {
		resp.ModeOverride = modeOverride(
			extprocfilterv3.ProcessingMode_BUFFERED,
			extprocfilterv3.ProcessingMode_BUFFERED,
		)
	} else if injs := lookupCreds(ds.creds, ds.routes, ds.rec.RequestHeaders); len(injs) > 0 {
		if collisions := redactInjected(ds.rec.RequestHeaders, injs); len(collisions) > 0 {
			ds.logger.Info("Agent supplied a header that policy is configured to inject; proxy overwriting",
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
	if !contentTypeIncluded(ds.rec.RequestHeaders["content-type"], ds.includedTypes) {
		ds.reqBytesLeft = 0
	}
	return resp
}

func (ds *downstreamStream) onResponseHeaders(m *extprocv3.HttpHeaders, now time.Time) *extprocv3.ProcessingResponse {
	ds.rec.ResponseHeadersAt = now
	ds.rec.ResponseHeaders = headersToMap(m)
	resp := headersContinue(false)
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
	respIsStreaming := isStreamingContentType(ds.rec.ResponseHeaders["content-type"])
	if ds.xlate == nil && (ds.streamRequested || respIsStreaming) && ds.rec.ResponseHeaders[":status"] == "200" {
		resp.ModeOverride = modeOverride(
			extprocfilterv3.ProcessingMode_NONE,
			extprocfilterv3.ProcessingMode_STREAMED,
		)
	}
	if ds.xlate != nil && ds.rec.ResponseHeaders[":status"] == "200" {
		// Committed translation: the body Envoy forwards will not
		// be the body the upstream sent. Drop content-length while
		// the header phase is still open — by the body phase Envoy
		// instead 500s with "mismatch between content length and
		// the length of the mutated body" — and deliver chunked.
		// (Streamed responses carry no content-length; removal is
		// a no-op there.)
		mut := &extprocv3.HeaderMutation{RemoveHeaders: []string{"content-length"}}
		resp.GetResponseHeaders().GetResponse().HeaderMutation = mut
		switch {
		case ds.xlate.streaming && respIsStreaming:
			// Arm chunk-by-chunk stream translation: decode the
			// backend's framing, render the agent's, and promote
			// to STREAMED — buffering an unbounded stream is not
			// an option and the agent wants TTFB. Selection
			// guaranteed both stream codecs exist
			// (StreamsTranslatable); the nil check is a crash
			// guard, failing closed like every post-commit error.
			if ds.xlate.tgt.StreamCodec == nil || ds.xlate.src.StreamCodec == nil {
				ds.rec.TranslationError = "response: stream codec missing post-commit"
				return translation502(ds.xlate.src.Name, "clrk: cross-schema stream translation failed")
			}
			ds.xlate.dec = ds.xlate.tgt.StreamCodec.NewStreamDecoder(ds.xlate.req)
			ds.xlate.enc = ds.xlate.src.StreamCodec.NewStreamEncoder(ds.xlate.srcIR)
			if ct := ds.xlate.enc.ContentType(); !strings.HasPrefix(ds.rec.ResponseHeaders["content-type"], ct) {
				mut.SetHeaders = []*corev3.HeaderValueOption{{
					Header: &corev3.HeaderValue{
						Key:      "content-type",
						RawValue: []byte(ct),
					},
					AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
				}}
			}
			resp.ModeOverride = modeOverride(
				extprocfilterv3.ProcessingMode_NONE,
				extprocfilterv3.ProcessingMode_STREAMED,
			)
		case ds.xlate.streaming:
			// The agent asked for a stream and the commit sent
			// stream:true upstream, but the backend answered with
			// a plain body. The agent's SDK is mid-SSE-parse —
			// re-framing a whole response as a synthetic stream
			// would be guesswork. Rare and honest: fail closed.
			ds.rec.TranslationError = "response: upstream answered a streaming translated request without streaming"
			return translation502(ds.xlate.src.Name, "clrk: upstream did not stream a translated streaming request")
		case respIsStreaming:
			// The backend streamed an answer to a request that was
			// committed non-streaming. The reverse translation
			// needs whole buffered bytes; buffering an unbounded
			// SSE stream is not an option. Post-commit failure:
			// fail closed before any of it reaches the agent.
			ds.rec.TranslationError = "response: upstream streamed a non-streaming translated request"
			return translation502(ds.xlate.src.Name, "clrk: cross-schema translation does not support streaming responses")
		}
	}
	if !contentTypeIncluded(ds.rec.ResponseHeaders["content-type"], ds.includedTypes) {
		ds.respBytesLeft = 0
	}
	ds.respKeepLast = respIsStreaming
	return resp
}

func (ds *downstreamStream) onRequestBody(m *extprocv3.HttpBody, now time.Time) *extprocv3.ProcessingResponse {
	chunk := m.GetBody()
	body, trunc := appendBounded(ds.rec.RequestBody, chunk, &ds.reqBytesLeft)
	if ds.rec.RequestBodyAt.IsZero() {
		ds.rec.RequestBodyAt = now
	}
	ds.rec.RequestBody = body
	ds.rec.RequestTruncated = ds.rec.RequestTruncated || trunc
	if ds.deferReselect {
		ds.fullReqBody = append(ds.fullReqBody, chunk...)
	}
	// Probe each chunk so multi-chunk BUFFERED_PARTIAL deliveries
	// latch as soon as "stream":true appears. Once true, we leave
	// it.
	if !ds.streamRequested {
		ds.streamRequested = parsers.BodyAdvertisesStream(chunk)
	}
	// On the terminal body chunk, run the deferred backend
	// re-selection (reselectable streams) or the existing
	// include-usage rewrite + MCP enforcement (everything else).
	// BUFFERED(_PARTIAL) delivers the whole body in one
	// ProcessingRequest under the buffer limit; multi-chunk only
	// happens when the body exceeds it.
	if m.GetEndOfStream() {
		host, _ := splitHostPort(ds.rec.RequestHeaders[":authority"])
		reqPath := ds.rec.RequestHeaders[":path"]

		if ds.deferReselect {
			return ds.reselectAtBodyEOS(host, reqPath)
		}

		// Non-reselectable stream: today's path, unchanged. Skip
		// when truncation already kicked in — partial JSON can't
		// be safely re-serialized or policy-gated.
		if !ds.rec.RequestTruncated {
			if newBody, mut := parsers.EnsureIncludeUsage(host, reqPath, ds.rec.RequestBody); mut {
				// Update the captured copy so OTLP shows what the
				// upstream actually received, not what the agent
				// originally sent.
				ds.rec.RequestBody = newBody
				ds.rec.RequestBodyRewritten = true
				return bodyMutation(true, newBody)
			}
			// MCP enforcement. Cheap host + content-type gate so
			// non-MCP traffic pays nothing for the JSON-RPC parse.
			// The include-usage rewrite (above) and an MCP deny
			// are mutually exclusive — the former only fires for
			// OpenAI-shaped requests, the latter only when a host
			// falls under an MCPRoute.
			if mcpCandidate(ds.mcpRoutes, host, ds.rec.RequestHeaders["content-type"]) {
				res := ds.mcpRoutes.evaluate(host, parsers.Input{
					ReqBody:      ds.rec.RequestBody,
					ReqTruncated: ds.rec.RequestTruncated,
					ReqHeaders:   ds.rec.RequestHeaders,
				})
				stampMCPResult(&ds.rec, res)
				if res.decision == mcpDecisionDeny {
					return immediateResponse(typev3.StatusCode_Forbidden, res.denyDetail)
				}
			}
		}
	}
	return bodyContinue(true)
}

// reselectAtBodyEOS runs the deferred backend re-selection for a
// reselectable stream once the complete request body is buffered:
// model-aware rule pinning, cross-schema candidate filtering, the
// weighted pick, and the commit (repoint + translation + credential
// injection) or its fallbacks.
func (ds *downstreamStream) reselectAtBodyEOS(host, reqPath string) *extprocv3.ProcessingResponse {
	// Re-selection works from fullReqBody — the complete
	// bytes — never the capped capture; a truncated CAPTURE
	// no longer forces the fallback path.
	system := parsers.SystemFor(host)
	model := parsers.RequestModel(ds.fullReqBody)
	final := ds.routes.match(system, reqPath, model)
	if final == nil {
		final = ds.provMatch
	}
	// Pin the accepting rule now, while the body still
	// carries the agent's original system/model — emit-time
	// re-matching would run against the rewritten body.
	ds.finalRule = final

	// Decode the request through the source codec once,
	// only when the candidate set spans schemas. A decode
	// failure isn't fatal: cross-schema candidates drop out
	// at the filter below (ir == nil) and same-schema
	// reselection proceeds as before. Only malformed bodies
	// are surfaced — ErrUnsupported just means an endpoint
	// translation doesn't cover (embeddings, non-chat).
	var (
		srcProv *llmcall.Provider
		ir      *llmcall.Request
	)
	if crossSchemaCandidates(final.backends, system) {
		if srcProv = llmcall.ByName(system); srcProv != nil && srcProv.Codec != nil {
			decoded, derr := srcProv.Codec.DecodeRequest(llmcall.RequestInput{
				Method:  ds.rec.RequestHeaders[":method"],
				Path:    reqPath,
				Headers: ds.rec.RequestHeaders,
				Body:    ds.fullReqBody,
			})
			switch {
			case derr == nil:
				ir = decoded
			default:
				var merr *llmcall.MalformedError
				if errors.As(derr, &merr) {
					ds.rec.TranslationError = "request decode: " + merr.Detail
				}
			}
		}
	}

	// Per-request translatability filter: same-schema
	// candidates always pass; cross-schema ones need a
	// clean TranslationBlockers verdict (streaming waits
	// for APO-743, capability misses, Gemini tool results)
	// and a modelRewrite covering the decoded model.
	cands, skipped := filterTranslatable(final.backends, system, srcProv, ir)
	ds.rec.TranslationSkippedBackends = skipped

	// Weighted/first pick over the surviving candidates
	// (BackendRef.Weight). A classifier-driven ExtensionRef
	// selector is APO-480 and will swap a different
	// backendSelector in here.
	chosen, ok := staticSelector{}.Select(cands, selectorInput{
		provider:     system,
		model:        model,
		path:         reqPath,
		invocationID: ds.rec.InvocationID,
		reqHeaders:   ds.rec.RequestHeaders,
		reqBody:      ds.fullReqBody,
	})

	// A cross-schema pick translates the request before
	// anything is committed. An encode failure here is
	// pre-commit: drop back to the same-schema candidates
	// and let the usual paths handle whatever remains.
	if ok && !chosen.refuse && ir != nil && chosen.system != system {
		tgtProv := llmcall.ByName(chosen.system)
		newModel, _ := chosen.rewriteModel(ir.Model)
		enc, xreq, xerr := translateRequest(tgtProv, ir, newModel)
		if xerr != nil {
			ds.rec.TranslationError = "request encode: " + xerr.Error()
			chosen, ok = staticSelector{}.Select(sameSchemaOnly(cands, system), selectorInput{
				provider:     system,
				model:        model,
				path:         reqPath,
				invocationID: ds.rec.InvocationID,
				reqHeaders:   ds.rec.RequestHeaders,
				reqBody:      ds.fullReqBody,
			})
		} else {
			// Streamed responses must still yield terminal
			// usage for TokenBudget/telemetry; the opt-in
			// flag belongs to the TARGET schema, so it is
			// injected into the translated body (the source
			// body's flag was already re-encoded away).
			if xreq.Stream && tgtProv.EnsureStreamUsage != nil {
				if nb, changed := tgtProv.EnsureStreamUsage(enc.Path, enc.Body); changed {
					enc.Body = nb
				}
			}
			// Commit: repoint :authority + :path, swap the
			// body for the target-schema encode, set the
			// target codec's headers, shed the source
			// schema's modeled + credential headers, inject
			// the backend's credentials, and ClearRouteCache
			// so DFP re-derives upstream host + SNI.
			mut := setHeaderMut(nil, ":authority", net.JoinHostPort(chosen.host, strconv.Itoa(chosen.port)))
			mut = setHeaderMut(mut, ":path", enc.Path)
			for _, k := range slices.Sorted(maps.Keys(enc.SetHeaders)) {
				mut = setHeaderMut(mut, k, enc.SetHeaders[k])
			}
			injs := ds.creds.lookupForBackend(final, chosen.name)
			if len(injs) > 0 {
				if collisions := redactInjected(ds.rec.RequestHeaders, injs); len(collisions) > 0 {
					ds.logger.Info("Agent supplied a header that policy is configured to inject; proxy overwriting",
						"policies", collisions)
				}
				mut = applyInjections(mut, injs)
			}
			mut = removeHeadersMut(mut, sourceHeaderRemovals(ir, enc, injs))

			ds.rec.SelectedBackendNamespace = chosen.namespace
			ds.rec.SelectedBackendName = chosen.name
			ds.rec.SelectedBackendSchema = chosen.system
			ds.rec.SelectedBackendReselected = true
			ds.rec.TranslationApplied = true
			ds.rec.TranslationFrom = system
			ds.rec.TranslationTo = chosen.system
			ds.rec.TranslationDroppedExtras += enc.DroppedExtras
			// Keep the captured view coherent with what the
			// upstream receives: the translated path keys
			// the response parser (Gemini's model rides the
			// URL) and the translated body is what gen_ai.*
			// is parsed from, per the APO-689 convention.
			ds.rec.RequestHeaders[":path"] = enc.Path
			left := ds.maxCaptureBytes
			ds.rec.RequestBody, ds.rec.RequestTruncated = appendBounded(nil, enc.Body, &left)
			ds.rec.RequestBodyRewritten = true
			ds.xlate = &translationState{
				src:       srcProv,
				tgt:       tgtProv,
				req:       xreq,
				srcIR:     ir,
				streaming: xreq.Stream,
			}
			return bodyMutationWithHeaders(true, enc.Body, mut, true)
		}
	}

	if !ok {
		// Nothing servable for THIS request — no candidate
		// resolved, or per-request filtering emptied the
		// set. Fall back to the header-time host with the
		// rule's route-wide credentials, and still apply
		// the include-usage rewrite the non-reselectable
		// path would have: without it, streamed traffic on
		// an all-cross-schema rule would silently evade
		// TokenBudget.
		mut := injectFallbackCreds(ds.creds, final, ds.rec.RequestHeaders, ds.logger)
		if newBody, changed := parsers.EnsureIncludeUsage(host, reqPath, ds.fullReqBody); changed {
			left := ds.maxCaptureBytes
			ds.rec.RequestBody, ds.rec.RequestTruncated = appendBounded(nil, newBody, &left)
			ds.rec.RequestBodyRewritten = true
			return bodyMutationWithHeaders(true, newBody, mut, false)
		}
		if mut != nil {
			return bodyContinueWithHeaders(true, mut, false)
		}
		return bodyContinue(true)
	}
	if chosen.refuse {
		// Resolved but unservable (InferencePool). Fail
		// cleanly rather than mis-route to the agent's
		// original SaaS host.
		return immediateResponse(typev3.StatusCode_NotImplemented, "clrk: "+chosen.unsupportedReason)
	}

	// Same-schema pick: repoint :authority and inject the
	// backend's credentials (route-wide + the CIP whose
	// parentRef sectionName names this backend + gateway),
	// in the backend's scheme.
	mut := setHeaderMut(nil, ":authority", net.JoinHostPort(chosen.host, strconv.Itoa(chosen.port)))
	if injs := ds.creds.lookupForBackend(final, chosen.name); len(injs) > 0 {
		if collisions := redactInjected(ds.rec.RequestHeaders, injs); len(collisions) > 0 {
			ds.logger.Info("Agent supplied a header that policy is configured to inject; proxy overwriting",
				"policies", collisions)
		}
		mut = applyInjections(mut, injs)
	}
	ds.rec.SelectedBackendNamespace = chosen.namespace
	ds.rec.SelectedBackendName = chosen.name
	ds.rec.SelectedBackendSchema = chosen.system
	ds.rec.SelectedBackendReselected = true

	// Apply per-backend body rewrites (model remap +
	// include-usage in the backend's schema) and emit the
	// repoint with ClearRouteCache so DFP re-derives the
	// upstream host + SNI from the new :authority.
	if newBody, changed := rewriteRequestBody(ds.fullReqBody, reqPath, model, chosen); changed {
		left := ds.maxCaptureBytes
		ds.rec.RequestBody, ds.rec.RequestTruncated = appendBounded(nil, newBody, &left)
		ds.rec.RequestBodyRewritten = true
		return bodyMutationWithHeaders(true, newBody, mut, true)
	}
	return bodyContinueWithHeaders(true, mut, true)
}

func (ds *downstreamStream) onResponseBody(m *extprocv3.HttpBody, now time.Time) *extprocv3.ProcessingResponse {
	ds.rec.ResponseBodyChunks++
	var (
		body  []byte
		trunc bool
	)
	if ds.respKeepLast {
		body, trunc = appendRing(ds.rec.ResponseBody, m.GetBody(), ds.maxCaptureBytes)
	} else {
		body, trunc = appendBounded(ds.rec.ResponseBody, m.GetBody(), &ds.respBytesLeft)
	}
	if ds.rec.ResponseBodyAt.IsZero() {
		ds.rec.ResponseBodyAt = now
	}
	ds.rec.ResponseBody = body
	ds.rec.ResponseTruncated = ds.rec.ResponseTruncated || trunc
	if ds.xlate != nil && ds.rec.ResponseHeaders[":status"] == "200" {
		if ds.xlate.streaming {
			// Chunk-by-chunk stream translation, armed at
			// ResponseHeaders. The captured rec.ResponseBody
			// (ring-capped above) intentionally keeps the
			// UPSTREAM stream bytes — telemetry parses them
			// keyed to SelectedBackendSchema, and the injected
			// include_usage means they carry usage. dec is nil
			// only when the header phase already failed the
			// stream closed; nothing to do then.
			if ds.xlate.dec != nil {
				return translateStreamChunk(&ds.rec, ds.xlate, m.GetBody(), m.GetEndOfStream())
			}
			return bodyContinue(false)
		}
		// Reverse translation: the upstream answered in the
		// backend's schema and the agent can only parse the one
		// it spoke. Decode through the backend codec from the
		// full-fidelity buffer (the capture above may be capped),
		// re-encode through the agent's. The captured
		// rec.ResponseBody intentionally keeps the UPSTREAM
		// bytes — that is the conversation gen_ai.* is parsed
		// from (the parser is keyed to SelectedBackendSchema).
		// Any failure here is post-commit: fail closed with a
		// 502 shaped for the agent's schema rather than hand it
		// a body it will misparse. Non-200 responses pass
		// through untranslated — the status code carries the
		// signal and error envelopes are provider-specific
		// anyway.
		ds.fullRespBody = append(ds.fullRespBody, m.GetBody()...)
		if m.GetEndOfStream() {
			decoded, derr := ds.xlate.tgt.Codec.DecodeResponse(llmcall.ResponseInput{
				Status:  200,
				Headers: ds.rec.ResponseHeaders,
				Body:    ds.fullRespBody,
			}, ds.xlate.req)
			if derr != nil {
				ds.rec.TranslationError = "response decode: " + derr.Error()
				return translation502(ds.xlate.src.Name, "clrk: cross-schema response translation failed")
			}
			dropped := llmcall.StripResponseForTranslation(decoded)
			decoded.Provider = ds.xlate.src.Name
			enc, eerr := ds.xlate.src.Codec.EncodeResponse(decoded, llmcall.EncodeOptions{Mode: llmcall.ModeStrip})
			if eerr != nil {
				ds.rec.TranslationError = "response encode: " + eerr.Error()
				return translation502(ds.xlate.src.Name, "clrk: cross-schema response translation failed")
			}
			ds.rec.TranslationDroppedExtras += dropped + enc.DroppedExtras
			if len(enc.SetHeaders) > 0 {
				var mut *extprocv3.HeaderMutation
				for _, k := range slices.Sorted(maps.Keys(enc.SetHeaders)) {
					mut = setHeaderMut(mut, k, enc.SetHeaders[k])
				}
				return bodyMutationWithHeaders(false, enc.Body, mut, false)
			}
			return bodyMutation(false, enc.Body)
		}
	}
	return bodyContinue(false)
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
