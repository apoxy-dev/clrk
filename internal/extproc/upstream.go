package extproc

// upstreamStream handles one upstream (cluster-level) ext_proc stream,
// which Envoy opens fresh for every router attempt against a
// synthesized clrk-llm-* cluster (see internal/egextension/llm.go).
// The filter is request-only: it sees the attempt's request headers
// and the router-replayed body, never the response. Holding response
// headers in an upstream filter races the router's retry decision and
// violates its upstream_requests_ invariant (see
// buildLLMUpstreamExtProcFilter) — response adaptation belongs
// downstream, keyed off the final serving attempt this handler
// records.
//
// Per attempt: learn which Backend Envoy's load balancer picked from
// the xds.upstream_host_metadata attribute, join it with the request's
// shared state (published by the downstream pin, keyed by
// x-request-id), and adapt the request to that backend — cross-schema
// translation or per-backend body rewrite, :authority/:path repoint,
// source-credential shedding, and CredentialInjectionPolicy injection.
// All mutations are returned on the request-body response: the body
// mode is BUFFERED, so header mutations attached there are honored,
// and the single commit point mirrors the shape of the old downstream
// commit this replaces.

import (
	"context"
	"fmt"
	"maps"
	"net"
	"slices"
	"strconv"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/go-logr/logr"
	"google.golang.org/protobuf/encoding/prototext"

	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
	"github.com/apoxy-dev/clrk/internal/llmroute"
)

// upstreamHostMetadataAttr is the request attribute the egextension
// configures on the upstream ext_proc filter. Envoy resolves it to the
// selected endpoint's metadata after load balancing and delivers it as
// a string holding the prototext rendering of a
// envoy.config.core.v3.Metadata message.
const upstreamHostMetadataAttr = "xds.upstream_host_metadata"

// attemptBackend is the endpoint identity parsed from the selected
// host's metadata (the clrk.apoxy.dev namespace stamped by
// buildLLMCluster).
type attemptBackend struct {
	namespace string
	name      string
	schema    string
	host      string
	port      int
}

// upstreamStream accumulates one attempt's state across its request
// phases.
type upstreamStream struct {
	srv    *Server
	logger logr.Logger

	creds *credTable

	requestID string
	path      string
	method    string
	// reqHeaders is the attempt's request header map (lowercased),
	// kept for SigV4 signing (unsigned x-amz-* shedding).
	reqHeaders map[string]string
	backend    *attemptBackend
	state      *requestState
	// cand is the pinned rule's candidate matching backend — the
	// modelRewrites/bodyMutation facts the adaptation needs.
	cand *resolvedBackend
	// failed latches a fail-closed headers phase so the body phase
	// doesn't double-respond.
	failed bool

	startedAt time.Time
}

// newUpstreamStream resolves the per-EG credential table once per
// attempt stream (same registry path as the downstream handler; the
// upstream filter's GrpcService carries the same per-EG authority).
func (s *Server) newUpstreamStream(ctx context.Context, logger logr.Logger) *upstreamStream {
	us := &upstreamStream{
		srv:       s,
		logger:    logger.WithName("upstream"),
		startedAt: time.Now(),
	}
	if s.client != nil {
		if es, err := s.registry.get(ctx); err == nil {
			us.creds = es.creds
		}
	}
	return us
}

// handle processes one message of the attempt stream and returns the
// response to send, or nil to skip. Only request-direction messages
// arrive (the filter's response modes are SKIP/NONE); the response
// cases are kept as guards against config drift.
func (us *upstreamStream) handle(req *extprocv3.ProcessingRequest) *extprocv3.ProcessingResponse {
	us.captureHostMetadata(req)
	switch m := req.GetRequest().(type) {
	case *extprocv3.ProcessingRequest_RequestHeaders:
		return us.onRequestHeaders(m.RequestHeaders)
	case *extprocv3.ProcessingRequest_RequestBody:
		return us.onRequestBody(m.RequestBody)
	case *extprocv3.ProcessingRequest_RequestTrailers:
		return trailersContinue(true)
	case *extprocv3.ProcessingRequest_ResponseHeaders:
		return headersContinue(false)
	case *extprocv3.ProcessingRequest_ResponseBody:
		return bodyContinue(false)
	case *extprocv3.ProcessingRequest_ResponseTrailers:
		return trailersContinue(false)
	default:
		us.logger.V(1).Info("Unhandled upstream message type", "type", fmt.Sprintf("%T", req.GetRequest()))
		return nil
	}
}

// onRequestHeaders joins the attempt with its request's shared state
// and the picked endpoint's backend identity. Mutations wait for the
// body phase; a join failure fails the attempt closed right here —
// an ImmediateResponse from an upstream filter is a local reply, so
// the failure is final rather than retried, which is correct: the
// state is process-local and a later attempt cannot fare better.
func (us *upstreamStream) onRequestHeaders(m *extprocv3.HttpHeaders) *extprocv3.ProcessingResponse {
	hdrs := headersToMap(m)
	us.requestID = hdrs["x-request-id"]
	us.path = hdrs[":path"]
	us.method = hdrs[":method"]
	us.reqHeaders = hdrs
	us.state = us.srv.states.get(us.requestID)
	if us.state == nil || us.state.rule == nil {
		us.failed = true
		us.logger.Info("Attempt has no request state; failing closed",
			"requestID", us.requestID)
		return immediateResponse(typev3.StatusCode_BadGateway, "clrk: no request state for pinned attempt")
	}
	if us.backend == nil {
		us.failed = true
		us.logger.Info("Attempt delivered no upstream host metadata; failing closed",
			"requestID", us.requestID)
		return immediateResponse(typev3.StatusCode_BadGateway, "clrk: no backend identity for pinned attempt")
	}
	for i := range us.state.rule.backends {
		c := &us.state.rule.backends[i]
		if c.namespace == us.backend.namespace && c.name == us.backend.name {
			us.cand = c
			break
		}
	}
	if us.cand == nil {
		// The endpoint Envoy picked is not in the pinned rule's
		// candidate set — a config-generation skew between the route
		// table snapshot and the synthesized cluster. Fail closed
		// rather than send the request without its adaptation.
		us.failed = true
		us.logger.Info("Attempt backend not in pinned rule's candidates; failing closed",
			"requestID", us.requestID,
			"backend", us.backend.namespace+"/"+us.backend.name)
		return translation502(us.state.system, "clrk: attempt backend not in rule candidates")
	}
	us.logger.V(1).Info("Attempt request headers",
		"requestID", us.requestID,
		"backend", us.backend.namespace+"/"+us.backend.name,
		"schema", us.backend.schema)
	// A bodyless request (GET/HEAD/DELETE control-plane calls) carries
	// end_of_stream on its headers: no RequestBody message will arrive, so
	// onRequestBody -- the usual commit point for repoint + credential
	// injection -- never fires. Commit here instead. There is no body to
	// translate or rewrite, and the downstream pin already dropped every
	// cross-schema candidate for a bodyless request, so the picked backend
	// is same-schema: repoint :authority and inject the backend's
	// credentials over the (empty) body.
	if m.GetEndOfStream() {
		return us.commitBodyless()
	}
	return headersContinue(true)
}

// commitBodyless adapts a bodyless attempt at request-headers time: the
// same-schema repoint + credential injection onRequestBody would do, minus
// the body rewrite and translation (there is no body). It signs over an
// empty payload, which is correct for the GET/HEAD/DELETE control-plane
// calls that reach this path.
func (us *upstreamStream) commitBodyless() *extprocv3.ProcessingResponse {
	st, cand := us.state, us.cand
	injs := us.creds.lookupForBackend(st.rule, cand.name)
	mut := us.repointAndInject(injs, nil)
	st.appendAttempt(attemptFact{
		backendNamespace: cand.namespace,
		backendName:      cand.name,
		backendSchema:    cand.system,
	})
	return headersContinueWithHeaders(mut, false)
}

// repointAndInject builds the per-attempt request-header mutation shared
// by the same-schema commit paths: the :authority repoint to the picked
// endpoint, the backend's header-form credential injections, and any
// SigV4 signature over body -- the bytes that actually go out (nil for a
// bodyless attempt, the empty-body hash). Cross-schema attempts build
// their own mutation in commitTranslated because path/body change too.
func (us *upstreamStream) repointAndInject(injs []credInjection, body []byte) *extprocv3.HeaderMutation {
	authority := net.JoinHostPort(us.cand.host, strconv.Itoa(us.cand.port))
	hdrInjs, sigInjs := splitInjections(injs)
	mut := setHeaderMut(nil, ":authority", authority)
	mut = applyInjections(mut, hdrInjs)
	return applySigning(mut, sigInjs, signInput{
		method:    us.method,
		authority: authority,
		path:      us.path,
		body:      body,
		headers:   us.reqHeaders,
	}, us.logger)
}

// onRequestBody is the attempt's commit point: the router replayed the
// complete (BUFFERED) body, the backend is known, adapt and send.
func (us *upstreamStream) onRequestBody(m *extprocv3.HttpBody) *extprocv3.ProcessingResponse {
	if us.failed {
		return bodyContinue(true)
	}
	if !m.GetEndOfStream() {
		// BUFFERED mode delivers exactly one message with
		// end_of_stream; anything else means the mode drifted.
		return bodyContinue(true)
	}
	st, cand := us.state, us.cand
	injs := us.creds.lookupForBackend(st.rule, cand.name)

	// Custom rules are operator-vouched passthrough: clrk cannot verify
	// schema parity (often there is no recognized source schema at
	// all), so a schema mismatch is not a translation trigger — the
	// attempt repoints and rewrites like a same-schema one.
	if !st.rule.trustsOperator() {
		if cand.system != st.system && st.srcIR != nil {
			return us.commitTranslated(injs)
		}
		if cand.system != st.system {
			// A cross-schema endpoint without a source decode should have
			// been filtered at pin time (or excluded via the subset pin);
			// reaching here means state skew. Never send the source-schema
			// body to a foreign-schema endpoint.
			return translation502(st.system, "clrk: cross-schema attempt without decoded request")
		}
	}

	// Same-schema attempt: apply the per-backend body rewrites first
	// (model remap + include-usage in the backend's schema), THEN build
	// the header mutation — :authority repoint, credential injection,
	// and any SigV4 signature, which must cover the bytes that actually
	// go out.
	finalBody := m.GetBody()
	bodyChanged := false
	if newBody, changed := rewriteRequestBody(m.GetBody(), us.path, st.model, *cand); changed {
		finalBody, bodyChanged = newBody, true
	}
	mut := us.repointAndInject(injs, finalBody)
	fact := attemptFact{
		backendNamespace: cand.namespace,
		backendName:      cand.name,
		backendSchema:    cand.system,
	}
	if bodyChanged {
		fact.bodyRewritten = true
		fact.sentPath = us.path
		fact.sentBody = finalBody
		st.appendAttempt(fact)
		return bodyMutationWithHeaders(true, finalBody, mut, false)
	}
	st.appendAttempt(fact)
	return bodyContinueWithHeaders(true, mut, false)
}

// commitTranslated adapts a cross-schema attempt: translate the source
// IR to the picked backend's schema and swap path/headers/body. An
// encode failure fails the attempt closed — candidates were
// pre-filtered for translatability at pin time, so this is
// exceptional, and a local 502 beats sending a half-adapted request.
func (us *upstreamStream) commitTranslated(injs []credInjection) *extprocv3.ProcessingResponse {
	st, cand := us.state, us.cand
	tgtProv := llmcall.ByName(cand.system)
	if tgtProv == nil || tgtProv.Codec == nil {
		return translation502(st.system, "clrk: no codec for attempt backend schema")
	}
	// Candidates were pre-filtered for a covering rewrite at pin time;
	// a miss here means the attempt landed outside the viable set
	// (subset-pin violation or config skew). Fail closed rather than
	// commit a translated request with an empty model.
	newModel, ok := cand.rewriteModel(st.srcIR.Model)
	if !ok {
		us.logger.Info("Attempt backend has no model rewrite for the pinned model; failing closed",
			"requestID", us.requestID, "model", st.srcIR.Model)
		return translation502(st.system, "clrk: no model rewrite for attempt backend")
	}
	enc, xreq, xerr := translateRequest(tgtProv, st.srcIR, newModel)
	if xerr != nil {
		us.logger.Info("Attempt translation failed; failing closed",
			"requestID", us.requestID, "error", xerr.Error())
		return translation502(st.system, "clrk: cross-schema request translation failed")
	}
	// Streamed responses must still yield terminal usage for
	// TokenBudget/telemetry; the opt-in flag belongs to the TARGET
	// schema, so it is injected into the translated body (the source
	// body's flag was already re-encoded away).
	if xreq.Stream && tgtProv.EnsureStreamUsage != nil {
		if nb, changed := tgtProv.EnsureStreamUsage(enc.Path, enc.Body); changed {
			enc.Body = nb
		}
	}
	authority := net.JoinHostPort(cand.host, strconv.Itoa(cand.port))
	mut := setHeaderMut(nil, ":authority", authority)
	mut = setHeaderMut(mut, ":path", enc.Path)
	for _, k := range slices.Sorted(maps.Keys(enc.SetHeaders)) {
		mut = setHeaderMut(mut, k, enc.SetHeaders[k])
	}
	hdrInjs, sigInjs := splitInjections(injs)
	mut = applyInjections(mut, hdrInjs)
	// The signature covers enc.Body/enc.Path as finalized above (the
	// stream-usage rewrite included); sourceHeaderRemovals' keep-set
	// excludes the signature headers, so the removals below never undo
	// what signing just set.
	mut = applySigning(mut, sigInjs, signInput{
		method:    us.method,
		authority: authority,
		path:      enc.Path,
		body:      enc.Body,
		headers:   us.reqHeaders,
	}, us.logger)
	mut = removeHeadersMut(mut, sourceHeaderRemovals(st.srcIR, enc, injs))

	st.appendAttempt(attemptFact{
		backendNamespace:   cand.namespace,
		backendName:        cand.name,
		backendSchema:      cand.system,
		translationApplied: true,
		tgt:                tgtProv,
		xreq:               xreq,
		droppedExtras:      enc.DroppedExtras,
		bodyRewritten:      true,
		sentPath:           enc.Path,
		sentBody:           enc.Body,
	})
	return bodyMutationWithHeaders(true, enc.Body, mut, false)
}

// finish logs the attempt summary once the stream closes.
func (us *upstreamStream) finish() {
	backend := ""
	if us.backend != nil {
		backend = us.backend.namespace + "/" + us.backend.name
	}
	us.logger.V(1).Info("Attempt finished",
		"requestID", us.requestID,
		"backend", backend,
		"failed", us.failed,
		"duration", time.Since(us.startedAt))
}

// captureHostMetadata parses the selected endpoint's identity from the
// xds.upstream_host_metadata attribute, when present. Envoy delivers
// request attributes keyed by the requesting filter's name; we don't
// depend on that key and scan all entries for the attribute field.
func (us *upstreamStream) captureHostMetadata(req *extprocv3.ProcessingRequest) {
	if us.backend != nil {
		return
	}
	for _, attrs := range req.GetAttributes() {
		v, ok := attrs.GetFields()[upstreamHostMetadataAttr]
		if !ok {
			continue
		}
		md := &corev3.Metadata{}
		if err := prototext.Unmarshal([]byte(v.GetStringValue()), md); err != nil {
			us.logger.V(1).Info("Unparseable upstream host metadata", "error", err.Error())
			continue
		}
		fm, ok := md.GetFilterMetadata()[llmroute.EndpointMetaNamespace]
		if !ok {
			continue
		}
		fields := fm.GetFields()
		b := &attemptBackend{
			namespace: fields[llmroute.EndpointMetaBackendNamespace].GetStringValue(),
			name:      fields[llmroute.EndpointMetaBackendName].GetStringValue(),
			schema:    fields[llmroute.EndpointMetaBackendSchema].GetStringValue(),
			host:      fields[llmroute.EndpointMetaBackendHost].GetStringValue(),
		}
		b.port, _ = strconv.Atoi(fields[llmroute.EndpointMetaBackendPort].GetStringValue())
		if b.name != "" {
			us.backend = b
		}
	}
}
