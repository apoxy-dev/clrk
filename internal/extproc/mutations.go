package extproc

// This file holds the ProcessingResponse and mutation builders shared
// by the stream handlers. Everything here is a pure constructor: no
// per-stream state, no I/O.

import (
	"regexp"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocfilterv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"

	"github.com/apoxy-dev/clrk/internal/extproc/invocationctx"
	"github.com/apoxy-dev/clrk/internal/extproc/tracectx"
)

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

// modeOverride builds a complete ProcessingMode for ModeOverride
// replies. Envoy REPLACES the stream's whole ProcessingMode rather
// than merging with the filter's static config (ext_proc.cc
// handleHeadersResponse): a body mode left at proto zero means NONE,
// so BOTH directions must be asserted on every override or the
// unasserted one silently stops reaching ext_proc. Header/trailer
// modes are safe at zero (DEFAULT keeps the static behavior).
func modeOverride(req, resp extprocfilterv3.ProcessingMode_BodySendMode) *extprocfilterv3.ProcessingMode {
	return &extprocfilterv3.ProcessingMode{
		RequestBodyMode:  req,
		ResponseBodyMode: resp,
	}
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

// bodyMutationStreamed returns a CONTINUE body response whose
// BodyMutation replaces the current STREAMED chunk. It must NOT use
// CONTINUE_AND_REPLACE: in streamed mode that halts delivery of every
// subsequent chunk to ext_proc (Envoy treats it as "replace the whole
// body, stop processing"), while CONTINUE+BodyMutation swaps just the
// dequeued chunk and keeps the stream flowing — the envoyproxy/
// ai-gateway pattern. newBody may be empty (the chunk is consumed
// without emitting bytes, e.g. a mid-frame fragment or a swallowed
// post-failure chunk).
func bodyMutationStreamed(isRequest bool, newBody []byte) *extprocv3.ProcessingResponse {
	r := &extprocv3.BodyResponse{Response: &extprocv3.CommonResponse{
		Status: extprocv3.CommonResponse_CONTINUE,
		BodyMutation: &extprocv3.BodyMutation{
			Mutation: &extprocv3.BodyMutation_Body{Body: newBody},
		},
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
