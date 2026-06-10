package extproc

import (
	"maps"
	"slices"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"

	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
)

// translationState carries the cross-schema facts a stream needs from
// the request-side commit through to response-side translation: which
// schema the agent spoke, which the selected backend speaks, and the
// translated IR request the backend codec needs to decode the
// response. Non-nil means a translated request was sent upstream, so
// the response MUST come back through the reverse translation (or fail
// closed) — the agent cannot parse the backend's schema.
type translationState struct {
	src *llmcall.Provider
	tgt *llmcall.Provider
	req *llmcall.Request

	// srcIR is the pristine source decode — the stream encoder's
	// context (framing choices, model fallback come from what the
	// agent actually sent, not the translated request).
	srcIR *llmcall.Request

	// streaming records that the committed translated request has
	// Stream=true: the response must arrive as a stream and is
	// translated chunk-by-chunk in STREAMED mode rather than buffered.
	streaming bool

	// dec/enc are armed at ResponseHeaders once a 200 streaming
	// response is confirmed: dec consumes the backend's stream, enc
	// renders the agent's.
	dec llmcall.StreamDecoder
	enc llmcall.StreamEncoder

	// failed latches a mid-stream translation failure: a synthesized
	// source-schema error frame has been sent and every remaining
	// upstream chunk is swallowed (replaced with an empty mutation).
	failed bool
}

// translateStreamChunk converts one STREAMED response chunk from the
// backend's stream framing to the agent's. Mid-stream failure cannot
// 502 (response headers are long gone) and must not hand the agent
// raw foreign-schema bytes or silently truncate: it renders a
// schema-correct error frame plus the encoder's terminal sequence,
// latches failed, and swallows the rest of the upstream.
func translateStreamChunk(rec *Record, x *translationState, chunk []byte, eos bool) *extprocv3.ProcessingResponse {
	if x.failed {
		return bodyMutationStreamed(false, nil)
	}
	events, derr := x.dec.Decode(chunk, eos)
	var out []byte
	var encErr error
	if len(events) > 0 {
		out, encErr = x.enc.Encode(events)
	}
	if derr == nil && encErr == nil {
		return bodyMutationStreamed(false, out)
	}
	x.failed = true
	if derr != nil {
		rec.TranslationError = "stream decode: " + derr.Error()
	} else {
		rec.TranslationError = "stream encode: " + encErr.Error()
	}
	// Best-effort: the error frame and terminal sequence may
	// themselves fail to encode; the empty mutation still protects
	// the agent from foreign bytes.
	errOut, _ := x.enc.Encode([]llmcall.StreamEvent{
		{Type: llmcall.StreamEventError, Error: &llmcall.StreamError{
			Code:    "bad_gateway",
			Message: "clrk: cross-schema stream translation failed",
		}},
		{Type: llmcall.StreamEventDone},
	})
	return bodyMutationStreamed(false, append(out, errOut...))
}

// crossSchemaCandidates reports whether any servable candidate speaks
// a different wire schema than the rule's provider — i.e. whether this
// stream may need translation and the request is worth decoding.
// Rules with provider "custom" (or an unparseable host) never
// translate: there is no source codec, and the operator vouched for
// the candidate set.
func crossSchemaCandidates(cands []resolvedBackend, system string) bool {
	if system == "" || system == "custom" {
		return false
	}
	for _, c := range cands {
		if !c.refuse && c.system != system {
			return true
		}
	}
	return false
}

// filterTranslatable drops the cross-schema candidates this request
// cannot be translated to. Same-schema and refuse-mode candidates
// always pass. A cross-schema candidate passes when the source decode
// succeeded, TranslationBlockers clears the pair for the features this
// request actually uses, and the candidate has a modelRewrite covering
// the request's model (model IDs don't transfer across providers).
// skipped counts the dropped candidates for telemetry.
func filterTranslatable(cands []resolvedBackend, system string, src *llmcall.Provider, ir *llmcall.Request) (kept []resolvedBackend, skipped int) {
	kept = make([]resolvedBackend, 0, len(cands))
	for _, c := range cands {
		if c.refuse || c.system == system {
			kept = append(kept, c)
			continue
		}
		if ir == nil {
			skipped++
			continue
		}
		if len(llmcall.TranslationBlockers(ir, src, llmcall.ByName(c.system))) > 0 {
			skipped++
			continue
		}
		if _, ok := c.rewriteModel(ir.Model); !ok {
			skipped++
			continue
		}
		kept = append(kept, c)
	}
	return kept, skipped
}

// sameSchemaOnly restricts candidates to the rule provider's schema
// (plus refuse-mode entries, which must keep failing loudly) — the
// fallback set when a cross-schema encode fails pre-commit.
func sameSchemaOnly(cands []resolvedBackend, system string) []resolvedBackend {
	out := make([]resolvedBackend, 0, len(cands))
	for _, c := range cands {
		if c.refuse || c.system == system {
			out = append(out, c)
		}
	}
	return out
}

// translateRequest converts a decoded request to the target schema:
// DeepCopy (fallback paths keep the pristine decode), normalize
// (system lifting, tool-call IDs), strip the source schema's wire
// bookkeeping, stamp the rewritten model, and encode canonically.
// Returns the encoded request and the translated IR the response
// phase needs for DecodeResponse.
func translateRequest(tgt *llmcall.Provider, ir *llmcall.Request, model string) (*llmcall.EncodedRequest, *llmcall.Request, error) {
	cp := ir.DeepCopy()
	llmcall.LiftSystemMessages(cp)
	llmcall.EnsureToolCallIDs(cp)
	dropped := llmcall.StripRequestForTranslation(cp)
	cp.Provider = tgt.Name
	cp.Model = model
	enc, err := tgt.Codec.EncodeRequest(cp, llmcall.EncodeOptions{Mode: llmcall.ModeStrip})
	if err != nil {
		return nil, nil, err
	}
	enc.DroppedExtras += dropped
	return enc, cp, nil
}

// sourceHeaderRemovals lists the headers to strip from a translated
// request: every registered provider's auth headers (the agent's
// credential belongs to the SOURCE provider and must not reach a
// different upstream — the target's credential arrives via
// CredentialInjectionPolicy) plus whatever request headers the source
// codec modeled (ir.Wire.Headers — e.g. anthropic-version), minus
// anything the target codec or credential injection is about to set
// (Envoy's ordering between SetHeaders and RemoveHeaders is not a
// contract we want to depend on). Sorted for deterministic mutations.
func sourceHeaderRemovals(ir *llmcall.Request, enc *llmcall.EncodedRequest, injs []credInjection) []string {
	keep := make(map[string]bool, len(enc.SetHeaders)+len(injs))
	for k := range enc.SetHeaders {
		keep[strings.ToLower(k)] = true
	}
	for _, inj := range injs {
		keep[strings.ToLower(inj.headerName)] = true
	}
	seen := make(map[string]bool)
	var out []string
	add := func(h string) {
		h = strings.ToLower(h)
		if h == "" || keep[h] || seen[h] {
			return
		}
		seen[h] = true
		out = append(out, h)
	}
	for _, h := range llmcall.AuthHeaderUnion() {
		add(h)
	}
	for _, h := range slices.Sorted(maps.Keys(ir.Wire.Headers)) {
		add(h)
	}
	for _, h := range enc.RemoveHeaders {
		add(h)
	}
	slices.Sort(out)
	return out
}

// removeHeadersMut appends RemoveHeaders entries on existing
// (allocating one if nil), the removal-side sibling of setHeaderMut.
func removeHeadersMut(existing *extprocv3.HeaderMutation, names []string) *extprocv3.HeaderMutation {
	if len(names) == 0 {
		return existing
	}
	if existing == nil {
		existing = &extprocv3.HeaderMutation{}
	}
	existing.RemoveHeaders = append(existing.RemoveHeaders, names...)
	return existing
}

// translation502 fails a committed-translation stream closed: the
// request went upstream in the backend's schema, the response cannot
// be brought back into the client's, and handing the client a body it
// will misparse is worse than a clean 502. Sent from a response phase
// — ResponseBodyMode=BUFFERED means Envoy is still holding the
// upstream's headers and body, so the ImmediateResponse replaces the
// whole response.
func translation502(clientSchema, msg string) *extprocv3.ProcessingResponse {
	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_ImmediateResponse{
			ImmediateResponse: &extprocv3.ImmediateResponse{
				Status: &typev3.HttpStatus{Code: typev3.StatusCode_BadGateway},
				Body:   llmcall.ErrorBodyFor(clientSchema, msg),
				Headers: &extprocv3.HeaderMutation{
					SetHeaders: []*corev3.HeaderValueOption{{
						Header: &corev3.HeaderValue{
							Key:      "content-type",
							RawValue: []byte("application/json"),
						},
						AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
					}},
				},
			},
		},
	}
}
