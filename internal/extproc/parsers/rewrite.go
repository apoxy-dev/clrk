package parsers

import (
	"bytes"
	"encoding/json"
	"strings"
)

// streamColonTrue is a cheap byte-scan probe for `"stream":true` (or
// the same with whitespace around the colon) in a request body. It's
// not a JSON parser — false positives are possible inside string
// values that contain that literal — but it's good enough as a
// pre-check to avoid the dominant non-streamed code path doing a
// full JSON decode + map allocation just to bail.
var streamColonTrueProbes = [][]byte{
	[]byte(`"stream":true`),
	[]byte(`"stream": true`),
}

// BodyAdvertisesStream reports whether the request body looks like it
// opts into streaming. The match is the byte-scan above; callers that
// need strict JSON semantics should decode instead.
func BodyAdvertisesStream(body []byte) bool {
	for _, p := range streamColonTrueProbes {
		if bytes.Contains(body, p) {
			return true
		}
	}
	return false
}

// EnsureIncludeUsage rewrites an OpenAI-shape chat-completion request
// body so that streaming responses always emit terminal usage. Returns
// (newBody, true) when the body was mutated and (nil, false) otherwise.
//
// OpenAI ships streamed `usage` only when the caller sets
// `stream_options.include_usage=true` in the request. Without that
// opt-in, streamed responses produce zero tokens at clrk's parser
// layer — which silently breaks daily TokenBudget enforcement and
// cost attribution on the dominant traffic shape (most agent SDKs
// stream by default for TTFB).
//
// Forcing the flag here turns "caller-config gap" into "always
// instrumented": agents don't have to know about the flag, and budget
// enforcement applies uniformly to streaming and non-streaming.
//
// Targets:
//
//   - api.openai.com /v1/chat/completions (OpenAI direct).
//   - generativelanguage.googleapis.com /v1beta/openai/chat/completions
//     and similar (Gemini OpenAI-compat surface — same wire format).
//
// We deliberately don't rewrite when `stream` is false or absent — a
// non-streamed response carries usage natively and the flag becomes a
// no-op the upstream may reject as unknown.
//
// Caller is responsible for emitting the mutated bytes via ext_proc
// BodyMutation; Envoy recomputes content-length on the wire.
func EnsureIncludeUsage(host, path string, body []byte) ([]byte, bool) {
	if !shouldRewriteIncludeUsage(host, path) {
		return nil, false
	}
	if !BodyAdvertisesStream(body) {
		return nil, false
	}
	return rewriteIncludeUsage(body)
}

// shouldRewriteIncludeUsage routes via the same provider table the
// parsers use, so adding a new OpenAI-shape upstream (e.g. Azure
// OpenAI) only needs to register in `hostProviders` once.
func shouldRewriteIncludeUsage(host, path string) bool {
	switch SystemFor(host) {
	case "openai":
		return strings.HasPrefix(path, "/v1/chat/completions")
	case "google_genai":
		// Gemini's OpenAI-compat surface: /v1[beta]/openai/chat/completions.
		// Native /v1beta/models/...:streamGenerateContent never carries
		// stream_options — emits usageMetadata cumulatively.
		return strings.Contains(path, "/openai/chat/completions")
	}
	return false
}

// rewriteIncludeUsage decodes body as JSON, ensures
// stream_options.include_usage=true when stream=true, and returns the
// re-serialized body. Returns (nil, false) when the body isn't a JSON
// object, isn't a streamed call, or already opted in.
//
// JSON roundtrip via map[string]any preserves every top-level field
// the upstream cares about; key ordering may shift (JSON has no
// ordering contract), and integer-valued numbers ride through float64
// — chat-completion params (temperature, max_tokens, n, ...) are all
// well within float64 precision.
func rewriteIncludeUsage(body []byte) ([]byte, bool) {
	if len(body) == 0 {
		return nil, false
	}
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		return nil, false
	}
	if obj == nil {
		return nil, false
	}
	streamRaw, ok := obj["stream"]
	if !ok {
		return nil, false
	}
	streamOn, _ := streamRaw.(bool)
	if !streamOn {
		return nil, false
	}
	// stream_options either missing, set to a non-object (caller bug
	// we don't try to fix — replace), or an object that may or may
	// not opt in. We only need to leave the call site with a clean
	// stream_options.include_usage=true.
	so, ok := obj["stream_options"].(map[string]any)
	if !ok {
		obj["stream_options"] = map[string]any{"include_usage": true}
	} else {
		if b, _ := so["include_usage"].(bool); b {
			return nil, false
		}
		so["include_usage"] = true
	}
	out, err := json.Marshal(obj)
	if err != nil {
		// Should never happen for a successfully-decoded map.
		return nil, false
	}
	return out, true
}
