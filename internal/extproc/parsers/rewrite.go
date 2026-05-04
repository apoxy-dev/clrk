package parsers

import (
	"encoding/json"
	"strings"
)

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
	return rewriteIncludeUsage(body)
}

func shouldRewriteIncludeUsage(host, path string) bool {
	host = strings.ToLower(host)
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	switch host {
	case "api.openai.com":
		return strings.HasPrefix(path, "/v1/chat/completions")
	case "generativelanguage.googleapis.com":
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
	// stream_options either missing, or set to a non-object (caller
	// bug we don't try to fix), or an object that may or may not opt
	// in. We only mutate when we can produce a syntactically clean
	// stream_options.include_usage=true.
	soRaw, hasSO := obj["stream_options"]
	if hasSO {
		so, ok := soRaw.(map[string]any)
		if !ok {
			// Caller passed a non-object (e.g. null, array). Replace
			// rather than try to coerce — upstream would reject the
			// malformed shape regardless.
			obj["stream_options"] = map[string]any{"include_usage": true}
		} else {
			if v, present := so["include_usage"]; present {
				if b, isBool := v.(bool); isBool && b {
					// Already opted in; nothing to do.
					return nil, false
				}
			}
			so["include_usage"] = true
			obj["stream_options"] = so
		}
	} else {
		obj["stream_options"] = map[string]any{"include_usage": true}
	}
	out, err := json.Marshal(obj)
	if err != nil {
		// Should never happen for a successfully-decoded map.
		return nil, false
	}
	return out, true
}
