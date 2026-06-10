package openai

import (
	"strings"

	"github.com/apoxy-dev/clrk/internal/extproc/jsonx"
)

// EnsureIncludeUsageBody decodes an OpenAI-shape chat-completion body,
// ensures stream_options.include_usage=true when stream=true, and
// returns the re-serialized body. Returns (nil, false) when the body
// isn't a JSON object, isn't a streamed call, or already opted in.
//
// OpenAI ships streamed `usage` only on this opt-in. Without it,
// streamed responses produce zero tokens at clrk's parser layer —
// silently breaking TokenBudget enforcement and cost attribution on
// the dominant traffic shape (most agent SDKs stream by default for
// TTFB). Forcing the flag turns "caller-config gap" into "always
// instrumented".
//
// Exported for wire-shape sharing: the google plugin applies it on
// Gemini's OpenAI-compat surface and azure_openai on its deployments
// surface — the shape is OpenAI's, not this package's registration.
//
// JSON roundtrip via map[string]any preserves every top-level field
// the upstream cares about; key ordering may shift (JSON has no
// ordering contract), and integer-valued numbers ride through float64
// — chat-completion params are all well within float64 precision.
func EnsureIncludeUsageBody(body []byte) ([]byte, bool) {
	if len(body) == 0 {
		return nil, false
	}
	var obj map[string]any
	if err := jsonx.Unmarshal(body, &obj); err != nil {
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
	out, err := jsonx.Marshal(obj)
	if err != nil {
		// Should never happen for a successfully-decoded map.
		return nil, false
	}
	return out, true
}

// ensureStreamUsage is the registered Provider.EnsureStreamUsage hook:
// the include_usage rewrite gated to the chat-completions path.
func ensureStreamUsage(path string, body []byte) ([]byte, bool) {
	if !strings.HasPrefix(path, "/v1/chat/completions") {
		return nil, false
	}
	return EnsureIncludeUsageBody(body)
}
