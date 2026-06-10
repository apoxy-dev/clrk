package parsers

import (
	"bytes"

	"github.com/apoxy-dev/clrk/internal/extproc/jsonx"
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
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

// EnsureIncludeUsage rewrites a streamed request body so the response
// always emits terminal usage, when the provider serving host needs an
// opt-in for that (OpenAI's stream_options.include_usage). Returns
// (newBody, true) when the body was mutated and (nil, false) otherwise.
//
// Without the opt-in, streamed responses produce zero tokens at clrk's
// parser layer — silently breaking TokenBudget enforcement and cost
// attribution on the dominant traffic shape (most agent SDKs stream by
// default for TTFB). Forcing the flag turns "caller-config gap" into
// "always instrumented".
//
// The rewrite itself is provider-owned (Provider.EnsureStreamUsage);
// this is the host-keyed shim. Caller emits the mutated bytes via
// ext_proc BodyMutation; Envoy recomputes content-length on the wire.
func EnsureIncludeUsage(host, path string, body []byte) ([]byte, bool) {
	return EnsureIncludeUsageForSystem(SystemFor(host), path, body)
}

// EnsureIncludeUsageForSystem is EnsureIncludeUsage keyed on a canonical
// gen_ai.system value rather than the request's :authority host. Used
// when ext_proc re-selected a backend whose wire schema is known
// directly (the original host is no longer where the request goes).
func EnsureIncludeUsageForSystem(system, path string, body []byte) ([]byte, bool) {
	p := llmcall.ByName(system)
	if p == nil || p.EnsureStreamUsage == nil {
		return nil, false
	}
	if !BodyAdvertisesStream(body) {
		// Cheap byte probe before the hook's full JSON decode — this
		// shim runs on every request-body EOS.
		return nil, false
	}
	return p.EnsureStreamUsage(path, body)
}

// RequestModel decodes the top-level "model" string from a JSON request
// body (the OpenAI/Anthropic request shape). Returns "" when the body is
// not a decodable JSON object or carries no string model — e.g. Google's
// native surface puts the model in the URL path, not the body.
func RequestModel(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var obj map[string]any
	if err := jsonx.Unmarshal(body, &obj); err != nil {
		return ""
	}
	m, _ := obj["model"].(string)
	return m
}

// RewriteModel sets the top-level "model" field of a JSON request body to
// `to` and returns the re-serialized body. Returns (nil, false) when the
// body is not a JSON object or has no "model" field to replace — we don't
// invent a model field the upstream didn't expect. JSON roundtrip caveats
// match rewriteIncludeUsage (key order may shift; numbers ride float64).
func RewriteModel(body []byte, to string) ([]byte, bool) {
	if len(body) == 0 || to == "" {
		return nil, false
	}
	var obj map[string]any
	if err := jsonx.Unmarshal(body, &obj); err != nil || obj == nil {
		return nil, false
	}
	if _, ok := obj["model"]; !ok {
		return nil, false
	}
	obj["model"] = to
	out, err := jsonx.Marshal(obj)
	if err != nil {
		return nil, false
	}
	return out, true
}
