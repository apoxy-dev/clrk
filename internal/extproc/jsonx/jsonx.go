// Package jsonx pins the egress data path to a single JSON engine:
// bytedance/sonic in std-compatible mode. sonic's optimized engine
// covers amd64 and arm64 (go1.20-1.26; other platforms and newer
// toolchains transparently fall back to its encoding/json shim), and
// the llmcall corpus pins byte-identity, so any semantic drift from
// encoding/json surfaces as a golden diff.
//
// Two frozen profiles, one engine:
//
//   - Marshal/Unmarshal: full std compatibility (sorted map keys,
//     HTML escaping, copied strings) — drop-in for bare
//     encoding/json call sites.
//   - MarshalWire: std compatibility minus HTML escaping — provider
//     wire bytes. Providers emit <, >, & verbatim, so escaping them
//     would break preserve-mode byte-identity (see
//     llmcall.MarshalCompact).
//
// Deliberately NOT routed through sonic: json.Compact (byte-level
// whitespace strip, no value parse) and the order-preserving object
// token walk in llmcall.DecodeObject (sonic has no std-compatible
// streaming token API). json.RawMessage and json.Number remain the
// interchange types — sonic handles both natively.
package jsonx

import (
	"github.com/bytedance/sonic"
)

var (
	std  = sonic.ConfigStd
	wire = sonic.Config{
		SortMapKeys:      true,
		CompactMarshaler: true,
		CopyString:       true,
		ValidateString:   true,
	}.Froze()
)

// Unmarshal parses data into v with encoding/json semantics.
func Unmarshal(data []byte, v any) error {
	return std.Unmarshal(data, v)
}

// Marshal renders v with encoding/json semantics (sorted map keys,
// HTML-escaped <, >, &).
func Marshal(v any) ([]byte, error) {
	return std.Marshal(v)
}

// MarshalWire renders v compactly without HTML escaping — the
// provider-wire profile. Codecs must use this (via
// llmcall.MarshalCompact) instead of Marshal.
func MarshalWire(v any) ([]byte, error) {
	return wire.Marshal(v)
}
