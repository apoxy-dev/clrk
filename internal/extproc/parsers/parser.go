// Package parsers extracts AI-provider-specific facts from buffered
// HTTP request/response pairs captured by ext_proc.
//
// Provider knowledge lives in the llmcall registry: each provider is a
// self-registering package under internal/extproc/llmcall/providers/,
// and this package is a thin shim over the registry's lookups. The
// blank import of providers/all below is load-bearing — it is what
// registers the built-in providers in every binary that links this
// package.
//
// Provider selection is host-based: For(host) returns the parser whose
// registration matches; nil when the host isn't a known AI provider.
// Hosts the registry doesn't recognize but that an AIProviderRoute
// targets via provider="custom" + endpoint match are deliberately not
// handled here — the route matcher tags those records with the route
// name but no parser fires.
//
// The MCP JSON-RPC parser (mcp.go) and the request-body rewrite
// helpers (rewrite.go) also live here; they are transaction parsing,
// not LLM provider plugins.
package parsers

import (
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
	// Register the built-in providers. See the package comment.
	_ "github.com/apoxy-dev/clrk/internal/extproc/llmcall/providers/all"
)

// ProviderInfo, Input, and Parser are type aliases so existing callers
// (and the structs embedding them) keep identical type identity while
// the definitions live with the registry the providers implement
// against.
type (
	ProviderInfo = llmcall.ProviderInfo
	Input        = llmcall.Input
	Parser       = llmcall.Parser
)

// Canonical normalizes a CRD-supplied provider name to the canonical
// gen_ai.system value used everywhere downstream (route matching,
// span attributes). Unknown names pass through verbatim — `custom`
// has special semantics in the route matcher and a typo is better
// surfaced as a non-matching rule than silently rewritten.
func Canonical(name string) string {
	return llmcall.Canonical(name)
}

// For returns the parser for host (the request's :authority host
// portion, port stripped). Returns nil when host doesn't match a
// known provider.
func For(host string) Parser {
	if p := llmcall.ByHost(host); p != nil {
		return p.Telemetry
	}
	return nil
}

// SystemFor returns the canonical gen_ai.system name for host without
// invoking the body parser. Used by callers that need the provider tag
// before the response is buffered (e.g. pre-flight budget checks).
// Returns "" when host doesn't map to a known provider.
func SystemFor(host string) string {
	if p := llmcall.ByHost(host); p != nil {
		return p.Name
	}
	return ""
}

// ForSchema returns the parser for a canonical gen_ai.system value
// (e.g. "anthropic", "openai", "google_genai") without a host lookup.
// Used when the response must be parsed against a backend's declared
// wire schema rather than the request's original :authority host — i.e.
// when ext_proc re-selected a different backend. Returns nil when no
// registered provider speaks that schema (e.g. azure_openai,
// aws_bedrock have no provider package yet), in which case usage simply
// isn't parsed, the same as for any unrecognized host.
func ForSchema(system string) Parser {
	if p := llmcall.ByName(system); p != nil && p.Telemetry != nil {
		return p.Telemetry
	}
	return nil
}
