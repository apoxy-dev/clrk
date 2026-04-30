// Package parsers extracts AI-provider-specific facts from buffered
// HTTP request/response pairs captured by ext_proc.
//
// Each Parser inspects the request URL + body and the response body for
// one provider family (Anthropic, OpenAI, etc.) and produces a
// ProviderInfo with the canonical gen_ai.* attributes the OTLP sinks
// publish. Parsers are stateless and safe for concurrent use.
//
// Provider selection is host-based: For(host) returns the parser whose
// table entry matches; nil when the host isn't a known AI provider.
// Hosts the table doesn't recognize but that an AIProviderRoute targets
// via provider="custom" + endpoint match are deliberately not handled
// here — the route matcher tags those records with the route name but
// no parser fires.
package parsers

import "strings"

// ProviderInfo is the parsed view of one HTTP transaction against a
// known AI provider. All fields are best-effort — when the captured
// body is truncated or in an unrecognized shape, fields stay at their
// zero values and the caller should treat them as absent.
type ProviderInfo struct {
	// System is the canonical OTel gen_ai.system value
	// (e.g. "anthropic", "openai", "azure_openai", "google_genai",
	// "aws_bedrock"). Always non-empty when the parser ran.
	System string

	// Operation is the gen_ai.operation.name value
	// ("chat", "text_completion", "embeddings", ...). Empty when the
	// parser couldn't classify the path.
	Operation string

	// RequestModel is the model the caller asked for. From the request
	// body's "model" field. Empty when the request body wasn't decodable
	// (truncation, non-JSON).
	RequestModel string

	// ResponseModel is the model the provider actually served (often
	// equal to RequestModel but can include a version suffix). Some
	// providers don't echo it for non-streaming responses.
	ResponseModel string

	// InputTokens / OutputTokens are the provider-reported usage
	// counts. Zero when not present (truncation, error response with no
	// usage block, streaming response with no usage block).
	InputTokens  int64
	OutputTokens int64

	// StreamResponse is true when the response advertised
	// content-type: text/event-stream. Token accounting is intentionally
	// skipped on streams in MVP — providers report usage inconsistently
	// across SSE chunks.
	StreamResponse bool

	// UsageVisible indicates that the response body contained the usage
	// block we look for. False when truncation hid it; lets the OTLP
	// sink emit clrk.body.usage_visible=false to nudge operators to
	// raise CaptureBody.MaxBytes.
	UsageVisible bool
}

// Input is the narrow shape passed to a parser. Avoids importing
// extproc.Record so the parser package stays a leaf in the dep graph.
type Input struct {
	Method       string
	Path         string
	ReqHeaders   map[string]string
	RespHeaders  map[string]string
	ReqBody      []byte
	ReqTruncated bool
	RespBody     []byte
	RespTruncated bool
}

// Parser fills a ProviderInfo from one captured transaction.
type Parser interface {
	Parse(in Input) *ProviderInfo
}

// hostMatch returns true when a captured request's :authority host
// portion belongs to this provider entry.
type hostMatch func(host string) bool

type providerEntry struct {
	match  hostMatch
	system string // canonical gen_ai.system name
	parser Parser
}

// hostProviders is the built-in provider table. Order doesn't matter —
// the matchers are disjoint by construction. Keep additions cheap and
// data-driven; one-off providers belong in their own parser file.
var hostProviders = []providerEntry{
	{match: equalsAny("api.anthropic.com"), system: "anthropic", parser: anthropicParser{}},
	{match: equalsAny("api.openai.com"), system: "openai", parser: openaiParser{}},
}

// For returns the parser for host (the request's :authority host
// portion, port stripped). Returns nil when host doesn't match a
// known provider.
func For(host string) Parser {
	if e := lookup(host); e != nil {
		return e.parser
	}
	return nil
}

// SystemFor returns the canonical gen_ai.system name for host without
// invoking the body parser. Used by callers that need the provider tag
// before the response is buffered (e.g. pre-flight budget checks).
// Returns "" when host doesn't map to a known provider.
func SystemFor(host string) string {
	if e := lookup(host); e != nil {
		return e.system
	}
	return ""
}

func lookup(host string) *providerEntry {
	if host == "" {
		return nil
	}
	host = strings.ToLower(host)
	for i := range hostProviders {
		if hostProviders[i].match(host) {
			return &hostProviders[i]
		}
	}
	return nil
}

func equalsAny(hosts ...string) hostMatch {
	return func(h string) bool {
		for _, want := range hosts {
			if h == want {
				return true
			}
		}
		return false
	}
}

// hasContentTypePrefix reports whether the headers' content-type starts
// with prefix (case-insensitive). Header keys in Input are pre-lowered
// by ext_proc's headersToMap.
func hasContentTypePrefix(headers map[string]string, prefix string) bool {
	ct := headers["content-type"]
	if ct == "" {
		return false
	}
	return strings.HasPrefix(strings.ToLower(ct), prefix)
}
