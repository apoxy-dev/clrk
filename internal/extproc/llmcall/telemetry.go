package llmcall

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

// Input is the narrow shape passed to a telemetry parser. Avoids
// importing extproc.Record so this package stays a leaf in the dep
// graph.
type Input struct {
	Method        string
	Path          string
	ReqHeaders    map[string]string
	RespHeaders   map[string]string
	ReqBody       []byte
	ReqTruncated  bool
	RespBody      []byte
	RespTruncated bool
}

// Parser fills a ProviderInfo from one captured transaction.
type Parser interface {
	Parse(in Input) *ProviderInfo
}

// HasContentTypePrefix reports whether the headers' content-type starts
// with prefix (case-insensitive). Header keys in Input are pre-lowered
// by ext_proc's headersToMap.
func HasContentTypePrefix(headers map[string]string, prefix string) bool {
	ct := headers["content-type"]
	if ct == "" {
		return false
	}
	return strings.HasPrefix(strings.ToLower(ct), prefix)
}
