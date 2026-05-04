package parsers

import (
	"encoding/json"
	"strings"
)

// googleParser implements Parser for generativelanguage.googleapis.com
// (Google's consumer Gemini API; "google_genai" in OTel gen_ai semconv).
//
// Two wire formats share this host:
//
//  1. Native Gemini under /v1beta/models/{model}:generateContent —
//     model encoded in path, response carries usageMetadata with
//     promptTokenCount / candidatesTokenCount.
//  2. OpenAI-compatibility layer under /v1beta/openai/* (e.g.
//     /v1beta/openai/chat/completions) — request/response are pure
//     OpenAI shape (model in body, usage with prompt_tokens /
//     completion_tokens). We delegate body parsing to the same
//     openaiResponse decoder openaiParser uses but stamp
//     gen_ai.system=google_genai because the upstream is still Google;
//     dashboards should group by upstream, not by wire format.
//
// Vertex AI (regional `*-aiplatform.googleapis.com`, `vertex_ai`) is a
// separate API surface — when we add a Vertex parser it ships under a
// different host matcher and a different gen_ai.system value.
//
// Native request shape (POST /v1beta/models/{model}:generateContent):
//
//	{ "contents": [...], "generationConfig": {...} }
//
// Native response shape (200, non-stream):
//
//	{ "candidates": [...],
//	  "modelVersion": "gemini-1.5-pro-002",
//	  "usageMetadata": {
//	    "promptTokenCount": 12,
//	    "candidatesTokenCount": 34,
//	    "totalTokenCount": 46 } }
//
// Streaming endpoint :streamGenerateContent ships either SSE
// (text/event-stream) or NDJSON (application/x-ndjson) depending on
// the ?alt= query param. Usage lands in the final chunk; cross-chunk
// reassembly is out of scope for MVP, same call as Anthropic/OpenAI.
type googleParser struct{}

type googleUsageMetadata struct {
	PromptTokenCount     int64 `json:"promptTokenCount"`
	CandidatesTokenCount int64 `json:"candidatesTokenCount"`
}

type googleResponse struct {
	ModelVersion  string              `json:"modelVersion"`
	UsageMetadata googleUsageMetadata `json:"usageMetadata"`
}

func (googleParser) Parse(in Input) *ProviderInfo {
	if isGoogleOpenAICompatPath(in.Path) {
		info := &ProviderInfo{
			System:    "google_genai",
			Operation: googleOpenAICompatOperation(in.Path),
		}
		parseOpenAIShape(in, info)
		return info
	}

	model, op := googlePathInfo(in.Path)
	info := &ProviderInfo{
		System:       "google_genai",
		Operation:    op,
		RequestModel: model,
	}

	if hasContentTypePrefix(in.RespHeaders, "text/event-stream") ||
		hasContentTypePrefix(in.RespHeaders, "application/x-ndjson") {
		info.StreamResponse = true
		return info
	}

	if len(in.RespBody) == 0 {
		return info
	}

	var resp googleResponse
	if err := json.Unmarshal(in.RespBody, &resp); err != nil {
		return info
	}
	info.ResponseModel = resp.ModelVersion
	info.InputTokens = resp.UsageMetadata.PromptTokenCount
	info.OutputTokens = resp.UsageMetadata.CandidatesTokenCount
	info.UsageVisible = resp.UsageMetadata.PromptTokenCount > 0 ||
		resp.UsageMetadata.CandidatesTokenCount > 0 ||
		resp.ModelVersion != ""
	return info
}

// isGoogleOpenAICompatPath reports whether path targets Gemini's
// OpenAI-compatibility layer (/v1beta/openai/* or /v1/openai/*). The
// guard at the top of Parse uses this rather than relying on
// googleOpenAICompatOperation returning "" — an unclassified compat
// path (e.g. /v1beta/openai/models) must NOT fall through to native
// Gemini extraction, which would try to read a Gemini-shaped body
// and stamp a meaningless model from the URL.
func isGoogleOpenAICompatPath(path string) bool {
	return strings.Contains(path, "/openai/")
}

// googleOpenAICompatOperation classifies a Gemini OpenAI-compat path
// into a gen_ai.operation.name. Returns "" for unclassified compat
// endpoints (e.g. /openai/models — listing, not a generation op);
// the caller still treats the request as compat-shape based on
// isGoogleOpenAICompatPath, just without an Operation tag.
func googleOpenAICompatOperation(path string) string {
	_, after, ok := strings.Cut(path, "/openai/")
	if !ok {
		return ""
	}
	switch {
	case strings.HasPrefix(after, "chat/completions"):
		return "chat"
	case strings.HasPrefix(after, "embeddings"):
		return "embeddings"
	case strings.HasPrefix(after, "completions"):
		return "text_completion"
	default:
		return ""
	}
}

// googlePathInfo extracts (model, operation) from a Gemini API path.
// Examples:
//
//	/v1beta/models/gemini-1.5-pro:generateContent       → ("gemini-1.5-pro", "chat")
//	/v1/models/gemini-2.0-flash:streamGenerateContent   → ("gemini-2.0-flash", "chat")
//	/v1beta/models/text-embedding-004:embedContent      → ("text-embedding-004", "embeddings")
//	/v1beta/models/gemini-1.5-pro:countTokens           → ("gemini-1.5-pro", "")
//
// Empty (model, operation) when the path doesn't fit the schema.
func googlePathInfo(path string) (string, string) {
	_, after, ok := strings.Cut(path, "/models/")
	if !ok {
		return "", ""
	}
	model, method, ok := strings.Cut(after, ":")
	if !ok {
		// e.g. GET /v1beta/models/gemini-1.5-pro — model listing,
		// not a generation op.
		return model, ""
	}
	if q := strings.IndexAny(method, "?#"); q >= 0 {
		method = method[:q]
	}
	switch method {
	case "generateContent", "streamGenerateContent":
		return model, "chat"
	case "embedContent", "batchEmbedContents":
		return model, "embeddings"
	default:
		return model, ""
	}
}
