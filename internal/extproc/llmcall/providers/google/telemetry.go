package google

import (
	"strings"

	"github.com/apoxy-dev/clrk/internal/extproc/jsonx"
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall/providers/openai"
)

// telemetryParser implements llmcall.Parser for
// generativelanguage.googleapis.com.
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
//     OpenAI-shape decoder the openai provider uses but stamp
//     gen_ai.system=google_genai because the upstream is still Google;
//     dashboards should group by upstream, not by wire format.
//
// Vertex AI (regional `*-aiplatform.googleapis.com`, `vertex_ai`) is a
// separate API surface — when we add a Vertex provider it ships under a
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
// the ?alt= query param. usageMetadata in the final chunk is the
// cumulative count; we walk all chunks and use the last successful
// decode. Capture is keep-last-N for both shapes so the terminal
// chunk survives even when the stream is larger than the cap.
type telemetryParser struct{}

type googleUsageMetadata struct {
	PromptTokenCount     int64 `json:"promptTokenCount"`
	CandidatesTokenCount int64 `json:"candidatesTokenCount"`
}

type googleResponse struct {
	ModelVersion  string              `json:"modelVersion"`
	UsageMetadata googleUsageMetadata `json:"usageMetadata"`
}

func (telemetryParser) Parse(in llmcall.Input) *llmcall.ProviderInfo {
	if isGoogleOpenAICompatPath(in.Path) {
		info := &llmcall.ProviderInfo{
			System:    "google_genai",
			Operation: googleOpenAICompatOperation(in.Path),
		}
		openai.ParseShape(in, info)
		return info
	}

	model, op := googlePathInfo(in.Path)
	info := &llmcall.ProviderInfo{
		System:       "google_genai",
		Operation:    op,
		RequestModel: model,
	}

	if llmcall.HasContentTypePrefix(in.RespHeaders, "text/event-stream") {
		info.StreamResponse = true
		extractGoogleSSEUsage(in.RespBody, info)
		return info
	}
	if llmcall.HasContentTypePrefix(in.RespHeaders, "application/x-ndjson") {
		info.StreamResponse = true
		extractGoogleNDJSONUsage(in.RespBody, info)
		return info
	}

	if len(in.RespBody) == 0 {
		return info
	}

	var resp googleResponse
	if err := jsonx.Unmarshal(in.RespBody, &resp); err != nil {
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

func extractGoogleSSEUsage(body []byte, info *llmcall.ProviderInfo) {
	if len(body) == 0 {
		return
	}
	llmcall.ScanSSEData(body, func(payload []byte) {
		var resp googleResponse
		if err := jsonx.Unmarshal(payload, &resp); err != nil {
			return
		}
		applyGoogleResponse(resp, info)
	})
}

func extractGoogleNDJSONUsage(body []byte, info *llmcall.ProviderInfo) {
	line := llmcall.LastJSONLine(body)
	if line == nil {
		return
	}
	var resp googleResponse
	if err := jsonx.Unmarshal(line, &resp); err != nil {
		return
	}
	applyGoogleResponse(resp, info)
}

func applyGoogleResponse(resp googleResponse, info *llmcall.ProviderInfo) {
	if resp.ModelVersion != "" {
		info.ResponseModel = resp.ModelVersion
	}
	if resp.UsageMetadata.PromptTokenCount > 0 ||
		resp.UsageMetadata.CandidatesTokenCount > 0 {
		info.InputTokens = resp.UsageMetadata.PromptTokenCount
		info.OutputTokens = resp.UsageMetadata.CandidatesTokenCount
		info.UsageVisible = true
	}
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
