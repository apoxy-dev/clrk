package parsers

import (
	"encoding/json"
	"strings"
)

// googleParser implements Parser for generativelanguage.googleapis.com
// (Google's consumer Gemini API; "google_genai" in OTel gen_ai semconv).
//
// Vertex AI (regional `*-aiplatform.googleapis.com`, `vertex_ai`) is a
// separate API surface — when we add a Vertex parser it ships under a
// different host matcher and a different gen_ai.system value.
//
// Request shape (POST /v1beta/models/{model}:generateContent):
//
//	{ "contents": [...], "generationConfig": {...} }
//
// The model name is in the URL path, not the body — Gemini differs
// from Anthropic/OpenAI here and we extract it from the path instead
// of decoding the request body.
//
// Response shape (200, non-stream):
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
