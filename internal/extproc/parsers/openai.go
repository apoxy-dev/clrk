package parsers

import (
	"encoding/json"
	"strings"
)

// openaiParser implements Parser for api.openai.com.
//
// Request shape (POST /v1/chat/completions):
//
//	{ "model": "gpt-4o", "messages": [...], ... }
//
// Response shape (200, non-stream):
//
//	{ "id": "chatcmpl-...", "model": "gpt-4o-2024-08-06",
//	  "usage": { "prompt_tokens": 12, "completion_tokens": 34, "total_tokens": 46 },
//	  ... }
//
// On 4xx the error response is `{"error": {...}}` with no model and
// no usage; we still record the parser's System so the OTLP record is
// tagged as openai.
//
// SSE responses (`stream: true`, content-type: text/event-stream) ship
// usage in the terminal chunk only when the client opts in via
// `stream_options.include_usage=true` — too dependent on caller config
// for MVP, so we just flag StreamResponse and skip.
type openaiParser struct{}

type openaiRequest struct {
	Model string `json:"model"`
}

type openaiUsage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
}

type openaiResponse struct {
	Model string      `json:"model"`
	Usage openaiUsage `json:"usage"`
}

func (openaiParser) Parse(in Input) *ProviderInfo {
	info := &ProviderInfo{
		System:    "openai",
		Operation: openaiOperation(in.Path),
	}

	if model := decodeOpenAIModel(in.ReqBody); model != "" {
		info.RequestModel = model
	}

	if hasContentTypePrefix(in.RespHeaders, "text/event-stream") {
		info.StreamResponse = true
		return info
	}

	if len(in.RespBody) == 0 {
		return info
	}

	var resp openaiResponse
	if err := json.Unmarshal(in.RespBody, &resp); err != nil {
		return info
	}
	info.ResponseModel = resp.Model
	info.InputTokens = resp.Usage.PromptTokens
	info.OutputTokens = resp.Usage.CompletionTokens
	info.UsageVisible = resp.Usage.PromptTokens > 0 || resp.Usage.CompletionTokens > 0 || resp.Model != ""
	return info
}

func openaiOperation(path string) string {
	switch {
	case strings.HasPrefix(path, "/v1/chat/completions"):
		return "chat"
	case strings.HasPrefix(path, "/v1/completions"):
		return "text_completion"
	case strings.HasPrefix(path, "/v1/embeddings"):
		return "embeddings"
	default:
		return ""
	}
}

func decodeOpenAIModel(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var r openaiRequest
	if err := json.Unmarshal(body, &r); err != nil {
		return ""
	}
	return r.Model
}
