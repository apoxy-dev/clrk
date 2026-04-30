package parsers

import (
	"encoding/json"
	"strings"
)

// anthropicParser implements Parser for api.anthropic.com.
//
// Request shape (POST /v1/messages):
//
//	{ "model": "claude-3-5-sonnet-20241022", "messages": [...], ... }
//
// Response shape (200, non-stream):
//
//	{ "id": "msg_...", "model": "claude-3-5-sonnet-20241022",
//	  "usage": { "input_tokens": 12, "output_tokens": 34 }, ... }
//
// SSE responses (`stream: true` request, content-type:
// text/event-stream) report usage in the terminal `message_delta`
// event; we don't reassemble SSE in MVP.
type anthropicParser struct{}

// anthropicUsage is the slim subset we read off response bodies.
type anthropicUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

type anthropicRequest struct {
	Model string `json:"model"`
}

type anthropicResponse struct {
	Model string         `json:"model"`
	Usage anthropicUsage `json:"usage"`
}

func (anthropicParser) Parse(in Input) *ProviderInfo {
	info := &ProviderInfo{
		System:    "anthropic",
		Operation: anthropicOperation(in.Path),
	}

	if model := decodeAnthropicModel(in.ReqBody); model != "" {
		info.RequestModel = model
	}

	if hasContentTypePrefix(in.RespHeaders, "text/event-stream") {
		info.StreamResponse = true
		return info
	}

	if len(in.RespBody) == 0 {
		return info
	}

	var resp anthropicResponse
	if err := json.Unmarshal(in.RespBody, &resp); err != nil {
		// Truncation or non-JSON error body. Leave usage zero; the
		// OTLP sink decides whether to flag UsageVisible based on
		// truncation state.
		return info
	}
	info.ResponseModel = resp.Model
	info.InputTokens = resp.Usage.InputTokens
	info.OutputTokens = resp.Usage.OutputTokens
	// Anthropic always echoes usage on a non-stream success; treat
	// any successful unmarshal that produced non-zero usage OR a model
	// echo as proof the relevant region of the body was visible.
	info.UsageVisible = resp.Usage.InputTokens > 0 || resp.Usage.OutputTokens > 0 || resp.Model != ""
	return info
}

func anthropicOperation(path string) string {
	switch {
	case strings.HasPrefix(path, "/v1/messages"):
		return "chat"
	case strings.HasPrefix(path, "/v1/complete"):
		return "text_completion"
	default:
		return ""
	}
}

func decodeAnthropicModel(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var r anthropicRequest
	if err := json.Unmarshal(body, &r); err != nil {
		return ""
	}
	return r.Model
}
