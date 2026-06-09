package openai

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
)

// telemetryParser implements llmcall.Parser for api.openai.com.
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
// `stream_options.include_usage=true`. We scan all `data:` events in
// the captured tail and use the LAST decode that carries a non-zero
// usage block — older events with `usage: null` (the default for
// non-terminal chunks) are ignored. Capture-side keeps the last N
// bytes for SSE so the terminal event survives even when the stream
// is larger than the cap.
type telemetryParser struct{}

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

func (telemetryParser) Parse(in llmcall.Input) *llmcall.ProviderInfo {
	info := &llmcall.ProviderInfo{
		System:    "openai",
		Operation: openaiOperation(in.Path),
	}
	ParseShape(in, info)
	return info
}

// ParseShape fills RequestModel + response fields from an
// OpenAI-shaped HTTP transaction. Used by this package's parser and by
// the google provider when the request hit Google's /v1beta/openai/*
// OAI-compat endpoint (same wire format, different upstream). This is
// wire-shape sharing, not cross-provider translation — the google
// package depends on the shape OpenAI defines, not on this package's
// registration. Caller pre-sets info.System and info.Operation.
func ParseShape(in llmcall.Input, info *llmcall.ProviderInfo) {
	if model := decodeOpenAIModel(in.ReqBody); model != "" {
		info.RequestModel = model
	}
	if llmcall.HasContentTypePrefix(in.RespHeaders, "text/event-stream") {
		info.StreamResponse = true
		extractOpenAISSEUsage(in.RespBody, info)
		return
	}
	if len(in.RespBody) == 0 {
		return
	}
	var resp openaiResponse
	if err := json.Unmarshal(in.RespBody, &resp); err != nil {
		return
	}
	info.ResponseModel = resp.Model
	info.InputTokens = resp.Usage.PromptTokens
	info.OutputTokens = resp.Usage.CompletionTokens
	info.UsageVisible = resp.Usage.PromptTokens > 0 || resp.Usage.CompletionTokens > 0 || resp.Model != ""
}

// extractOpenAISSEUsage walks `data:` events in body and fills the
// last successfully-decoded chunk's model + usage onto info. OpenAI
// streaming emits `usage: null` on every non-terminal chunk and the
// real numbers only when the caller passed
// stream_options.include_usage=true; ResponseModel is echoed on every
// chunk, so the loop tracks "last seen model" separately from "last
// seen non-empty usage."
func extractOpenAISSEUsage(body []byte, info *llmcall.ProviderInfo) {
	if len(body) == 0 {
		return
	}
	llmcall.ScanSSEData(body, func(payload []byte) {
		if bytes.Equal(bytes.TrimSpace(payload), []byte("[DONE]")) {
			return
		}
		var resp openaiResponse
		if err := json.Unmarshal(payload, &resp); err != nil {
			return
		}
		if resp.Model != "" {
			info.ResponseModel = resp.Model
		}
		if resp.Usage.PromptTokens > 0 || resp.Usage.CompletionTokens > 0 {
			info.InputTokens = resp.Usage.PromptTokens
			info.OutputTokens = resp.Usage.CompletionTokens
			info.UsageVisible = true
		}
	})
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
