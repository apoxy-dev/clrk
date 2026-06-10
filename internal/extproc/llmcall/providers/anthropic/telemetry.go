package anthropic

import (
	"strings"

	"github.com/apoxy-dev/clrk/internal/extproc/jsonx"
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
)

// telemetryParser implements llmcall.Parser for api.anthropic.com.
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
// text/event-stream) emit `input_tokens` once on the leading
// `message_start` event and `output_tokens` cumulatively on the
// terminal `message_delta` event. We scan all `data:` events in the
// captured tail and pick the last successful decode of each shape.
// Under aggressive keep-last-N truncation `message_start` may be
// evicted — in that case we report only `output_tokens` and rely on
// `UsageVisible` to indicate partial visibility.
type telemetryParser struct{}

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

func (telemetryParser) Parse(in llmcall.Input) *llmcall.ProviderInfo {
	info := &llmcall.ProviderInfo{
		System:    "anthropic",
		Operation: anthropicOperation(in.Path),
	}

	if model := decodeAnthropicModel(in.ReqBody); model != "" {
		info.RequestModel = model
	}

	if llmcall.HasContentTypePrefix(in.RespHeaders, "text/event-stream") {
		info.StreamResponse = true
		extractAnthropicSSEUsage(in.RespBody, info)
		return info
	}

	if len(in.RespBody) == 0 {
		return info
	}

	var resp anthropicResponse
	if err := jsonx.Unmarshal(in.RespBody, &resp); err != nil {
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

// anthropicSSEMessageStart is the shape of a `message_start` event
// payload — only the fields we read.
type anthropicSSEMessageStart struct {
	Type    string `json:"type"`
	Message struct {
		Model string         `json:"model"`
		Usage anthropicUsage `json:"usage"`
	} `json:"message"`
}

// anthropicSSEMessageDelta is the shape of a `message_delta` event
// payload. Anthropic emits the cumulative output_tokens here; the
// inner usage block has no input_tokens echo.
type anthropicSSEMessageDelta struct {
	Type  string         `json:"type"`
	Usage anthropicUsage `json:"usage"`
}

func extractAnthropicSSEUsage(body []byte, info *llmcall.ProviderInfo) {
	if len(body) == 0 {
		return
	}
	// Each SSE event carries a {"type": ...} discriminator; decode
	// once into that and dispatch. Avoids 2× json.Unmarshal per event
	// (one per shape) when streams have many non-usage events
	// (content_block_delta etc.).
	type anthropicSSEEnvelope struct {
		Type string `json:"type"`
	}
	llmcall.ScanSSEData(body, func(payload []byte) {
		var env anthropicSSEEnvelope
		if err := jsonx.Unmarshal(payload, &env); err != nil {
			return
		}
		switch env.Type {
		case "message_start":
			var ms anthropicSSEMessageStart
			if err := jsonx.Unmarshal(payload, &ms); err != nil {
				return
			}
			if ms.Message.Model != "" {
				info.ResponseModel = ms.Message.Model
			}
			if ms.Message.Usage.InputTokens > 0 {
				info.InputTokens = ms.Message.Usage.InputTokens
				info.UsageVisible = true
			}
			// message_start seeds output_tokens=1 (leading role token);
			// message_delta will overwrite cumulatively.
			if ms.Message.Usage.OutputTokens > 0 && info.OutputTokens == 0 {
				info.OutputTokens = ms.Message.Usage.OutputTokens
			}
		case "message_delta":
			var md anthropicSSEMessageDelta
			if err := jsonx.Unmarshal(payload, &md); err != nil {
				return
			}
			if md.Usage.OutputTokens > 0 {
				info.OutputTokens = md.Usage.OutputTokens
				info.UsageVisible = true
			}
		}
	})
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
	if err := jsonx.Unmarshal(body, &r); err != nil {
		return ""
	}
	return r.Model
}
