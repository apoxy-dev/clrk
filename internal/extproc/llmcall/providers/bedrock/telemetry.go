package bedrock

import (
	"github.com/apoxy-dev/clrk/internal/extproc/jsonx"
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
)

// telemetryParser implements llmcall.Parser for Bedrock runtime hosts.
// The request model is path-borne (Converse bodies carry no model
// member) and responses don't echo it, so RequestModel comes from the
// path and ResponseModel stays empty. Event-stream responses are
// flagged streaming with no usage parse — the binary framing has no
// decoder yet (see the provider registration).
type telemetryParser struct{}

func (telemetryParser) Parse(in llmcall.Input) *llmcall.ProviderInfo {
	info := &llmcall.ProviderInfo{System: "aws_bedrock"}
	modelID, op, ok := bedrockPath(in.Path)
	if !ok || (op != "converse" && op != "converse-stream") {
		return info
	}
	info.Operation = "chat"
	info.RequestModel = modelID
	if llmcall.HasContentTypePrefix(in.RespHeaders, "application/vnd.amazon.eventstream") {
		info.StreamResponse = true
		return info
	}
	if in.RespTruncated || len(in.RespBody) == 0 {
		return info
	}
	var body struct {
		Usage struct {
			InputTokens  int64 `json:"inputTokens"`
			OutputTokens int64 `json:"outputTokens"`
		} `json:"usage"`
	}
	if err := jsonx.Unmarshal(in.RespBody, &body); err != nil {
		return info
	}
	info.InputTokens = body.Usage.InputTokens
	info.OutputTokens = body.Usage.OutputTokens
	info.UsageVisible = info.InputTokens > 0 || info.OutputTokens > 0
	return info
}
