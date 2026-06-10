package azureopenai

import (
	"strings"

	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall/providers/openai"
)

// telemetryParser implements llmcall.Parser for *.openai.azure.com.
// Bodies are OpenAI-shaped (openai.ParseShape does the heavy lifting);
// the azure-specific parts are the path-classified operation and the
// deployment standing in for the request model when the body carries
// none.
type telemetryParser struct{}

func (telemetryParser) Parse(in llmcall.Input) *llmcall.ProviderInfo {
	info := &llmcall.ProviderInfo{
		System:    "azure_openai",
		Operation: azureOperation(in.Path),
	}
	openai.ParseShape(in, info)
	if info.RequestModel == "" {
		if dep, _, ok := azureChatPath(in.Path); ok {
			info.RequestModel = dep
		}
	}
	return info
}

func azureOperation(path string) string {
	p, _, _ := strings.Cut(path, "?")
	switch {
	case strings.HasSuffix(p, "/chat/completions"):
		return "chat"
	case strings.HasSuffix(p, "/completions"):
		return "text_completion"
	case strings.HasSuffix(p, "/embeddings"):
		return "embeddings"
	default:
		return ""
	}
}
