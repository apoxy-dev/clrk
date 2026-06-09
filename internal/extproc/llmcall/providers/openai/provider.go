// Package openai is the llmcall provider plugin for api.openai.com
// (the OpenAI Chat Completions API surface).
package openai

import (
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
)

func init() {
	llmcall.Register(llmcall.Provider{
		Name:      "openai",
		Hosts:     []string{"api.openai.com"},
		Telemetry: telemetryParser{},
		Codec:     codec{},
		Capabilities: llmcall.Capabilities{
			Operations:        []string{"chat", "text_completion", "embeddings"},
			Tools:             true,
			ParallelToolCalls: true,
			ImageInput:        true,
			AudioInput:        true,
			FileInput:         true,
			StructuredOutput:  true,
			Streaming:         true,
			SystemRole:        true,
		},
	})
}
