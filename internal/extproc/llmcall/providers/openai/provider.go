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
		// Capabilities are honest to the codec, not the API surface:
		// the Chat Completions API accepts input_audio and file parts,
		// but the codec models only text and image_url content, so
		// audio/file-bearing requests must not be translated here.
		Capabilities: llmcall.Capabilities{
			Operations:        []string{"chat", "text_completion", "embeddings"},
			Tools:             true,
			ParallelToolCalls: true,
			ImageInput:        true,
			StructuredOutput:  true,
			Streaming:         true,
			SystemRole:        true,
		},
	})
}
