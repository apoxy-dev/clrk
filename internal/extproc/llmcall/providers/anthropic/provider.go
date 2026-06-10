// Package anthropic is the llmcall provider plugin for
// api.anthropic.com (the Anthropic Messages API).
package anthropic

import (
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
)

func init() {
	llmcall.Register(llmcall.Provider{
		Name:      "anthropic",
		Hosts:     []string{"api.anthropic.com"},
		Telemetry: telemetryParser{},
		Codec:     codec{},
		// Capabilities are honest to the codec, not the API surface:
		// the Messages API accepts document blocks, but the codec does
		// not model them, so file-bearing requests must not be
		// translated here.
		Capabilities: llmcall.Capabilities{
			Operations:        []string{"chat", "text_completion"},
			Tools:             true,
			ParallelToolCalls: true,
			ImageInput:        true,
			Reasoning:         true,
			Streaming:         true,
			SystemRole:        true,
		},
	})
}
