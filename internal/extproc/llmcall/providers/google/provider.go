// Package google is the llmcall provider plugin for
// generativelanguage.googleapis.com (Google's consumer Gemini API;
// "google_genai" in OTel gen_ai semconv).
package google

import (
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
)

func init() {
	llmcall.Register(llmcall.Provider{
		Name:      "google_genai",
		Aliases:   []string{"google"},
		Hosts:     []string{"generativelanguage.googleapis.com"},
		Telemetry: telemetryParser{},
		Codec:     codec{},
		Capabilities: llmcall.Capabilities{
			Operations:        []string{"chat", "embeddings"},
			Tools:             true,
			ParallelToolCalls: true,
			ImageInput:        true,
			AudioInput:        true,
			FileInput:         true,
			Reasoning:         true,
			StructuredOutput:  true,
			Streaming:         true,
			SystemRole:        true,
		},
	})
}
