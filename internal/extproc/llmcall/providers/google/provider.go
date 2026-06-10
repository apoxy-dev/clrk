// Package google is the llmcall provider plugin for
// generativelanguage.googleapis.com (Google's consumer Gemini API;
// "google_genai" in OTel gen_ai semconv).
package google

import (
	"strings"

	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall/providers/openai"
)

func init() {
	llmcall.Register(llmcall.Provider{
		Name:        "google_genai",
		Aliases:     []string{"google"},
		Hosts:       []string{"generativelanguage.googleapis.com"},
		Telemetry:   telemetryParser{},
		Codec:       codec{},
		StreamCodec: streamCodec{},
		// Gemini's native stream carries usageMetadata unconditionally;
		// the rewrite matters only on its OpenAI-compat surface, where
		// the wire shape (and so the rewrite) is OpenAI's.
		EnsureStreamUsage: func(path string, body []byte) ([]byte, bool) {
			if !strings.Contains(path, "/openai/chat/completions") {
				return nil, false
			}
			return openai.EnsureIncludeUsageBody(body)
		},
		AuthHeaders: []string{"x-goog-api-key", "authorization"},
		ErrorBody: func(msg string) []byte {
			return []byte(`{"error":{"code":502,"message":` + llmcall.JSONString(msg) + `,"status":"INTERNAL"}}`)
		},
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
