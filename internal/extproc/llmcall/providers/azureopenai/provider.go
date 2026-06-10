// Package azureopenai is the llmcall provider plugin for Azure OpenAI
// (*.openai.azure.com). The wire schema is OpenAI's Chat Completions —
// codec, stream codec, and usage rewrite all delegate to the openai
// package — wrapped in Azure's envelope: deployment-in-path, api-key
// auth, api-version query.
package azureopenai

import (
	"strings"

	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall/providers/openai"
)

func init() {
	llmcall.Register(llmcall.Provider{
		Name:    "azure_openai",
		Aliases: []string{"azure-openai"},
		// Per-resource hosts (<resource>.openai.azure.com) — the
		// leading dot keeps lookalike registrations
		// (evilopenai.azure.com) out.
		MatchHost: func(host string) bool {
			return strings.HasSuffix(host, ".openai.azure.com")
		},
		Telemetry:   telemetryParser{},
		Codec:       newCodec(),
		StreamCodec: openai.NewChatStreamCodec("azure_openai"),
		EnsureStreamUsage: func(path string, body []byte) ([]byte, bool) {
			if _, _, ok := azureChatPath(path); !ok {
				return nil, false
			}
			return openai.EnsureIncludeUsageBody(body)
		},
		AuthHeaders: []string{"api-key", "authorization"},
		ErrorBody: func(msg string) []byte {
			return []byte(`{"error":{"message":` + llmcall.JSONString(msg) + `,"type":"server_error"}}`)
		},
		// Capabilities are honest to the codec: the delegated body
		// schema matches openai's, but only the chat surface is
		// modeled (the deployment path parse covers nothing else).
		Capabilities: llmcall.Capabilities{
			Operations:        []string{"chat"},
			Tools:             true,
			ParallelToolCalls: true,
			ImageInput:        true,
			StructuredOutput:  true,
			Streaming:         true,
			SystemRole:        true,
		},
	})
}
