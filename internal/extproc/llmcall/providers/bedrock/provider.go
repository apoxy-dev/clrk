// Package bedrock is the llmcall provider plugin for AWS Bedrock's
// Converse API (bedrock-runtime.<region>.amazonaws.com). The wire
// schema is Converse's content-block unions with the model carried in
// the path (/model/{modelId}/converse); authentication is SigV4
// request signing, injected by CredentialInjectionPolicy's
// ProviderAuth/AWSv4 target rather than a static header.
package bedrock

import (
	"strings"

	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
)

func init() {
	llmcall.Register(llmcall.Provider{
		Name: "aws_bedrock",
		// Migrated from the registry's pendingAliases — the CRD
		// spelling predates this plugin.
		Aliases:   []string{"bedrock"},
		MatchHost: matchBedrockHost,
		Telemetry: telemetryParser{},
		Codec:     codec{},
		// StreamCodec deliberately nil and Streaming false:
		// converse-stream responses are AWS binary event-stream framing
		// (prelude + CRCs), not SSE — the framing needs its own decoder
		// before streamed pairs can translate. The honest gate keeps
		// bedrock out of streamed-request candidate sets meanwhile.
		// The x-amz-* entries matter for translation AWAY from bedrock:
		// an agent running an AWS SDK self-signs, and its stale
		// signature material must shed with the credential.
		AuthHeaders: []string{"authorization", "x-amz-security-token", "x-amz-date", "x-amz-content-sha256"},
		ErrorBody: func(msg string) []byte {
			// The Smithy exception envelope Bedrock clients parse.
			return []byte(`{"message":` + llmcall.JSONString(msg) + `}`)
		},
		Capabilities: llmcall.Capabilities{
			Operations:        []string{"chat"},
			Tools:             true,
			ParallelToolCalls: true,
			ImageInput:        true,
			FileInput:         true,
			Reasoning:         true,
			Streaming:         false,
			SystemRole:        true,
		},
	})
}

// matchBedrockHost accepts the Bedrock runtime endpoint shapes:
//
//	bedrock-runtime.<region>.amazonaws.com
//	bedrock-runtime-fips.<region>.amazonaws.com
//	<vpce-id>.bedrock-runtime.<region>.vpce.amazonaws.com
//
// A label walk rather than a suffix match keeps lookalike hosts
// (evil-bedrock-runtime.us-east-1.amazonaws.com.attacker.io) out. The
// caller has already lowercased the host and stripped any port.
func matchBedrockHost(host string) bool {
	labels := strings.Split(host, ".")
	n := len(labels)
	if n < 4 || labels[n-1] != "com" || labels[n-2] != "amazonaws" {
		return false
	}
	switch {
	case n == 4 && (labels[0] == "bedrock-runtime" || labels[0] == "bedrock-runtime-fips"):
		return true
	case n == 6 && labels[1] == "bedrock-runtime" && labels[3] == "vpce" && labels[0] != "":
		return true
	}
	return false
}
