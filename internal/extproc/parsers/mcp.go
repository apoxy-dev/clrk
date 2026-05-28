package parsers

import (
	"encoding/json"
	"strings"

	"github.com/bytedance/sonic"
)

// JSON-RPC methods we read by name. Stable across all MCP spec
// versions (2024-11-05 → 2025-11-25) so they live next to the parser
// rather than in the route table.
const (
	MCPMethodToolsCall     = "tools/call"
	MCPMethodResourcesRead = "resources/read"
)

// MCPInfo is the parsed view of one MCP JSON-RPC 2.0 request envelope.
// All fields are best-effort: when the captured body is not a single
// JSON-RPC request (batch array, truncated, malformed) ParseRequest
// returns nil rather than a partial MCPInfo.
type MCPInfo struct {
	// Method is the JSON-RPC "method" string, e.g. "tools/call",
	// "tools/list", "resources/read". Always non-empty when the parser
	// returns a non-nil result.
	Method string

	// ToolName is set only when Method == "tools/call" — the value of
	// params.name. Empty for every other method.
	ToolName string

	// ResourceURI is set only when Method == "resources/read" — the
	// value of params.uri. Empty for every other method.
	ResourceURI string

	// ID is the stringified JSON-RPC request id (numeric or string).
	// Kept for log fidelity so operators can correlate denies with the
	// caller's view. Empty for notifications (no id field).
	ID string
}

// mcpEnvelope is the narrow shape we decode out of the JSON-RPC
// envelope. Only the four fields ParseRequest exposes are populated.
type mcpEnvelope struct {
	Method string `json:"method"`
	Params *struct {
		Name string `json:"name"`
		URI  string `json:"uri"`
	} `json:"params"`
	ID json.RawMessage `json:"id"`
}

// ParseRequest decodes an MCP JSON-RPC 2.0 envelope out of a buffered
// HTTP request body. The envelope is decoded into a narrow struct so
// only the four fields we expose touch the heap.
//
// Returns nil when:
//   - body is empty,
//   - body is a top-level JSON array (batch — MVP single-request only),
//   - the "method" field is missing or empty (or the JSON is invalid),
//   - the caller signals truncation (we won't gate policy on a partial
//     read of params.name).
//
// The four fields read here (method, params.name, params.uri, id) are
// stable across all MCP spec versions, so no per-version adapter is
// needed. When the parser grows to read fields that drift across
// versions (result content blocks, _meta, tool annotations, streamable
// transport framing) it will need adapters dispatched on the
// negotiated MCP-Protocol-Version header.
func ParseRequest(body []byte, truncated bool) *MCPInfo {
	if truncated || len(body) == 0 {
		return nil
	}
	// Top-level arrays are JSON-RPC batches. MVP rejects them so the
	// caller doesn't accidentally policy-gate the first request in a
	// batch while letting the rest through.
	if firstNonWhitespace(body) == '[' {
		return nil
	}
	var env mcpEnvelope
	if err := sonic.Unmarshal(body, &env); err != nil {
		return nil
	}
	if env.Method == "" {
		return nil
	}
	info := &MCPInfo{Method: env.Method}
	if env.Params != nil {
		switch env.Method {
		case MCPMethodToolsCall:
			info.ToolName = env.Params.Name
		case MCPMethodResourcesRead:
			info.ResourceURI = env.Params.URI
		}
	}
	if len(env.ID) > 0 {
		info.ID = unquoteJSONScalar(string(env.ID))
	}
	return info
}

// unquoteJSONScalar strips JSON string quotes for the string-id case
// while leaving numeric ids untouched. JSON-RPC ids are conventionally
// simple ASCII; if a client passes escaped characters we leave them
// in place rather than running a full JSON-string decoder for a
// log-attribution field.
func unquoteJSONScalar(raw string) string {
	raw = strings.TrimSpace(raw)
	if len(raw) >= 2 && raw[0] == '"' && raw[len(raw)-1] == '"' {
		return raw[1 : len(raw)-1]
	}
	return raw
}

func firstNonWhitespace(b []byte) byte {
	for _, c := range b {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			return c
		}
	}
	return 0
}
