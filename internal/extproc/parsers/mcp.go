package parsers

import (
	"encoding/json"
	"strings"

	"github.com/apoxy-dev/clrk/internal/extproc/jsonx"
)

// JSON-RPC methods we read by name. Stable across all MCP spec
// versions (2024-11-05 → 2025-11-25) so they live next to the parser
// rather than in the route table.
const (
	MCPMethodToolsCall     = "tools/call"
	MCPMethodResourcesRead = "resources/read"
)

// headerMCPProtocolVersion is the request header an MCP client sends on
// every HTTP request after the initialize handshake (MCP spec
// 2025-06-18+) to carry the negotiated protocol version. Lower-cased
// because Input.ReqHeaders keys are pre-lowered by ext_proc's
// headersToMap.
const headerMCPProtocolVersion = "mcp-protocol-version"

// MCPInfo is the parsed view of one MCP JSON-RPC 2.0 exchange. The
// request-side fields (Method, ToolName, ResourceURI, ID, ProtocolVersion)
// are filled by ParseRequest; the response fields (IsError, ErrorCode) are
// merged in at emit time from ParseResponse, keyed to the same HTTP
// transaction. All fields are best-effort: when the captured request
// body is not a single JSON-RPC request (batch array, truncated,
// malformed) ParseRequest returns nil rather than a partial MCPInfo.
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

	// ProtocolVersion is the negotiated MCP protocol version, read from
	// the MCP-Protocol-Version request header (e.g. "2025-06-18"). Clients
	// send it on every request after the initialize handshake; empty for
	// the initialize request itself and for pre-2025-06-18 clients that
	// predate the header. Attribution only — it does not affect parsing.
	ProtocolVersion string

	// IsError is true when the MCP JSON-RPC response envelope carried a
	// top-level "error" object. Set by ParseResponse during emit-time
	// augmentation; never set by ParseRequest.
	IsError bool

	// ErrorCode is the JSON-RPC error.code from the response envelope.
	// Meaningful only when IsError is true.
	ErrorCode int
}

// MCPResult is the parsed view of one MCP JSON-RPC 2.0 response
// envelope. ParseResponse returns it so the caller can merge the
// response-side facts onto the request's MCPInfo for the same
// transaction.
type MCPResult struct {
	// IsError is true when the response carried a top-level "error".
	IsError bool
	// ErrorCode is error.code; meaningful only when IsError is true.
	ErrorCode int
}

// mcpEnvelope is the narrow shape we decode out of the JSON-RPC request
// envelope. Only the four fields ParseRequest exposes are populated.
type mcpEnvelope struct {
	Method string `json:"method"`
	Params *struct {
		Name string `json:"name"`
		URI  string `json:"uri"`
	} `json:"params"`
	ID json.RawMessage `json:"id"`
}

// mcpResponseEnvelope is the narrow shape decoded from a JSON-RPC
// response. We read only error.code; the result content and error
// message ride on the captured body span event rather than being lifted
// into attributes (the message can carry caller-supplied text).
type mcpResponseEnvelope struct {
	Error *struct {
		Code int `json:"code"`
	} `json:"error"`
}

// ParseRequest decodes an MCP JSON-RPC 2.0 envelope out of a buffered
// HTTP request body. The envelope is decoded into a narrow struct so
// only the four fields we expose touch the heap.
//
// Returns nil when:
//   - in.ReqBody is empty,
//   - in.ReqBody is a top-level JSON array (batch — single-request only;
//     the route table fails such bodies closed under a ToolPolicy),
//   - the "method" field is missing or empty (or the JSON is invalid),
//   - in.ReqTruncated is set (we won't gate policy on a partial read of
//     params.name).
//
// The four envelope fields read here (method, params.name, params.uri,
// id) are stable across all MCP spec versions, so no per-version adapter
// is needed to parse them. The negotiated protocol version is read from
// the MCP-Protocol-Version request header and surfaced as
// ProtocolVersion for attribution; dispatching the parse itself on that
// version — result content blocks, _meta, tool annotations, streamable
// transport framing — remains a future adapter the Input seam already
// accommodates without another signature change.
func ParseRequest(in Input) *MCPInfo {
	body := in.ReqBody
	if in.ReqTruncated || len(body) == 0 {
		return nil
	}
	// Top-level arrays are JSON-RPC batches. We reject them so the
	// caller doesn't accidentally policy-gate the first request in a
	// batch while letting the rest through (see IsBatch + the route
	// table's fail-closed gate).
	if firstNonWhitespace(body) == '[' {
		return nil
	}
	var env mcpEnvelope
	if err := jsonx.Unmarshal(body, &env); err != nil {
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
	// The negotiated protocol version rides on a request header, not the
	// JSON-RPC body. Absent on the initialize request and on clients that
	// predate the header (MCP spec 2024-11-05); left empty in both cases.
	if v := in.ReqHeaders[headerMCPProtocolVersion]; v != "" {
		info.ProtocolVersion = v
	}
	return info
}

// ParseResponse extracts the JSON-RPC error facts from a buffered MCP
// response body. Returns nil when the body is empty, truncated, or not a
// single JSON-RPC response object. A single response is always a
// top-level object, so anything that doesn't open with '{' — a batch
// array, an SSE "data: {...}" frame from a streamed MCP transport, or
// non-JSON — is out of scope (SSE-mode transport error attribution is
// deferred). On a successful response (no top-level "error") the result
// is non-nil with IsError false.
func ParseResponse(in Input) *MCPResult {
	body := in.RespBody
	if in.RespTruncated || len(body) == 0 {
		return nil
	}
	if firstNonWhitespace(body) != '{' {
		return nil
	}
	var env mcpResponseEnvelope
	if err := jsonx.Unmarshal(body, &env); err != nil {
		return nil
	}
	if env.Error == nil {
		return &MCPResult{}
	}
	return &MCPResult{IsError: true, ErrorCode: env.Error.Code}
}

// IsBatch reports whether body is a JSON-RPC batch (a top-level array).
// The route table uses it to fail closed on batches under a ToolPolicy:
// the parser is single-request only, so a batch's tools/call entries
// can't be authorized individually. A truncated body is still
// classified by its first byte — capture keeps the leading bytes, so a
// leading '[' is reliable even when the tail was dropped.
func IsBatch(body []byte) bool {
	return firstNonWhitespace(body) == '['
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
