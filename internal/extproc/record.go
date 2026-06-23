package extproc

import (
	"net"
	"strings"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/protobuf/types/known/structpb"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
	"github.com/apoxy-dev/clrk/internal/extproc/parsers"
)

// MetadataNamespace is the dynamic-metadata key carrying clrk PROXY v2 TLV
// values decoded by the proxy_protocol listener filter. Keep in sync with
// internal/egextension when it programs listener-filter rules.
const MetadataNamespace = "clrk.apoxy.dev"

// Metadata sub-keys under MetadataNamespace.
const (
	MetaAgentKind      = "agent_kind"
	MetaAgentNamespace = "agent_namespace"
	MetaAgentName      = "agent_name"
	MetaAgentUID       = "agent_uid"
	MetaAgentRevision  = "agent_revision"
	MetaInvocationID   = "invocation_id"

	// MetaDstName carries the DNS-bound destination hostname for the
	// connection — the name the sandbox's resolver answered for the
	// dst IP. Worker emits it via PROXY v2 TLVDstName; the listener
	// filter publishes it under this key. Empty when no binding
	// existed at dial time (direct-IP, DNS bypass, expired entry).
	MetaDstName = "dst_name"
)

// Record is the structured representation of one HTTP request/response
// observed through ext_proc. All fields are best-effort; Envoy may not
// deliver bodies or trailers depending on mode.
//
// Timestamps mark the wall-clock arrival of each ext_proc callback so
// the trace sink can synthesize span events with realistic timings.
// They are zero when the corresponding phase did not occur (e.g. an
// empty-body request leaves RequestBodyAt zero).
type Record struct {
	Timestamp time.Time
	EndAt     time.Time

	AgentKind      string
	AgentNamespace string
	AgentName      string
	AgentUID       string
	AgentRevision  string
	InvocationID   string

	RequestHeadersAt time.Time
	RequestHeaders   map[string]string
	RequestBodyAt    time.Time
	RequestBody      []byte
	RequestTruncated bool

	ResponseHeadersAt time.Time
	ResponseHeaders   map[string]string
	ResponseBodyAt    time.Time
	ResponseBody      []byte
	ResponseTruncated bool

	// RequestBodyDecoded / ResponseBodyDecoded carry the content-encoding-
	// decoded copy of the body when enrichRecord inflated it (agents send
	// `accept-encoding: gzip, ...` and the MITM forwards it, so providers
	// gzip/br/zstd even SSE bodies). The OTLP body span event ships these
	// so the inspector renders readable JSON instead of a compressed blob.
	// Nil when the body was identity-encoded, truncated (a header-less
	// keep-last-N tail no codec can inflate), or failed to inflate -- the
	// sink then falls back to the raw RequestBody/ResponseBody. The raw
	// bytes are never overwritten, so clrk.req.bytes / clrk.resp.bytes stay
	// true on-wire counts.
	//
	// RequestContentEncoding / ResponseContentEncoding name the non-identity
	// wire encoding that was present (e.g. "gzip"), set whenever the body
	// carried one regardless of whether it inflated, so the sink can label a
	// decoded body as "was gzipped" and a truncated/undecodable compressed
	// body as not decodable.
	RequestBodyDecoded      []byte
	RequestContentEncoding  string
	ResponseBodyDecoded     []byte
	ResponseContentEncoding string
	// ResponseBodyChunks counts ResponseBody ProcessingRequest messages
	// received from Envoy. Under ResponseBodyMode=BUFFERED this is 1
	// (or 0 if the upstream produced no body); under STREAMED it
	// reflects per-chunk delivery. Surfaced on OTLP so operators (and
	// integration tests) can confirm the conditional STREAMED override
	// promoted the mode for streaming traffic.
	ResponseBodyChunks int

	// RequestBodyRewritten is true when ext_proc replaced the request
	// body before forwarding (e.g. forcing OpenAI
	// stream_options.include_usage=true so streamed responses always
	// emit terminal usage). Surfaced on the OTLP record as
	// clrk.body.request_rewritten so operators can correlate
	// upstream-observed bodies with what the agent actually sent.
	RequestBodyRewritten bool

	// Provider holds parsed AI-provider facts (gen_ai.* shape) when the
	// request's :authority host matched a known provider. Nil for any
	// other host.
	Provider *parsers.ProviderInfo

	// MatchedRouteNamespace / MatchedRouteName identify the
	// AIProviderRoute that accepted this transaction; empty when no
	// route attached to the calling EG matched.
	MatchedRouteNamespace string
	MatchedRouteName      string

	// BudgetDenied is set when the matched route's TokenBudget caused
	// us to short-circuit the request with an HTTP 429. The captured
	// record will only have RequestHeaders populated in that case
	// (response phases never ran).
	BudgetDenied bool
	// BudgetDailyUsed / BudgetDailyMax are the counter snapshot at the
	// moment of the deny decision, attached for operator visibility.
	BudgetDailyUsed int64
	BudgetDailyMax  int64

	// MCP carries parsed MCP JSON-RPC envelope facts when the
	// :authority host fell under an attached MCPRoute and the request
	// body was a single (non-batch) JSON-RPC request. Nil for any
	// other traffic.
	MCP *parsers.MCPInfo

	// MatchedMCPRouteNamespace / MatchedMCPRouteName identify the
	// MCPRoute whose rule accepted this transaction; empty when no
	// rule matched.
	MatchedMCPRouteNamespace string
	MatchedMCPRouteName      string

	// MCPToolPolicyDecision is "allow" or "deny" when the matched
	// rule's ToolPolicy fired on a tools/call request; empty for
	// non-tools/call methods, no-policy rules, or unmatched traffic.
	MCPToolPolicyDecision string

	// SelectedBackendNamespace / SelectedBackendName identify the clrk
	// Backend this transaction was re-pointed to at RequestBody EOS.
	// SelectedBackendSchema is that backend's canonical gen_ai.system
	// (the wire schema the response was parsed against).
	// SelectedBackendReselected is true only when re-selection actually
	// fired. All four stay zero for single-backend / non-reselectable
	// routes, so OTLP attributes are unchanged for existing traffic.
	SelectedBackendNamespace  string
	SelectedBackendName       string
	SelectedBackendSchema     string
	SelectedBackendReselected bool

	// Cross-schema translation facts (APO-742). TranslationApplied is
	// true when the request committed upstream in a different wire
	// schema than the agent spoke; From/To are the canonical
	// gen_ai.system names of those schemas. SkippedBackends counts
	// cross-schema candidates dropped by per-request filtering
	// (streaming, capability misses, no applicable modelRewrite).
	// DroppedExtras counts source-schema members not representable in
	// the target schema (request + response sides). Error carries the
	// reason translation degraded: a fallback's decode/encode failure,
	// or the response-side failure behind a fail-closed 502.
	TranslationApplied         bool
	TranslationFrom            string
	TranslationTo              string
	TranslationSkippedBackends int
	TranslationDroppedExtras   int
	TranslationError           string

	// Attempts is the number of upstream router attempts that recorded
	// facts for this request; AttemptBackends is the ordered
	// "<ns>/<name>" walk of the backends they targeted (the last entry
	// is the serving attempt, mirrored in SelectedBackend*). Both stay
	// zero for unpinned traffic.
	Attempts        int
	AttemptBackends []string

	// RetryIneligibleReason is set when a pinned request cannot be
	// retried — today only "body_too_large": the request body exceeded
	// the synthesized route's retry buffer, so Envoy silently disables
	// retries and an attached FallbackRoutingPolicy cannot fire.
	RetryIneligibleReason string
}

// RequestBodyForEvent returns the bytes to ship in the OTLP request body
// span event: the content-decoded copy when enrichRecord inflated it, else
// the raw captured bytes.
func (r Record) RequestBodyForEvent() []byte {
	if r.RequestBodyDecoded != nil {
		return r.RequestBodyDecoded
	}
	return r.RequestBody
}

// ResponseBodyForEvent is RequestBodyForEvent for the response direction.
func (r Record) ResponseBodyForEvent() []byte {
	if r.ResponseBodyDecoded != nil {
		return r.ResponseBodyDecoded
	}
	return r.ResponseBody
}

func applyClrkMetadata(rec *Record, filterMeta map[string]*structpb.Struct) {
	s, ok := filterMeta[MetadataNamespace]
	if !ok {
		return
	}
	fields := s.GetFields()
	if v := fields[MetaAgentKind]; v != nil {
		rec.AgentKind = decodeAgentKind(v.GetStringValue())
	}
	if v := fields[MetaAgentNamespace]; v != nil {
		rec.AgentNamespace = v.GetStringValue()
	}
	if v := fields[MetaAgentName]; v != nil {
		rec.AgentName = v.GetStringValue()
	}
	if v := fields[MetaAgentUID]; v != nil {
		rec.AgentUID = v.GetStringValue()
	}
	if v := fields[MetaAgentRevision]; v != nil {
		rec.AgentRevision = v.GetStringValue()
	}
	if v := fields[MetaInvocationID]; v != nil {
		rec.InvocationID = v.GetStringValue()
	}
}

// decodeAgentKind translates the raw byte value the proxy_protocol
// listener filter wrote into dynamic metadata back into the human-
// readable kind string OTel consumers expect. The TLV encoding (see
// internal/egress/proxyproto/tlv.go) is one byte: 0 = DaemonAgent,
// 1 = TaskAgent. Without this translation `agent.kind` reaches OTel
// as a NUL or 0x01 byte and downstream attribution silently drops the
// record (the dev TUI's per-agent detail view, ClickHouse views, etc.).
// Pass-through any value that isn't a recognised single byte so a
// future producer that sets the string directly still works.
func decodeAgentKind(raw string) string {
	if len(raw) != 1 {
		return raw
	}
	switch raw[0] {
	case byte(proxyproto.AgentKindDaemon):
		return clrkv1alpha1.AgentKindDaemon
	case byte(proxyproto.AgentKindTask):
		return clrkv1alpha1.AgentKindTask
	}
	return raw
}

// splitHostPort returns host without port from an HTTP/2 :authority,
// stripping IPv6 brackets. Distinct from sink_otlp.splitAuthority,
// which returns host+port; we only need the host here.
func splitHostPort(authority string) (string, string) {
	if authority == "" {
		return "", ""
	}
	host, port, err := net.SplitHostPort(authority)
	if err != nil {
		return strings.Trim(authority, "[]"), ""
	}
	return host, port
}

func headersToMap(h *extprocv3.HttpHeaders) map[string]string {
	m := make(map[string]string)
	for _, kv := range h.GetHeaders().GetHeaders() {
		// Envoy may populate either Value (string) or RawValue (bytes).
		v := kv.GetValue()
		if v == "" && len(kv.GetRawValue()) > 0 {
			v = string(kv.GetRawValue())
		}
		m[strings.ToLower(kv.GetKey())] = v
	}
	return m
}

// appendRing appends src to dst and trims from the head when the
// total exceeds capBytes, keeping the last capBytes bytes. trunc is
// true when bytes were dropped (either from this call or any prior
// one — once we trim, every subsequent call inherits trunc=true via
// dst already being at capacity). Used for streaming response bodies
// where the terminal usage event lives at the tail.
func appendRing(dst, src []byte, capBytes int) ([]byte, bool) {
	if capBytes <= 0 {
		return dst, len(src) > 0
	}
	if len(src) >= capBytes {
		// New chunk alone exceeds the cap; keep only its tail.
		out := make([]byte, capBytes)
		copy(out, src[len(src)-capBytes:])
		return out, true
	}
	dst = append(dst, src...)
	if len(dst) > capBytes {
		drop := len(dst) - capBytes
		dst = dst[drop:]
		return dst, true
	}
	return dst, false
}

// isStreamingContentType reports whether ct names a streamed response
// shape that needs keep-last-N capture (terminal usage event lives at
// the tail).
func isStreamingContentType(ct string) bool {
	if ct == "" {
		return false
	}
	ct = strings.ToLower(ct)
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	switch ct {
	case "text/event-stream", "application/x-ndjson":
		return true
	case "application/vnd.amazon.eventstream":
		// Bedrock converse-stream. Without this entry the response
		// stays BUFFERED on the passthrough path — Envoy holds the
		// whole stream until EOS or the buffer limit.
		return true
	}
	return false
}
