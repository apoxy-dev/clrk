// Package extproc implements envoy.service.ext_proc.v3.ExternalProcessor.
// Streams through an Envoy ext_proc filter deliver HTTP headers + body
// chunks (both directions) to this server; we buffer them per stream,
// emit a structured log on stream close, and hand back continue responses
// that leave traffic untouched.
//
// The EG extension (see internal/egextension) wires Envoy so that MITM
// listener traffic flows through the filter, and so that PROXY v2 TLVs
// (agent identity + invocation ID) appear as dynamic metadata under the
// clrk.apoxy.dev namespace — which we read from MetadataContext below.
package extproc

import (
	"errors"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/protobuf/types/known/structpb"
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
)

// captureMaxBytesDefault bounds buffered request+response body bytes per
// stream. Callers can override via ServerOption.
const captureMaxBytesDefault = 64 * 1024

// Sink receives one captured record per HTTP transaction. Implementations
// might push to OTLP, Axiom, stdout, etc. Errors are logged and dropped.
type Sink interface {
	Emit(record Record)
}

// Record is the structured representation of one HTTP request/response
// observed through ext_proc. All fields are best-effort; Envoy may not
// deliver bodies or trailers depending on mode.
type Record struct {
	Timestamp time.Time

	AgentKind      string
	AgentNamespace string
	AgentName      string
	AgentUID       string
	AgentRevision  string
	InvocationID   string

	RequestHeaders  map[string]string
	RequestBody     []byte
	RequestTruncated bool

	ResponseHeaders   map[string]string
	ResponseBody      []byte
	ResponseTruncated bool
}

// ServerOption configures the ext_proc Server.
type ServerOption func(*Server)

// WithMaxCaptureBytes overrides the per-direction body capture cap.
func WithMaxCaptureBytes(n int) ServerOption {
	return func(s *Server) { s.maxCaptureBytes = n }
}

// WithSink sets the destination for captured records. Defaults to a
// slog-backed sink.
func WithSink(sink Sink) ServerOption {
	return func(s *Server) { s.sink = sink }
}

// Server implements ExternalProcessorServer.
type Server struct {
	extprocv3.UnimplementedExternalProcessorServer

	maxCaptureBytes int
	sink            Sink
}

// New constructs an ext_proc server.
func New(opts ...ServerOption) *Server {
	s := &Server{
		maxCaptureBytes: captureMaxBytesDefault,
		sink:            slogSink{},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Process handles one ext_proc stream. One stream corresponds to one HTTP
// transaction (request + response pair, assuming upstream keep-alives).
func (s *Server) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	rec := Record{Timestamp: time.Now()}
	reqBytesLeft := s.maxCaptureBytes
	respBytesLeft := s.maxCaptureBytes

	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				s.sink.Emit(rec)
				return nil
			}
			// Client cancelled or transport error. Still emit whatever we
			// captured before the break.
			s.sink.Emit(rec)
			return err
		}

		// Agent identity is attached to every message under the clrk
		// metadata namespace. It's idempotent to re-read — same values
		// every message — so we just overwrite each time.
		if mctx := req.GetMetadataContext(); mctx != nil {
			applyClrkMetadata(&rec, mctx.GetFilterMetadata())
		}

		var resp *extprocv3.ProcessingResponse
		switch m := req.Request.(type) {
		case *extprocv3.ProcessingRequest_RequestHeaders:
			rec.RequestHeaders = headersToMap(m.RequestHeaders)
			resp = headersContinue(true)
			// Force port 443 onto :authority when none was supplied. The
			// dynamic_forward_proxy filter parses host:port off the
			// authority and defaults to :80, so HTTPS requests from the
			// sandbox (which omit the implicit :443) get plaintext-
			// dialed to 80 and rejected by hosts behind Cloudflare.
			// Done here, before the dfp HTTP filter, so dfp parses the
			// rewritten value. Filter order is ext_proc → dfp → router.
			if mut := authorityPortMutation(rec.RequestHeaders[":authority"]); mut != nil {
				resp.GetRequestHeaders().GetResponse().HeaderMutation = mut
			}
		case *extprocv3.ProcessingRequest_ResponseHeaders:
			rec.ResponseHeaders = headersToMap(m.ResponseHeaders)
			resp = headersContinue(false)
		case *extprocv3.ProcessingRequest_RequestBody:
			body, trunc := appendBounded(rec.RequestBody, m.RequestBody.GetBody(), &reqBytesLeft)
			rec.RequestBody = body
			rec.RequestTruncated = rec.RequestTruncated || trunc
			resp = bodyContinue(true)
		case *extprocv3.ProcessingRequest_ResponseBody:
			body, trunc := appendBounded(rec.ResponseBody, m.ResponseBody.GetBody(), &respBytesLeft)
			rec.ResponseBody = body
			rec.ResponseTruncated = rec.ResponseTruncated || trunc
			resp = bodyContinue(false)
		case *extprocv3.ProcessingRequest_RequestTrailers:
			resp = trailersContinue(true)
		case *extprocv3.ProcessingRequest_ResponseTrailers:
			resp = trailersContinue(false)
		default:
			// Unknown oneof branch (e.g. new Envoy version). Skip.
			continue
		}

		if resp == nil {
			continue
		}
		if err := stream.Send(resp); err != nil {
			s.sink.Emit(rec)
			return err
		}
	}
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

// appendBounded appends src to dst up to *left bytes, truncating the
// remainder. Returns whether truncation occurred.
func appendBounded(dst, src []byte, left *int) ([]byte, bool) {
	if *left <= 0 {
		return dst, len(src) > 0
	}
	if len(src) <= *left {
		*left -= len(src)
		return append(dst, src...), false
	}
	dst = append(dst, src[:*left]...)
	*left = 0
	return dst, true
}

func applyClrkMetadata(rec *Record, filterMeta map[string]*structpb.Struct) {
	s, ok := filterMeta[MetadataNamespace]
	if !ok {
		return
	}
	fields := s.GetFields()
	if v := fields[MetaAgentKind]; v != nil {
		rec.AgentKind = v.GetStringValue()
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

// authorityPortRE matches a trailing :NN port suffix on a host. We don't
// need to handle bracketed IPv6 — agents resolve target hostnames, not
// literals — but the regex is anchored so a colon in a userinfo segment
// (which is also forbidden in :authority) doesn't false-match.
var authorityPortRE = regexp.MustCompile(`:[0-9]+$`)

// authorityPortMutation returns a HeaderMutation that rewrites
// :authority to host:443 when no port is set; nil otherwise. See the
// caller for context on why this is necessary.
func authorityPortMutation(authority string) *extprocv3.HeaderMutation {
	if authority == "" || authorityPortRE.MatchString(authority) {
		return nil
	}
	return &extprocv3.HeaderMutation{
		SetHeaders: []*corev3.HeaderValueOption{{
			Header: &corev3.HeaderValue{Key: ":authority", Value: authority + ":443"},
		}},
	}
}

// headersContinue / bodyContinue / trailersContinue build observability-
// mode-safe responses that tell Envoy to forward unchanged.
func headersContinue(isRequest bool) *extprocv3.ProcessingResponse {
	r := &extprocv3.HeadersResponse{Response: &extprocv3.CommonResponse{
		Status: extprocv3.CommonResponse_CONTINUE,
	}}
	if isRequest {
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestHeaders{RequestHeaders: r}}
	}
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseHeaders{ResponseHeaders: r}}
}

func bodyContinue(isRequest bool) *extprocv3.ProcessingResponse {
	r := &extprocv3.BodyResponse{Response: &extprocv3.CommonResponse{
		Status: extprocv3.CommonResponse_CONTINUE,
	}}
	if isRequest {
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestBody{RequestBody: r}}
	}
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseBody{ResponseBody: r}}
}

func trailersContinue(isRequest bool) *extprocv3.ProcessingResponse {
	r := &extprocv3.TrailersResponse{}
	if isRequest {
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestTrailers{RequestTrailers: r}}
	}
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseTrailers{ResponseTrailers: r}}
}

// slogSink is the default sink used when no OTLP destination is wired in.
// Emits one structured log line per record. OTLP emission is layered on
// via WithSink once the EgressGateway controller plumbs the endpoint.
type slogSink struct{}

func (slogSink) Emit(r Record) {
	slog.Info("clrk egress HTTP transaction",
		"agent.kind", r.AgentKind,
		"agent.namespace", r.AgentNamespace,
		"agent.name", r.AgentName,
		"agent.uid", r.AgentUID,
		"agent.revision", r.AgentRevision,
		"invocation.id", r.InvocationID,
		"req.method", r.RequestHeaders[":method"],
		"req.authority", r.RequestHeaders[":authority"],
		"req.path", r.RequestHeaders[":path"],
		"req.body_bytes", len(r.RequestBody),
		"req.truncated", r.RequestTruncated,
		"resp.status", r.ResponseHeaders[":status"],
		"resp.body_bytes", len(r.ResponseBody),
		"resp.truncated", r.ResponseTruncated,
	)
}
