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
	"context"
	"errors"
	"io"
	"regexp"
	"strings"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/protobuf/types/known/structpb"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
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
// stream when the resolved EgressGateway didn't pin a specific cap.
const captureMaxBytesDefault = 64 * 1024

// Sink receives one captured record per HTTP transaction. Implementations
// might push to OTLP, Axiom, stdout, etc. Errors are logged and dropped.
type Sink interface {
	Emit(record Record)
}

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
}

// ServerOption configures the ext_proc Server.
type ServerOption func(*Server)

// WithSinkOverride forces every stream to use the given sink, bypassing
// per-EgressGateway lookup. Intended for tests; production wires the
// controller-runtime client and resolves sinks per-EG.
func WithSinkOverride(sink Sink) ServerOption {
	return func(s *Server) { s.sinkOverride = sink }
}

// Server implements ExternalProcessorServer.
type Server struct {
	extprocv3.UnimplementedExternalProcessorServer

	client       client.Client
	sinkOverride Sink

	registry *sinkRegistry
}

// New constructs an ext_proc server. The client is used to look up
// EgressGateway state per-stream (OTLP endpoint + body capture bounds);
// pass cm.GetClient() in the controller-manager. Tests may pass nil and
// use WithSinkOverride.
func New(c client.Client, opts ...ServerOption) *Server {
	s := &Server{
		client:   c,
		registry: newSinkRegistry(c),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Stop releases any resources held by per-EgressGateway sinks (in
// particular, OTLP exporter background workers and pending batches).
// Safe to call multiple times.
func (s *Server) Stop(ctx context.Context) {
	if s.registry != nil {
		s.registry.shutdownAll(ctx)
	}
}

// Process handles one ext_proc stream. One stream corresponds to one HTTP
// transaction (request + response pair, assuming upstream keep-alives).
func (s *Server) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	ctx := stream.Context()
	logger := ctrllog.FromContext(ctx).WithName("extproc")

	rec := Record{Timestamp: time.Now()}

	// Resolve per-stream sink + capture bounds once. We don't redo this
	// per message — config changes between request and response of a
	// single transaction would split the record, which is worse than
	// using the start-of-stream config.
	var (
		sink            Sink
		maxCaptureBytes int
		includedTypes   []string
	)
	if s.sinkOverride != nil {
		sink = s.sinkOverride
		maxCaptureBytes = captureMaxBytesDefault
	} else {
		es, err := s.registry.get(ctx)
		if err != nil {
			logger.V(1).Info("Falling back to slog sink", "reason", err.Error())
			sink = slogSink{}
			maxCaptureBytes = captureMaxBytesDefault
		} else {
			sink = es.sink
			maxCaptureBytes = es.maxCaptureBytes
			includedTypes = es.includedTypes
		}
	}

	reqBytesLeft := maxCaptureBytes
	respBytesLeft := maxCaptureBytes

	for {
		req, err := stream.Recv()
		if err != nil {
			rec.EndAt = time.Now()
			if errors.Is(err, io.EOF) {
				sink.Emit(rec)
				return nil
			}
			// Client cancelled or transport error. Still emit whatever we
			// captured before the break.
			sink.Emit(rec)
			return err
		}

		// Agent identity is attached to every message under the clrk
		// metadata namespace. It's idempotent to re-read — same values
		// every message — so we just overwrite each time.
		if mctx := req.GetMetadataContext(); mctx != nil {
			applyClrkMetadata(&rec, mctx.GetFilterMetadata())
		}

		now := time.Now()
		var resp *extprocv3.ProcessingResponse
		switch m := req.Request.(type) {
		case *extprocv3.ProcessingRequest_RequestHeaders:
			rec.RequestHeadersAt = now
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
			// Apply content-type body gate per-direction. If the request
			// content-type isn't in the included set, drop request body
			// capture (set bytesLeft to 0). Headers stay either way.
			if !contentTypeIncluded(rec.RequestHeaders["content-type"], includedTypes) {
				reqBytesLeft = 0
			}
		case *extprocv3.ProcessingRequest_ResponseHeaders:
			rec.ResponseHeadersAt = now
			rec.ResponseHeaders = headersToMap(m.ResponseHeaders)
			resp = headersContinue(false)
			if !contentTypeIncluded(rec.ResponseHeaders["content-type"], includedTypes) {
				respBytesLeft = 0
			}
		case *extprocv3.ProcessingRequest_RequestBody:
			body, trunc := appendBounded(rec.RequestBody, m.RequestBody.GetBody(), &reqBytesLeft)
			if rec.RequestBodyAt.IsZero() {
				rec.RequestBodyAt = now
			}
			rec.RequestBody = body
			rec.RequestTruncated = rec.RequestTruncated || trunc
			resp = bodyContinue(true)
		case *extprocv3.ProcessingRequest_ResponseBody:
			body, trunc := appendBounded(rec.ResponseBody, m.ResponseBody.GetBody(), &respBytesLeft)
			if rec.ResponseBodyAt.IsZero() {
				rec.ResponseBodyAt = now
			}
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
			rec.EndAt = time.Now()
			sink.Emit(rec)
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
