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
	"net"
	"regexp"
	"strings"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/protobuf/types/known/structpb"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/apoxy-dev/clrk/internal/egidentity"
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
	budget   *budgetStore
}

// New constructs an ext_proc server. The client is used to look up
// EgressGateway state per-stream (OTLP endpoint + body capture bounds);
// pass cm.GetClient() in the controller-manager. Tests may pass nil and
// use WithSinkOverride.
func New(c client.Client, opts ...ServerOption) *Server {
	s := &Server{
		client:   c,
		registry: newSinkRegistry(c),
		budget:   newBudgetStore(),
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
	//
	// Sink emission and registry-driven config (capture bounds, route
	// table, EG identity) are independent: a test can use
	// WithSinkOverride to capture emitted records while still letting
	// the registry resolve routes + budget against a fake client. When
	// the registry is unavailable (no client wired, EG missing), we
	// fall back to slogSink for emission and skip route/budget logic.
	var (
		sink            Sink = s.sinkOverride
		maxCaptureBytes int  = captureMaxBytesDefault
		includedTypes   []string
		routes          *routeTable
		creds           *credTable
		egKey           types.NamespacedName
	)
	if s.client != nil {
		es, err := s.registry.get(ctx)
		if err != nil {
			if sink == nil {
				logger.V(1).Info("Falling back to slog sink", "reason", err.Error())
			}
		} else {
			if sink == nil {
				sink = es.sink
			}
			maxCaptureBytes = es.maxCaptureBytes
			includedTypes = es.includedTypes
			routes = es.routes
			creds = es.creds
			if k, kerr := egidentity.MustFromContext(ctx); kerr == nil {
				egKey = k
			}
		}
	}
	if sink == nil {
		sink = slogSink{}
	}

	reqBytesLeft := maxCaptureBytes
	respBytesLeft := maxCaptureBytes

	for {
		req, err := stream.Recv()
		if err != nil {
			rec.EndAt = time.Now()
			if errors.Is(err, io.EOF) {
				matched := enrichRecord(&rec, routes)
				s.chargeBudget(matched, egKey, rec)
				sink.Emit(rec)
				return nil
			}
			// Client cancelled or transport error. Still emit whatever we
			// captured before the break.
			matched := enrichRecord(&rec, routes)
			s.chargeBudget(matched, egKey, rec)
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
			// Pre-flight TokenBudget check: if the matched route's
			// daily counter is already over cap, return ImmediateResponse
			// 429 instead of letting the request reach the upstream. The
			// match here uses model="" because we haven't buffered the
			// body yet — model-scoped rules can't enforce pre-flight by
			// design (see routeTable.match).
			if denied, used, max := s.evaluateBudget(routes, egKey, rec.RequestHeaders); denied != "" {
				rec.BudgetDenied = true
				rec.BudgetDailyUsed = used
				rec.BudgetDailyMax = max
				rec.MatchedRouteName = denied
				if err := stream.Send(immediateResponse429("clrk: token budget exceeded for route " + denied)); err != nil {
					rec.EndAt = time.Now()
					enrichRecord(&rec, routes)
					sink.Emit(rec)
					return err
				}
				// Don't break — keep the stream up so Envoy can drain;
				// the next Recv() will return EOF and we emit the record
				// from the EOF branch above.
				continue
			}
			resp = headersContinue(true)
			// Force port 443 onto :authority when none was supplied. The
			// dynamic_forward_proxy filter parses host:port off the
			// authority and defaults to :80, so HTTPS requests from the
			// sandbox (which omit the implicit :443) get plaintext-
			// dialed to 80 and rejected by hosts behind Cloudflare.
			// Done here, before the dfp HTTP filter, so dfp parses the
			// rewritten value. Filter order is ext_proc → dfp → router.
			mut := authorityPortMutation(rec.RequestHeaders[":authority"])
			// Inject credentials from any CredentialInjectionPolicy
			// attached to the matched APR or to the EG itself. This is
			// the architectural enforcement that API keys never live
			// inside the agent: the proxy adds them on the way out.
			// Match uses model="" because the request body isn't
			// buffered yet — same constraint as the budget pre-flight
			// (see routeTable.match).
			if injs := lookupCreds(creds, routes, rec.RequestHeaders); len(injs) > 0 {
				if collisions := redactInjected(rec.RequestHeaders, injs); len(collisions) > 0 {
					logger.Info("Agent supplied a header that policy is configured to inject; proxy overwriting",
						"policies", collisions)
				}
				mut = applyInjections(mut, injs)
			}
			if mut != nil {
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
			matched := enrichRecord(&rec, routes)
			s.chargeBudget(matched, egKey, rec)
			sink.Emit(rec)
			return err
		}
	}
}

// enrichRecord runs the provider parser on the captured headers/bodies
// and, if a parser matched, asks the per-EG route table to identify
// the AIProviderRoute that accepted the transaction. Returns the
// matched routeRule (for downstream budget charging) or nil. Both
// outcomes are no-ops when the request didn't reach a known provider
// host or when no APR is attached to the calling EG.
//
// Called once per stream, just before sink.Emit, so partial-capture
// records (truncated bodies) still attempt parsing.
func enrichRecord(rec *Record, routes *routeTable) *routeRule {
	host, _ := splitHostPort(rec.RequestHeaders[":authority"])
	if parser := parsers.For(host); parser != nil {
		rec.Provider = parser.Parse(parsers.Input{
			Method:        rec.RequestHeaders[":method"],
			Path:          rec.RequestHeaders[":path"],
			ReqHeaders:    rec.RequestHeaders,
			RespHeaders:   rec.ResponseHeaders,
			ReqBody:       rec.RequestBody,
			ReqTruncated:  rec.RequestTruncated,
			RespBody:      rec.ResponseBody,
			RespTruncated: rec.ResponseTruncated,
		})
	}
	if routes == nil || rec.Provider == nil {
		return nil
	}
	rr := routes.match(rec.Provider.System, rec.RequestHeaders[":path"], rec.Provider.RequestModel)
	if rr == nil {
		return nil
	}
	rec.MatchedRouteNamespace = rr.routeNamespace
	rec.MatchedRouteName = rr.routeName
	return rr
}

// lookupCreds matches the request to an APR (if any) and returns the
// credentials that should be injected. Pure read against pre-resolved
// tables; called from the RequestHeaders branch where we do header
// mutation. Returns an empty slice when no CIPs attach to this
// transaction.
func lookupCreds(creds *credTable, routes *routeTable, headers map[string]string) []credInjection {
	if creds == nil {
		return nil
	}
	var rr *routeRule
	if routes != nil {
		host, _ := splitHostPort(headers[":authority"])
		if system := parsers.SystemFor(host); system != "" {
			rr = routes.match(system, headers[":path"], "")
		}
	}
	return creds.lookup(rr)
}

// evaluateBudget runs the pre-flight TokenBudget check at request-
// headers time. Returns ("", 0, 0) when nothing should block the
// request; otherwise returns the route name plus current daily total
// and cap, and the caller is expected to short-circuit with a 429.
//
// Pre-flight matches with model="" because the request body hasn't
// been buffered yet — model-scoped rules don't enforce here (see
// routeTable.match for the rationale).
func (s *Server) evaluateBudget(routes *routeTable, eg types.NamespacedName, headers map[string]string) (route string, used, max int64) {
	if s.budget == nil || routes == nil || eg.Name == "" {
		return "", 0, 0
	}
	host, _ := splitHostPort(headers[":authority"])
	system := parsers.SystemFor(host)
	if system == "" {
		return "", 0, 0
	}
	rr := routes.match(system, headers[":path"], "")
	if rr == nil || rr.tokenBudget == nil || rr.tokenBudget.MaxTokensPerDay == nil {
		return "", 0, 0
	}
	cap := *rr.tokenBudget.MaxTokensPerDay
	if cap <= 0 {
		return "", 0, 0
	}
	bk := budgetKey{
		egNamespace:    eg.Namespace,
		egName:         eg.Name,
		routeNamespace: rr.routeNamespace,
		routeName:      rr.routeName,
	}
	if s.budget.Allow(bk, cap) {
		return "", 0, 0
	}
	return rr.routeName, s.budget.snapshot(bk), cap
}

// chargeBudget increments the daily counter for the matched route by
// the parsed input+output token total. No-op when the route has no
// TokenBudget, the parser found no usage, or the request was denied
// at pre-flight (in which case nothing reached the upstream).
func (s *Server) chargeBudget(rr *routeRule, eg types.NamespacedName, rec Record) {
	if s.budget == nil || rr == nil || rr.tokenBudget == nil {
		return
	}
	if rec.BudgetDenied || rec.Provider == nil {
		return
	}
	tokens := rec.Provider.InputTokens + rec.Provider.OutputTokens
	if tokens <= 0 {
		return
	}
	s.budget.Add(budgetKey{
		egNamespace:    eg.Namespace,
		egName:         eg.Name,
		routeNamespace: rr.routeNamespace,
		routeName:      rr.routeName,
	}, tokens)
}

// immediateResponse429 builds an ext_proc ImmediateResponse that
// short-circuits the upstream call with HTTP 429 and a small JSON
// body explaining the deny reason. Envoy synthesizes the client
// response from this; the client sees a 429 immediately.
func immediateResponse429(detail string) *extprocv3.ProcessingResponse {
	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_ImmediateResponse{
			ImmediateResponse: &extprocv3.ImmediateResponse{
				Status: &typev3.HttpStatus{Code: typev3.StatusCode_TooManyRequests},
				Body:   []byte(`{"error":` + jsonString(detail) + `}`),
				Headers: &extprocv3.HeaderMutation{
					SetHeaders: []*corev3.HeaderValueOption{{
						Header: &corev3.HeaderValue{Key: "content-type", Value: "application/json"},
					}},
				},
			},
		},
	}
}

// jsonString quotes s as a JSON string. Tiny helper so we don't pull
// encoding/json in just to format an error body.
func jsonString(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"', '\\':
			out = append(out, '\\', c)
		case '\n':
			out = append(out, '\\', 'n')
		case '\r':
			out = append(out, '\\', 'r')
		case '\t':
			out = append(out, '\\', 't')
		default:
			if c < 0x20 {
				continue
			}
			out = append(out, c)
		}
	}
	out = append(out, '"')
	return string(out)
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
			// RawValue (not Value) — see applyInjections for the rationale.
			Header:       &corev3.HeaderValue{Key: ":authority", RawValue: []byte(authority + ":443")},
			AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
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
