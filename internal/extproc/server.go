// Package extproc implements envoy.service.ext_proc.v3.ExternalProcessor.
// Streams through an Envoy ext_proc filter deliver HTTP headers + body
// chunks (both directions) to this server; we buffer them per stream,
// emit a structured log on stream close, and hand back continue responses
// that leave traffic untouched.
//
// The EG extension (see internal/egextension) wires Envoy so that MITM
// listener traffic flows through the filter, and so that PROXY v2 TLVs
// (agent identity + invocation ID) appear as dynamic metadata under the
// clrk.apoxy.dev namespace — which we read from MetadataContext in the
// downstream handler.
//
// One gRPC service serves two filter positions. Downstream (listener-
// level) streams carry the agent-side transaction and are handled by
// downstreamStream (downstream.go). Upstream (cluster-level) streams —
// one per router attempt against a synthesized LLM cluster — are
// stamped with x-clrk-extproc-role: upstream in the filter's
// GrpcService initial_metadata and get the per-attempt handler.
package extproc

import (
	"context"
	"errors"
	"io"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/go-logr/logr"
	"google.golang.org/grpc/metadata"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/apoxy-dev/clrk/internal/egidentity"
	"github.com/apoxy-dev/clrk/internal/extproc/invocationctx"
)

// captureMaxBytesDefault bounds buffered request+response body bytes per
// stream when the resolved EgressGateway didn't pin a specific cap.
const captureMaxBytesDefault = 64 * 1024

// extprocRoleKey is the gRPC initial-metadata key the egextension
// stamps onto the upstream (cluster-level) ext_proc filter's
// GrpcService so this server can tell the two filter positions apart.
// Absent or any value other than extprocRoleUpstream means downstream.
const (
	extprocRoleKey      = "x-clrk-extproc-role"
	extprocRoleUpstream = "upstream"
)

// Sink receives one captured record per HTTP transaction or L4
// connection. Implementations might push to OTLP, Axiom, stdout, etc.
// Errors are logged and dropped.
type Sink interface {
	Emit(record Record)
	EmitL4(record L4Record)
}

// ServerOption configures the ext_proc Server.
type ServerOption func(*Server)

// WithSinkOverride forces every stream to use the given sink, bypassing
// per-EgressGateway lookup. Intended for tests; production wires the
// controller-runtime client and resolves sinks per-EG.
func WithSinkOverride(sink Sink) ServerOption {
	return func(s *Server) { s.sinkOverride = sink }
}

// WithInvocationContext threads the controller-manager-local store
// that carries an invocation's W3C trace parent context from ingress
// to egress. The egress filter consults it on every stream's
// RequestHeaders phase to (a) parent the egress span on the inbound
// trace and (b) inject `traceparent` on the outbound request when
// the agent didn't set one — see the package doc on
// internal/extproc/invocationctx.
func WithInvocationContext(store *invocationctx.Store) ServerOption {
	return func(s *Server) { s.invocations = store }
}

// Server implements ExternalProcessorServer.
type Server struct {
	extprocv3.UnimplementedExternalProcessorServer

	client       client.Client
	sinkOverride Sink

	registry    *sinkRegistry
	budget      *budgetStore
	invocations *invocationctx.Store

	// states correlates a request's downstream stream with its
	// upstream (per-attempt) streams by x-request-id. See reqstate.go.
	states *requestStateStore
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
		states:   newRequestStateStore(),
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

// streamRole reads the filter-position role from the stream's gRPC
// initial metadata. See extprocRoleKey.
func streamRole(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if v := md.Get(extprocRoleKey); len(v) > 0 {
		return v[0]
	}
	return ""
}

// Process handles one ext_proc stream. A downstream stream corresponds
// to one HTTP transaction (request + response pair, assuming upstream
// keep-alives); an upstream stream corresponds to one router attempt.
func (s *Server) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	ctx := stream.Context()
	logger := ctrllog.FromContext(ctx).WithName("extproc")

	if streamRole(ctx) == extprocRoleUpstream {
		return s.processUpstream(stream, logger)
	}

	ds := s.newDownstreamStream(ctx, logger)
	for {
		req, err := stream.Recv()
		if err != nil {
			ds.finish()
			if errors.Is(err, io.EOF) {
				return nil
			}
			// Client cancelled or transport error. finish() already
			// emitted whatever we captured before the break.
			return err
		}

		// Agent identity is attached to every message under the clrk
		// metadata namespace. It's idempotent to re-read — same values
		// every message — so we just overwrite each time.
		if mctx := req.GetMetadataContext(); mctx != nil {
			applyClrkMetadata(&ds.rec, mctx.GetFilterMetadata())
		}

		resp := ds.handle(req, time.Now())
		if resp == nil {
			continue
		}
		if err := stream.Send(resp); err != nil {
			ds.finish()
			return err
		}
	}
}

// processUpstream drives one upstream (per-attempt) stream: the
// request-only adapter that translates, repoints, and authenticates
// each router attempt for the backend Envoy picked (see upstream.go).
func (s *Server) processUpstream(stream extprocv3.ExternalProcessor_ProcessServer, logger logr.Logger) error {
	us := s.newUpstreamStream(stream.Context(), logger)
	for {
		req, err := stream.Recv()
		if err != nil {
			us.finish()
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		resp := us.handle(req)
		if resp == nil {
			continue
		}
		if err := stream.Send(resp); err != nil {
			us.finish()
			return err
		}
	}
}

// newDownstreamStream resolves the per-stream sink + config once and
// builds the downstream handler state. We don't redo the resolution per
// message — config changes between request and response of a single
// transaction would split the record, which is worse than using the
// start-of-stream config.
//
// Sink emission and registry-driven config (capture bounds, route
// table, EG identity) are independent: a test can use WithSinkOverride
// to capture emitted records while still letting the registry resolve
// routes + budget against a fake client. When the registry is
// unavailable (no client wired, EG missing), we fall back to slogSink
// for emission and skip route/budget logic.
func (s *Server) newDownstreamStream(ctx context.Context, logger logr.Logger) *downstreamStream {
	ds := &downstreamStream{
		srv:             s,
		logger:          logger,
		rec:             Record{Timestamp: time.Now()},
		sink:            s.sinkOverride,
		maxCaptureBytes: captureMaxBytesDefault,
	}
	if s.client != nil {
		es, err := s.registry.get(ctx)
		if err != nil {
			if ds.sink == nil {
				logger.V(1).Info("Falling back to slog sink", "reason", err.Error())
			}
		} else {
			if ds.sink == nil {
				ds.sink = es.sink
			}
			ds.maxCaptureBytes = es.maxCaptureBytes
			ds.includedTypes = es.includedTypes
			ds.routes = es.routes
			ds.mcpRoutes = es.mcpRoutes
			ds.creds = es.creds
			if k, kerr := egidentity.MustFromContext(ctx); kerr == nil {
				ds.egKey = k
			}
		}
	}
	if ds.sink == nil {
		ds.sink = slogSink{}
	}
	ds.reqBytesLeft = ds.maxCaptureBytes
	ds.respBytesLeft = ds.maxCaptureBytes
	return ds
}
