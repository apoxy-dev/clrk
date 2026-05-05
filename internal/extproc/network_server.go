// Network ext_proc server. Implements the L4 sibling of the HTTP
// ext_proc server in server.go. Wired onto the same gRPC endpoint by
// RegisterNetworkServer; reuses sinkRegistry for OTLP fanout. Receives
// agent → server bytes (ProcessingMode.ProcessRead = STREAMED), counts
// + discards them, and emits one L4Record per connection on stream
// close. Server → agent bytes are unobserved (ProcessWrite = SKIP).
package extproc

import (
	"errors"
	"io"
	"time"

	netextprocsvcv3 "github.com/envoyproxy/go-control-plane/envoy/service/network_ext_proc/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

// NetworkServer implements the network_ext_proc.v3 NetworkExternalProcessor
// service. One gRPC stream corresponds to one TCP/TLS connection on the
// egress listener's TCP-fallback chain.
type NetworkServer struct {
	netextprocsvcv3.UnimplementedNetworkExternalProcessorServer

	// Reuses the HTTP server's registry so per-EG sink resolution is
	// shared (same EG identity → same OTLP exporter pair).
	server *Server
}

// NewNetworkServer constructs a network ext_proc server that emits to
// the same per-EG sinks the HTTP server uses.
func NewNetworkServer(s *Server) *NetworkServer {
	return &NetworkServer{server: s}
}

// RegisterNetworkServer attaches the L4 ext_proc service to the given
// gRPC server. Call alongside the HTTP ExternalProcessorServer
// registration in cmd/controller-manager so a single gRPC listener
// serves both.
func RegisterNetworkServer(s *grpc.Server, server *Server) {
	netextprocsvcv3.RegisterNetworkExternalProcessorServer(s, NewNetworkServer(server))
}

// Process handles one network ext_proc stream. With ProcessRead =
// STREAMED, ProcessWrite = SKIP the filter sends ReadData chunks for
// every agent → server packet plus a final ReadData{end_of_stream:true}
// when the connection closes. We respond UNMODIFIED + CONTINUE on
// every chunk so Envoy forwards the original bytes unchanged.
func (n *NetworkServer) Process(stream netextprocsvcv3.NetworkExternalProcessor_ProcessServer) error {
	ctx := stream.Context()
	logger := ctrllog.FromContext(ctx).WithName("extproc-l4")

	rec := L4Record{Timestamp: time.Now()}
	identitySet := false

	// Resolve the per-EG sink once at stream start. Falls back to
	// slogSink when no client is wired or EG resolution fails — same
	// shape as the HTTP path's bootstrap.
	var sink Sink = n.server.sinkOverride
	if sink == nil && n.server.client != nil {
		if es, err := n.server.registry.get(ctx); err == nil {
			sink = es.sink
		} else {
			logger.V(1).Info("Falling back to slog sink", "reason", err.Error())
		}
	}
	if sink == nil {
		sink = slogSink{}
	}

	for {
		req, err := stream.Recv()
		if err != nil {
			rec.EndAt = time.Now()
			sink.EmitL4(rec)
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		if !identitySet {
			if md := req.GetMetadata(); md != nil {
				applyClrkMetadataL4(&rec, md.GetFilterMetadata())
				identitySet = true
			}
		}

		if d := req.GetReadData(); d != nil {
			rec.BytesUpstream += int64(len(d.Data))
		}
		// WriteData branch is unreachable under ProcessWrite = SKIP.

		if err := stream.Send(&netextprocsvcv3.ProcessingResponse{
			DataProcessingStatus: netextprocsvcv3.ProcessingResponse_UNMODIFIED,
			ConnectionStatus:     netextprocsvcv3.ProcessingResponse_CONTINUE,
		}); err != nil {
			rec.EndAt = time.Now()
			sink.EmitL4(rec)
			return err
		}
	}
}

// applyClrkMetadataL4 reads the same clrk.apoxy.dev namespace fields
// the HTTP path reads (see applyClrkMetadata in server.go) but onto an
// L4Record. Kept separate to avoid an extra struct or interface just
// to share the field-mapping loop.
func applyClrkMetadataL4(rec *L4Record, filterMeta map[string]*structpb.Struct) {
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
	if v := fields[MetaDstName]; v != nil {
		rec.DstName = v.GetStringValue()
	}
}
