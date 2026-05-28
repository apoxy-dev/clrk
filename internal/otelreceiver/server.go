package otelreceiver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"

	"github.com/apoxy-dev/clrk/internal/chwriter"
	"github.com/apoxy-dev/clrk/internal/otelemit"
	"github.com/apoxy-dev/clrk/internal/otelforward"
)

// Server is cm's OTLP/HTTP receiver. Inbound requests are decoded and
// dispatched to chwriter (always) plus a per-EG otelforward.Forwarder
// (when one is registered for the request's EGRef). Cancelling the
// ctx passed to Start triggers graceful shutdown.
type Server struct {
	writer   *chwriter.Writer
	forwards *otelforward.Registry

	addr string
}

// Start binds an HTTP listener on addr and returns a running Server.
// writer is required; forwards may be nil to disable customer
// re-export.
func Start(ctx context.Context, addr string, writer *chwriter.Writer, forwards *otelforward.Registry) (*Server, error) {
	if writer == nil {
		return nil, errors.New("writer is required")
	}
	s := &Server{writer: writer, forwards: forwards}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/logs", s.handleLogs)
	mux.HandleFunc("/v1/traces", s.handleTraces)
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		slog.Warn("OTLP receiver got unknown path", "path", req.URL.Path, "method", req.Method)
		http.NotFound(w, req)
	})

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	s.addr = lis.Addr().String()
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("OTLP receiver exited", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	return s, nil
}

// Addr returns the bound address. Useful when Start was called with
// `:0` to pick a random port.
func (s *Server) Addr() string { return s.addr }

func (s *Server) handleLogs(w http.ResponseWriter, req *http.Request) {
	body, err := ReadBody(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var msg collogspb.ExportLogsServiceRequest
	if err := proto.Unmarshal(body, &msg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(msg.GetResourceLogs()) > 0 {
		s.writer.EnqueueLogs(msg.GetResourceLogs())
		if s.forwards != nil {
			if egRef := requestEGRef(msg.GetResourceLogs()[0].GetResource().GetAttributes()); egRef != "" {
				if fwd := s.forwards.Get(egRef); fwd != nil {
					fwd.EnqueueLogs(body)
				}
			}
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(EmptyLogsResp)
}

func (s *Server) handleTraces(w http.ResponseWriter, req *http.Request) {
	body, err := ReadBody(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var msg coltracepb.ExportTraceServiceRequest
	if err := proto.Unmarshal(body, &msg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(msg.GetResourceSpans()) > 0 {
		s.writer.EnqueueTraces(msg.GetResourceSpans())
		if s.forwards != nil {
			if egRef := requestEGRef(msg.GetResourceSpans()[0].GetResource().GetAttributes()); egRef != "" {
				if fwd := s.forwards.Get(egRef); fwd != nil {
					fwd.EnqueueTraces(body)
				}
			}
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(EmptyTracesResp)
}

// requestEGRef extracts the clrk.egress_gateway resource attribute
// from a request. Producers in clrk emit one signal per EG identity,
// so a single value is expected; when the request mixes EGRefs we
// take the first as the forward destination and rely on chwriter's
// per-row EGRef column to keep persistence correct.
func requestEGRef(attrs []*commonpb.KeyValue) string {
	for _, kv := range attrs {
		if kv.GetKey() == otelemit.AttrEgressGateway {
			return otelemit.AnyValueString(kv.GetValue())
		}
	}
	return ""
}
