// Package nats embeds a nats-server with JetStream enabled inside the
// controller-manager so workers and in-process producers can publish
// and subscribe to Invocation lifecycle events without standing up a
// separate NATS deployment.
//
// The server listens on a TCP client port reachable by worker pods via
// the controller-manager Service, and also serves cm-local clients (the
// JetStream->ClickHouse consumer, the ingress ext_proc publisher, and
// the Invocation Watch) over an in-process pipe with no network hop.
//
// Lifecycle mirrors internal/clickhouse: a ctx-scoped Run that blocks
// for the process lifetime and drains with a graceful Shutdown on
// cancel. JetStream storage is file-backed under StoreDir (PVC-backed
// in the cm pod) so the durable INVOCATIONS stream survives a restart;
// the stream's MaxAge bounds how far back a Watch can resume, while
// ClickHouse (separate TTL) is the long-term store.
package nats

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/apoxy-dev/clrk/internal/invevent"
	"github.com/apoxy-dev/clrk/internal/ports"
)

const (
	// DefaultPort is the client port the embedded server listens on.
	// Worker pods dial controller-manager.<ns>.svc:4222; cm-local
	// clients use in-process connections via Connect. Sourced from the
	// leaf ports package so the dev bootstrap shares one literal.
	DefaultPort = int(ports.NATSClientPort)

	// DefaultStoreDir is the on-disk JetStream store. PVC-backed in the
	// cm pod so the durable INVOCATIONS stream survives pod restarts.
	DefaultStoreDir = "/var/lib/clrk/nats"

	// defaultStreamMaxAge bounds how long events are retained for Watch
	// replay. It must exceed the materialization lag + a client's
	// List->Watch handoff gap + the watch reconnect window. A Watch
	// requesting a sequence older than the stream's first retained
	// message gets a 410 (resourceVersion too old) and re-Lists.
	defaultStreamMaxAge = 72 * time.Hour

	// readyTimeout caps how long Run waits for the server to accept
	// connections before declaring startup failed.
	readyTimeout = 10 * time.Second

	// workerStatusKVTTL bounds how long a worker's last status lingers
	// after its final Put. Set strictly greater than the consumer's
	// dead-after window (healthcheck.healthcheckerDeadAfter, 15s) so the
	// controller-manager's lastSeen staleness check drops a quiet worker
	// from routing before the key is reaped; TTL is bucket hygiene only
	// (so a cold replica's initial Watch replay doesn't see corpses), not
	// a liveness signal. LimitMarkerTTL is deliberately left unset — a
	// MaxAge expiry is silent to live watchers, and we rely on lastSeen +
	// explicit Delete-on-shutdown instead.
	workerStatusKVTTL = 20 * time.Second
)

// Server is the embedded nats-server + JetStream stream supervisor.
type Server struct {
	host       string
	port       int
	storeDir   string
	serverName string
	maxAge     time.Duration
	maxBytes   int64

	srv *natsserver.Server
}

// Option configures a Server.
type Option func(*Server)

// WithListen overrides the client listen host:port. Host "0.0.0.0"
// (default) exposes the port to worker pods via the cm Service.
func WithListen(host string, port int) Option {
	return func(s *Server) { s.host = host; s.port = port }
}

// WithStoreDir overrides the JetStream on-disk store directory.
func WithStoreDir(dir string) Option { return func(s *Server) { s.storeDir = dir } }

// WithServerName overrides the advertised server name.
func WithServerName(name string) Option { return func(s *Server) { s.serverName = name } }

// WithStreamMaxAge overrides the INVOCATIONS stream retention window.
func WithStreamMaxAge(d time.Duration) Option { return func(s *Server) { s.maxAge = d } }

// WithStreamMaxBytes caps the stream's on-disk size (0 = unlimited,
// bounded only by MaxAge).
func WithStreamMaxBytes(b int64) Option { return func(s *Server) { s.maxBytes = b } }

// New constructs the embedded server without starting it. It creates
// the JetStream store directory and builds the underlying nats-server;
// call Run to start accepting connections and to ensure the stream.
func New(opts ...Option) (*Server, error) {
	s := &Server{
		host:       "0.0.0.0",
		port:       DefaultPort,
		storeDir:   DefaultStoreDir,
		serverName: "clrk-cm",
		maxAge:     defaultStreamMaxAge,
	}
	for _, o := range opts {
		o(s)
	}

	if err := os.MkdirAll(s.storeDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir jetstream store %s: %w", s.storeDir, err)
	}

	// NoSigs: ctx owns the lifecycle, not signal handlers. NoLog: we
	// keep NATS quiet and emit our own lifecycle slog lines (the
	// server's logging calls are nil-guarded, so leaving the logger
	// unconfigured is safe).
	ns, err := natsserver.NewServer(&natsserver.Options{
		ServerName: s.serverName,
		Host:       s.host,
		Port:       s.port,
		JetStream:  true,
		StoreDir:   s.storeDir,
		NoSigs:     true,
		NoLog:      true,
	})
	if err != nil {
		return nil, fmt.Errorf("new nats server: %w", err)
	}
	s.srv = ns
	return s, nil
}

// Run starts the server, ensures the INVOCATIONS stream exists, then
// blocks until ctx is cancelled and drains with a graceful shutdown.
// Returns nil on clean cancellation; an error if startup fails.
func (s *Server) Run(ctx context.Context) error {
	slog.Info("Starting embedded NATS/JetStream",
		"host", s.host, "port", s.port, "store_dir", s.storeDir)
	s.srv.Start()
	if !s.srv.ReadyForConnections(readyTimeout) {
		s.srv.Shutdown()
		return fmt.Errorf("nats server not ready within %s", readyTimeout)
	}
	if err := s.ensureStream(ctx); err != nil {
		s.srv.Shutdown()
		s.srv.WaitForShutdown()
		return fmt.Errorf("ensure %s stream: %w", invevent.StreamName, err)
	}
	if err := s.ensureWorkerStatusKV(ctx); err != nil {
		s.srv.Shutdown()
		s.srv.WaitForShutdown()
		return fmt.Errorf("ensure %s kv: %w", invevent.WorkerStatusBucket, err)
	}
	slog.Info("Embedded NATS ready",
		"client_url", s.srv.ClientURL(), "stream", invevent.StreamName, "max_age", s.maxAge)

	<-ctx.Done()
	s.srv.Shutdown()
	s.srv.WaitForShutdown()
	return nil
}

// Ready blocks until the server accepts connections or ctx is done.
// cm-local clients (consumer/publisher/watch) call this before Connect
// since Run starts the server on a separate goroutine.
func (s *Server) Ready(ctx context.Context) error {
	for {
		if s.srv != nil && s.srv.ReadyForConnections(50*time.Millisecond) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Connect opens a connection to the embedded server over an in-process
// pipe (no TCP hop). For cm-local clients only; worker pods dial
// ClientURL over the network instead.
func (s *Server) Connect(name string) (*natsgo.Conn, error) {
	return natsgo.Connect("", natsgo.InProcessServer(s.srv), natsgo.Name(name))
}

// ClientURL is the TCP URL external clients (worker pods) dial.
func (s *Server) ClientURL() string { return s.srv.ClientURL() }

// ensureStream idempotently creates (or updates) the INVOCATIONS
// stream. Uses a short-lived in-process connection; the stream itself
// persists in JetStream independent of this connection.
func (s *Server) ensureStream(ctx context.Context) error {
	nc, err := s.Connect("clrk-cm-stream-admin")
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        invevent.StreamName,
		Description: "clrk Invocation lifecycle events",
		Subjects:    []string{invevent.StreamWildcard},
		Storage:     jetstream.FileStorage,
		Retention:   jetstream.LimitsPolicy,
		MaxAge:      s.maxAge,
		MaxBytes:    s.maxBytes,
	}); err != nil {
		return err
	}
	return nil
}

// ensureWorkerStatusKV idempotently creates (or updates) the WORKER_STATUS
// KV bucket workers publish their routing state into. Created before the
// server advertises ready so a worker's first Put and the cm's first Watch
// always find the bucket. History 1 (routing reads only the latest value
// per key); short TTL for dead-worker GC (see workerStatusKVTTL).
func (s *Server) ensureWorkerStatusKV(ctx context.Context) error {
	nc, err := s.Connect("clrk-cm-kv-admin")
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	if _, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      invevent.WorkerStatusBucket,
		Description: "clrk per-worker routing state (warm/in-flight/cached)",
		History:     1,
		TTL:         workerStatusKVTTL,
		Storage:     jetstream.FileStorage,
		Replicas:    1,
	}); err != nil {
		return err
	}
	return nil
}
