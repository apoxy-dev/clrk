package metadata

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
)

// shutdownGrace caps how long we wait for in-flight metadata
// requests to drain when the dispatch goroutine ends. Two
// in-progress reads max — agent's GET /v1/event and POST
// /v1/response — so this is a generous upper bound.
const shutdownGrace = 2 * time.Second

// proxyHandshakeTimeout bounds how long the listener wraps each
// accepted conn waiting for the PROXY v2 frame to arrive. The
// frame is only ~64 bytes, so a generous half-second deadline
// catches misbehaving callers without delaying real traffic.
const proxyHandshakeTimeout = 500 * time.Millisecond

// Server is the per-worker IMDS server. One TCP listener on
// 127.0.0.1:<port>; the handler resolves the per-dispatch *Entry
// per request via the supplied EntryLookup keyed by the SandboxID
// TLV the Sentry-side TCP forwarder writes onto each accepted
// connection.
type Server struct {
	httpSrv *http.Server
	ln      net.Listener
}

// New binds an IMDS listener on hostAddr (typically "127.0.0.1:<port>")
// and starts serving in the background. Each accepted connection is
// wrapped to parse a leading PROXY v2 frame, extract its SandboxID
// TLV, and stash it on the request context for the handler.
func New(hostAddr string, lookup EntryLookup) (*Server, error) {
	ln, err := net.Listen("tcp", hostAddr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", hostAddr, err)
	}
	return newWithListener(ln, lookup), nil
}

// newWithListener builds a Server around an already-bound listener.
// Used by tests that drive the wrapper directly without binding a
// real port.
func newWithListener(ln net.Listener, lookup EntryLookup) *Server {
	wrapped := &proxyListener{Listener: ln}
	mux := NewHandler(lookup)
	srv := &http.Server{
		Handler: mux,
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			if pc, ok := c.(*proxyConn); ok {
				return WithSandboxID(ctx, pc.sandboxID)
			}
			return ctx
		},
	}
	s := &Server{httpSrv: srv, ln: wrapped}
	go func() { _ = srv.Serve(wrapped) }()
	return s
}

// Addr returns the listener's bound address. Tests use this to dial
// the server without hardcoding a port.
func (s *Server) Addr() net.Addr {
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Close stops the HTTP listener. Idempotent.
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	_ = s.httpSrv.Shutdown(ctx)
	return nil
}

// proxyListener wraps net.Listener so every accepted conn is fronted
// with the PROXY v2 parser before http.Server's Serve goroutine
// touches it. Returning a typed *proxyConn lets ConnContext fish out
// the parsed SandboxID without a side map.
type proxyListener struct {
	net.Listener
}

func (l *proxyListener) Accept() (net.Conn, error) {
	raw, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if err := raw.SetReadDeadline(time.Now().Add(proxyHandshakeTimeout)); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("set handshake deadline: %w", err)
	}
	hdr, err := proxyproto.ParseHeader(raw)
	if err != nil {
		_ = raw.Close()
		// Skip this conn but keep the listener alive — a single
		// misbehaving caller shouldn't take the IMDS server down.
		return nil, &transientError{err: err}
	}
	if err := raw.SetReadDeadline(time.Time{}); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("clear handshake deadline: %w", err)
	}
	return &proxyConn{Conn: raw, sandboxID: hdr.Identity.SandboxID}, nil
}

// proxyConn pairs the underlying net.Conn with the SandboxID parsed
// from its leading PROXY v2 frame. http.Server's ConnContext hook
// reads sandboxID off this struct to seed the request context.
type proxyConn struct {
	net.Conn
	sandboxID string
}

// transientError wraps a handshake-time parse failure so http.Server's
// Accept loop treats it as a temporary error and keeps serving. The
// net package's accept-loop heuristic looks at the Temporary() method.
type transientError struct{ err error }

func (e *transientError) Error() string   { return "proxyproto handshake: " + e.err.Error() }
func (e *transientError) Temporary() bool { return true }
func (e *transientError) Timeout() bool   { return errors.Is(e.err, context.DeadlineExceeded) }
