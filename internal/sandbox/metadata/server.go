//go:build linux

package metadata

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"

	"github.com/apoxy-dev/clrk/internal/ports"
)

// shutdownGrace caps how long we wait for in-flight metadata
// requests to drain when the dispatch goroutine ends. Two
// in-progress reads max — agent's GET /v1/event and POST
// /v1/response — so this is a generous upper bound.
const shutdownGrace = 2 * time.Second

// Server is the per-RevisionStack IMDS server. One TCP listener on
// the shared gVisor stack at 169.254.169.254:80; the handler resolves
// the per-dispatch *Entry per request via the supplied EntryLookup
// (keyed by per-sandbox source IP).
//
// The listener is bound once at RevisionStack construction and stays
// up for the stack's lifetime — no per-dispatch listener churn.
type Server struct {
	httpV4 *http.Server
}

// New binds the metadata listener on the supplied gVisor stack on
// port 80 with a wildcard local address. Per-sandbox NICs added by
// RevisionStack.Attach carry 169.254.169.254 as a bound address so
// traffic the agent sends to that link-local destination is admitted
// to the stack and dispatched to this listener; binding the listener
// itself on a wildcard address avoids ordering issues where the
// listener would otherwise need a NIC to already own the IP at
// listen-time (the manager constructs the stack before any sandbox
// has attached).
//
// The v6 endpoint (fd00:ec2::254) is intentionally not bound here:
// existing agents reach IMDS via the v4 URL, and a second listener
// per RevisionStack would double the per-stack listener cost for no
// concrete consumer. Reintroduce when an actual v6 user shows up.
func New(stk *stack.Stack, lookup EntryLookup) (*Server, error) {
	listV4, err := gonet.ListenTCP(stk, tcpip.FullAddress{
		Port: uint16(ports.MetadataPort),
	}, ipv4.ProtocolNumber)
	if err != nil {
		return nil, fmt.Errorf("listen v4 :%d: %w", ports.MetadataPort, err)
	}

	mux := NewHandler(lookup)
	srv := &Server{httpV4: &http.Server{Handler: mux}}
	go func() { _ = srv.httpV4.Serve(listV4) }()

	return srv, nil
}

// Close stops the HTTP listener. Idempotent.
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	_ = s.httpV4.Shutdown(ctx)
	return nil
}

