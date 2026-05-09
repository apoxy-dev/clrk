//go:build linux

package metadata

import (
	"context"
	"fmt"
	"net/http"
	"net/netip"
	"sync"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"

	"github.com/apoxy-dev/clrk/internal/ports"
)

// shutdownGrace caps how long we wait for in-flight metadata
// requests to drain when the dispatch goroutine ends. Two
// in-progress reads max — agent's GET /v1/event and POST
// /v1/response — so this is a generous upper bound.
const shutdownGrace = 2 * time.Second

// Server is the per-execution IMDS server. It binds two TCP
// listeners on the per-sandbox gVisor stack — IPv4 and IPv6 —
// each on the well-known IMDS address at port 80, and serves
// /v1/event + /v1/response from the supplied Entry.
type Server struct {
	entry  *Entry
	httpV4 *http.Server
	httpV6 *http.Server

	wg sync.WaitGroup
}

// New starts the metadata server. The caller is responsible for
// invoking Close when the dispatch goroutine ends; Close is also
// safe to call after the entry's Done channel fires (the agent
// already posted a response).
func New(stk *stack.Stack, nicID tcpip.NICID, entry *Entry) (*Server, error) {
	s := &Server{entry: entry}

	v4Addr, err := tcpAddr(ports.MetadataAddrV4)
	if err != nil {
		return nil, fmt.Errorf("parse v4 addr: %w", err)
	}
	listV4, err := gonet.ListenTCP(stk, tcpip.FullAddress{
		NIC:  nicID,
		Addr: v4Addr,
		Port: uint16(ports.MetadataPort),
	}, ipv4.ProtocolNumber)
	if err != nil {
		return nil, fmt.Errorf("listen v4 %s:%d: %w", ports.MetadataAddrV4, ports.MetadataPort, err)
	}

	v6Addr, err := tcpAddr(ports.MetadataAddrV6)
	if err != nil {
		listV4.Close()
		return nil, fmt.Errorf("parse v6 addr: %w", err)
	}
	listV6, err := gonet.ListenTCP(stk, tcpip.FullAddress{
		NIC:  nicID,
		Addr: v6Addr,
		Port: uint16(ports.MetadataPort),
	}, ipv6.ProtocolNumber)
	if err != nil {
		listV4.Close()
		return nil, fmt.Errorf("listen v6 [%s]:%d: %w", ports.MetadataAddrV6, ports.MetadataPort, err)
	}

	mux := NewHandler(entry)

	s.httpV4 = &http.Server{Handler: mux}
	s.httpV6 = &http.Server{Handler: mux}

	s.wg.Add(2)
	go func() { defer s.wg.Done(); s.httpV4.Serve(listV4) }()
	go func() { defer s.wg.Done(); s.httpV6.Serve(listV6) }()

	return s, nil
}

// Close stops both HTTP listeners. Idempotent.
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	_ = s.httpV4.Shutdown(ctx)
	_ = s.httpV6.Shutdown(ctx)
	s.wg.Wait()
	return nil
}

func tcpAddr(s string) (tcpip.Address, error) {
	addr, err := netip.ParseAddr(s)
	if err != nil {
		return tcpip.Address{}, err
	}
	if addr.Is4() {
		return tcpip.AddrFrom4(addr.As4()), nil
	}
	return tcpip.AddrFrom16(addr.As16()), nil
}
