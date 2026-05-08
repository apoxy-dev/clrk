//go:build linux

package metadata

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"

	"github.com/apoxy-dev/clrk/internal/ports"
)

const (
	contentTypeCloudEventsJSON = "application/cloudevents+json"

	// shutdownGrace caps how long we wait for in-flight metadata
	// requests to drain when the dispatch goroutine ends. Two
	// in-progress reads max — agent's GET /v1/event and POST
	// /v1/response — so this is a generous upper bound.
	shutdownGrace = 2 * time.Second
)

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

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/event", s.handleEvent)
	mux.HandleFunc("/v1/response", s.handleResponse)

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

// handleEvent serves the request envelope. Binary mode by default
// (ce-* response headers + raw body). Structured mode when the
// caller advertises Accept: application/cloudevents+json.
func (s *Server) handleEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}

	if accepts(r, contentTypeCloudEventsJSON) {
		env := buildStructuredEnvelope(s.entry)
		w.Header().Set("Content-Type", contentTypeCloudEventsJSON)
		_ = json.NewEncoder(w).Encode(env)
		return
	}

	for k, v := range s.entry.Attrs {
		w.Header().Set("ce-"+k, v)
	}
	w.Header().Set("ce-id", s.entry.CEID)
	if s.entry.ContentType != "" {
		w.Header().Set("Content-Type", s.entry.ContentType)
	}
	_, _ = w.Write(s.entry.Body)
}

// handleResponse records the agent's response and signals Done.
// 204 on first delivery, 409 on duplicates so the agent's
// retry-after-failure path doesn't accidentally clobber a delivery
// that already shipped.
func (s *Server) handleResponse(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if !s.entry.SetResponse(body, r.Header.Get("Content-Type")) {
		http.Error(w, "response already delivered", http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// buildStructuredEnvelope produces a structured-mode CloudEvents
// JSON object. `data` is inlined for JSON (as raw JSON) and text/*
// (as a string); everything else is base64-encoded into
// `data_base64` per the CE JSON binding.
func buildStructuredEnvelope(e *Entry) map[string]any {
	env := map[string]any{
		"specversion": "1.0",
		"id":          e.CEID,
	}
	for k, v := range e.Attrs {
		// Skip reserved attrs we set explicitly.
		if k == "specversion" || k == "id" {
			continue
		}
		env[k] = v
	}
	switch {
	case isJSONContentType(e.ContentType) && len(e.Body) > 0:
		env["data"] = json.RawMessage(e.Body)
	case isTextContentType(e.ContentType):
		env["data"] = string(e.Body)
	default:
		env["data_base64"] = base64.StdEncoding.EncodeToString(e.Body)
	}
	return env
}

// isJSONContentType reports whether ct is application/json or a
// JSON-suffixed type (per RFC 6838 +json suffix).
func isJSONContentType(ct string) bool {
	mt := strings.SplitN(ct, ";", 2)[0]
	mt = strings.ToLower(strings.TrimSpace(mt))
	return mt == "application/json" || strings.HasSuffix(mt, "+json")
}

// isTextContentType reports whether ct is text/* (text/plain, etc.)
// — UTF-8 string-able payloads.
func isTextContentType(ct string) bool {
	mt := strings.SplitN(ct, ";", 2)[0]
	mt = strings.ToLower(strings.TrimSpace(mt))
	return strings.HasPrefix(mt, "text/")
}

// accepts is a Q-value-ignorant Accept-header check. Sufficient for
// our cloudevents+json toggle — caller is the agent inside the
// sandbox, not a browser.
func accepts(r *http.Request, mediaType string) bool {
	for _, h := range r.Header.Values("Accept") {
		for _, part := range strings.Split(h, ",") {
			mt := strings.SplitN(strings.TrimSpace(part), ";", 2)[0]
			if strings.EqualFold(mt, mediaType) {
				return true
			}
		}
	}
	return false
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
