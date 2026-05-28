// Package admin hosts the controller-manager's system-state HTTP
// surface — a single mux under /admin/* that other subsystems
// register handlers on for runtime introspection.
//
// The contract for handlers is small and rigid:
//   - read-only — never mutate state through /admin
//   - cheap — handlers run on the controller-manager's request path
//     and a slow handler can hide back-pressure on the platform
//   - non-blocking — never wait on a Kubernetes API call or
//     downstream service, all data must be in-process and pre-computed
//
// Auth posture: none. /admin is gated by network reachability; bind
// only to internal addresses (the default is loopback + the cluster's
// internal Service) so it isn't exposed externally. Same guarantee
// the controller-runtime health endpoint relies on.
//
// Candidates as the surface grows: egress sink registry contents,
// invocationctx store depth, worker health/picker snapshots, warmpool
// stack contents, OTLP forwarder registry + chwriter drop counters.
package admin

import (
	"net/http"
)

// Mux is the /admin/* HTTP mux. Construct with New(); register
// handlers via Mux.Register. Mount on the controller-manager's admin
// listener with mux.ServeHTTP — it acts as a normal http.Handler
// rooted at /admin.
type Mux struct {
	srv *http.ServeMux
}

// New returns an admin Mux. Subsystems add their handlers via
// Mux.Register.
func New() *Mux {
	return &Mux{srv: http.NewServeMux()}
}

// ServeHTTP satisfies http.Handler so callers can mount the mux at
// the root of an http.Server.
func (m *Mux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.srv.ServeHTTP(w, r)
}

// Register adds a custom handler under /admin/<path>. path must start
// with "/admin/".
func (m *Mux) Register(path string, h http.Handler) {
	m.srv.Handle(path, h)
}
