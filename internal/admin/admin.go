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
// Current handlers:
//
//	GET /admin/ingress-otlp   ingress.dispatch OTLP emitter state
//
// Candidates as the surface grows: egress sink registry contents,
// invocationctx store depth, worker health/picker snapshots, warmpool
// stack contents.
package admin

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

// Mux is the /admin/* HTTP mux. Construct with New(); register
// handlers via Mux.Register or via subsystem-specific helpers (e.g.
// IngressOTLPStatus's SetIngressOTLPStatus updates state through a
// Mux-owned atomic). Mount on the controller-manager's admin
// listener with mux.ServeHTTP — it acts as a normal http.Handler
// rooted at /admin.
type Mux struct {
	srv *http.ServeMux

	// ingressOTLP is the live state for /admin/ingress-otlp. Stored
	// in an atomic.Pointer so the reconciler can update it without
	// holding a lock during request handling.
	ingressOTLP atomic.Pointer[IngressOTLPStatus]
}

// New returns an admin Mux with all built-in routes registered. The
// initial IngressOTLPStatus is the zero value (noop, no endpoint).
func New() *Mux {
	m := &Mux{srv: http.NewServeMux()}
	m.ingressOTLP.Store(&IngressOTLPStatus{Noop: true})
	m.srv.HandleFunc("/admin/ingress-otlp", m.handleIngressOTLP)
	return m
}

// ServeHTTP satisfies http.Handler so callers can mount the mux at
// the root of an http.Server.
func (m *Mux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.srv.ServeHTTP(w, r)
}

// Register adds a custom handler under /admin/<path>. Use this only
// for new subsystem surfaces that don't have a typed helper here.
// path must start with "/admin/".
func (m *Mux) Register(path string, h http.Handler) {
	m.srv.Handle(path, h)
}

// IngressOTLPStatus is the JSON-serialized state of the ingress
// ext_proc's OTLP emitter — what the reconciler last applied.
// Updated by SetIngressOTLPStatus from the reconciler after every
// reconcile (whether it swapped or short-circuited).
type IngressOTLPStatus struct {
	// EgressGateway is "namespace/name" of the resolved EG, or empty
	// when no EG was resolved.
	EgressGateway string `json:"egress_gateway,omitempty"`

	// Endpoint is the OTLP endpoint currently exporting through.
	// Empty when the emitter is the Noop fallback.
	Endpoint string `json:"endpoint,omitempty"`

	// LastSwapTime is the wall-clock time the active emitter was
	// last replaced (or first installed at controller-manager boot).
	// Zero when nothing has been installed yet.
	LastSwapTime time.Time `json:"last_swap_time,omitempty"`

	// LastReason is a short human label for the most recent
	// state-changing reconcile: "cold_boot" / "swap" / "swap_to_noop"
	// / "no_eg" / "build_failed". Reconciles where the spec didn't
	// change do not republish, so this label stays sticky on the last
	// real state transition. Operators consult this from the
	// integration test and from manual debugging.
	LastReason string `json:"last_reason,omitempty"`

	// Noop is true when the active emitter is otelemit.Noop() — i.e.
	// no span will reach a backend. The integration test polls this
	// to gate driving the ext_proc with real requests.
	Noop bool `json:"noop"`
}

// SetIngressOTLPStatus atomically replaces the published
// /admin/ingress-otlp state. The reconciler calls this after every
// reconcile so the surface reflects steady-state.
func (m *Mux) SetIngressOTLPStatus(s IngressOTLPStatus) {
	m.ingressOTLP.Store(&s)
}

func (m *Mux) handleIngressOTLP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s := m.ingressOTLP.Load()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s)
}
