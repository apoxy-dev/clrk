package extproc

import (
	"sync"
	"time"
)

// requestState carries the facts the downstream (listener-level)
// ext_proc stream establishes once per request — the pinned rule, the
// decoded source IR, the candidate set — over to the upstream
// (cluster-level) ext_proc streams that adapt each router attempt, and
// the per-attempt facts back. Both streams of one request terminate in
// this process (the controller-manager runs single-replica; the
// downstream and upstream filters point at the same gRPC service), so
// an in-process store keyed by Envoy's x-request-id — generated before
// the filter chain, stable across retry attempts, forwarded upstream —
// is sufficient correlation. Fields land with the upstream cutover;
// the store ships first so its lifecycle is testable in isolation.
type requestState struct {
	// createdAt drives TTL eviction for states orphaned by a
	// downstream stream that never reached its close path.
	createdAt time.Time
}

// requestStateTTL bounds how long an orphaned requestState survives.
// The owning downstream stream deletes its state on close; the TTL is
// purely a leak backstop, generous enough to outlive any legitimate
// transaction (Envoy's route timeout caps those much earlier).
const requestStateTTL = 15 * time.Minute

// requestStateStore maps x-request-id to the shared per-request state.
// One per Server.
type requestStateStore struct {
	mu     sync.Mutex
	states map[string]*requestState
}

func newRequestStateStore() *requestStateStore {
	return &requestStateStore{states: make(map[string]*requestState)}
}

// put registers st under requestID, evicting any expired entries while
// the lock is held (the map stays small: one entry per in-flight
// request on this EG fleet, so a linear sweep on write is cheap).
func (r *requestStateStore) put(requestID string, st *requestState) {
	if requestID == "" || st == nil {
		return
	}
	if st.createdAt.IsZero() {
		st.createdAt = time.Now()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := time.Now().Add(-requestStateTTL)
	for k, v := range r.states {
		if v.createdAt.Before(cutoff) {
			delete(r.states, k)
		}
	}
	r.states[requestID] = st
}

// get returns the state for requestID, or nil.
func (r *requestStateStore) get(requestID string) *requestState {
	if requestID == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.states[requestID]
}

// delete removes requestID's state. Called by the owning downstream
// stream on close.
func (r *requestStateStore) delete(requestID string) {
	if requestID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.states, requestID)
}
