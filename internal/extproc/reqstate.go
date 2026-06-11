package extproc

import (
	"sync"
	"time"

	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
)

// requestState carries the facts the downstream (listener-level)
// ext_proc stream establishes once per request — the pinned rule, the
// decoded source IR — over to the upstream (cluster-level) ext_proc
// streams that adapt each router attempt, and the per-attempt facts
// back. Both streams of one request terminate in this process (the
// controller-manager runs single-replica; the downstream and upstream
// filters point at the same gRPC service), so an in-process store
// keyed by Envoy's x-request-id — generated before the filter chain,
// stable across retry attempts, forwarded upstream — is sufficient
// correlation.
//
// The pin-time fields are written once by the downstream stream before
// the state is published to the store and are read-only afterwards.
// attempts is appended by upstream streams (attempts are sequential —
// the router runs one at a time — but the TTL sweeper and the
// downstream reader run concurrently, hence the mutex).
type requestState struct {
	// createdAt drives TTL eviction for states orphaned by a
	// downstream stream that never reached its close path.
	createdAt time.Time

	// system is the canonical source schema the agent spoke (derived
	// from the original :authority host). model is the decoded request
	// model ("" when none was parsed).
	system string
	model  string

	// srcIR is the source-schema decode of the request, non-nil only
	// when the pinned rule's candidate set spans schemas (the decode
	// is skipped otherwise). Upstream attempts translate from this.
	srcIR *llmcall.Request

	// rule is the routeRule pinned at RequestBody EOS. Upstream
	// attempts resolve the picked endpoint's candidate facts
	// (modelRewrites, bodyMutation, host/port) and credential scope
	// from it.
	rule *routeRule

	mu       sync.Mutex
	attempts []attemptFact
}

// attemptFact records what one router attempt targeted and sent. The
// last entry is the serving attempt: once response headers reach the
// downstream filter no further retries are possible, so the downstream
// response side keys its translation arming and telemetry fold off it.
type attemptFact struct {
	backendNamespace string
	backendName      string
	backendSchema    string

	// translationApplied is true when the attempt sent a
	// cross-schema-translated request; tgt/xreq are the target
	// provider and translated IR the response-side decode needs.
	translationApplied bool
	tgt                *llmcall.Provider
	xreq               *llmcall.Request
	droppedExtras      int

	// sentPath/sentBody capture what this attempt actually sent
	// upstream when it rewrote the request (translation or
	// per-backend body rewrite); empty when the agent's bytes went
	// out untouched. Folded into the Record for the serving attempt,
	// preserving the capture convention that OTLP shows what the
	// upstream received.
	bodyRewritten bool
	sentPath      string
	sentBody      []byte
}

// appendAttempt records one attempt's facts.
func (st *requestState) appendAttempt(a attemptFact) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.attempts = append(st.attempts, a)
}

// lastAttempt returns a copy of the most recent attempt's facts, or
// nil when no upstream attempt has run (degraded pins, passthrough).
func (st *requestState) lastAttempt() *attemptFact {
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.attempts) == 0 {
		return nil
	}
	a := st.attempts[len(st.attempts)-1]
	return &a
}

// attemptCount returns how many attempts have recorded facts.
func (st *requestState) attemptCount() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.attempts)
}

// attemptBackends returns the ordered "<ns>/<name>" walk of the
// backends the attempts targeted, for telemetry.
func (st *requestState) attemptBackends() []string {
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.attempts) == 0 {
		return nil
	}
	out := make([]string, len(st.attempts))
	for i, a := range st.attempts {
		out[i] = a.backendNamespace + "/" + a.backendName
	}
	return out
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
