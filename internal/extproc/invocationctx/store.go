// Package invocationctx is the controller-manager-local map that
// carries a TaskAgent invocation's W3C trace parent context from the
// ingress edge to every outbound LLM/MCP call the agent makes.
//
// The flow:
//
//   - ingress ext_proc extracts (or synthesizes) a parent SpanContext
//     from the inbound HTTP request, generates an invocation-id, and
//     calls Put.
//   - dispatcher uses the same invocation-id as the per-sandbox
//     IdentityDialer's PROXY v2 invocation.id TLV.
//   - egress ext_proc reads invocation.id off the dynamic metadata
//     populated by the proxy_protocol listener filter and calls Get
//     to recover the parent. If the outbound request has no
//     traceparent, the egress filter mutates the request to inject
//     one derived from this parent — so the upstream LLM / MCP
//     server sees a continuation of the inbound trace, even when the
//     agent SDK isn't OTel-instrumented.
//
// Storage is in-memory with a TTL reaper. Single controller-manager
// replica today; restart loses in-flight contexts. An HA story (Redis
// or similar) is a follow-up.
package invocationctx

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// DefaultTTL bounds an invocation's parent-context lifetime in the
// store. Longer than the longest TaskAgent.spec.timeoutSeconds the
// dispatcher caps at (5 min today) so an in-flight invocation always
// finds its parent on every outbound call. The reaper drops entries
// past TTL on its next pass.
const DefaultTTL = 5 * time.Minute

// reapInterval bounds how stale an expired entry can sit before the
// reaper drops it. Picked small relative to DefaultTTL so memory
// growth from short-lived bursts of invocations stays bounded.
const reapInterval = 30 * time.Second

// Store maps invocation-id → parent SpanContext for the lifetime of
// each invocation (TTL-bounded). Safe for concurrent use.
type Store struct {
	mu      sync.RWMutex
	entries map[string]entry
	ttl     time.Duration

	now func() time.Time

	stop chan struct{}
}

type entry struct {
	parent   trace.SpanContext
	expireAt time.Time
}

// NewStore returns a Store with DefaultTTL. The caller must invoke
// Run on a goroutine to drive the reaper, and Close at shutdown.
func NewStore() *Store {
	return NewStoreWithTTL(DefaultTTL)
}

// NewStoreWithTTL is the test seam for picking a non-default TTL. In
// production, prefer NewStore.
func NewStoreWithTTL(ttl time.Duration) *Store {
	return &Store{
		entries: make(map[string]entry),
		ttl:     ttl,
		now:     time.Now,
		stop:    make(chan struct{}),
	}
}

// Put records the parent SpanContext for invocationID. Overwrites any
// existing entry. Empty invocationID and invalid SpanContexts are
// silently dropped — the caller (ingress) shouldn't be calling Put
// in those cases anyway, but defensive cheap.
func (s *Store) Put(invocationID string, parent trace.SpanContext) {
	if invocationID == "" || !parent.IsValid() {
		return
	}
	s.mu.Lock()
	s.entries[invocationID] = entry{
		parent:   parent,
		expireAt: s.now().Add(s.ttl),
	}
	s.mu.Unlock()
}

// Get returns the parent SpanContext for invocationID, or an invalid
// (zero) SpanContext when no live entry exists. ok reports whether a
// valid entry was found.
//
// Expired entries are not returned even when the reaper hasn't yet
// purged them — the on-read TTL check keeps the in-memory store
// behaving like a single-source-of-truth.
func (s *Store) Get(invocationID string) (trace.SpanContext, bool) {
	if invocationID == "" {
		return trace.SpanContext{}, false
	}
	s.mu.RLock()
	e, ok := s.entries[invocationID]
	s.mu.RUnlock()
	if !ok {
		return trace.SpanContext{}, false
	}
	if !s.now().Before(e.expireAt) {
		return trace.SpanContext{}, false
	}
	return e.parent, true
}

// Delete drops the entry for invocationID. Called by ingress on
// stream end if we want to free the slot eagerly; the reaper handles
// the case where Delete isn't called.
func (s *Store) Delete(invocationID string) {
	if invocationID == "" {
		return
	}
	s.mu.Lock()
	delete(s.entries, invocationID)
	s.mu.Unlock()
}

// Run drives the background reaper until ctx is cancelled or Close is
// called. Blocks; intended to run in its own goroutine.
func (s *Store) Run(ctx context.Context) {
	t := time.NewTicker(reapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		case <-t.C:
			s.reap()
		}
	}
}

// Close stops the reaper goroutine. Idempotent.
func (s *Store) Close() {
	select {
	case <-s.stop:
		// already closed
	default:
		close(s.stop)
	}
}

func (s *Store) reap() {
	now := s.now()
	s.mu.Lock()
	for k, e := range s.entries {
		if !now.Before(e.expireAt) {
			delete(s.entries, k)
		}
	}
	s.mu.Unlock()
}

// Len returns the live entry count (post-TTL filter). Test seam.
func (s *Store) Len() int {
	now := s.now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, e := range s.entries {
		if now.Before(e.expireAt) {
			n++
		}
	}
	return n
}
