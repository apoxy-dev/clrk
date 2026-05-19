// Package metadata implements the IMDS-style HTTP service exposed
// inside each sandbox. Agents that prefer not to (or can't) read
// CloudEvents from stdin fetch the request via $CLRK_METADATA_URL/event
// and POST the response back to $CLRK_METADATA_URL/response.
//
// Under runsc + sentrystack the HTTP listener is owned per-worker
// (one host-bound 127.0.0.1 listener shared by every sandbox on the
// node). Per-dispatch *Entry resolution is keyed by SandboxID
// extracted from a PROXY v2 TLV on the incoming conn — the Sentry-
// side TCP forwarder writes the frame when it intercepts a SYN to
// 169.254.169.254:80 or [fd00:ec2::254]:80.
package metadata

import (
	"sync"
)

// EntryLookup resolves the live *Entry for a sandbox identified by its
// worker-assigned SandboxID (carried over the wire as TLVSandboxID).
// Returns nil when no dispatch is in progress for that sandbox so the
// HTTP handler can answer 404 instead of serving a stale entry.
type EntryLookup func(sandboxID string) *Entry

// Registry tracks the live *Entry for each sandbox known to the
// worker's IMDS dispatcher. The worker dispatcher Registers per
// dispatch and the returned closer clears the slot on teardown.
//
// Registry is the central worker-side substitute for the per-revision
// slot table that used to live in revstack.go: under per-sandbox
// Sentry the IMDS listener is shared across all sandboxes on the
// worker, and demux happens via SandboxID rather than source IP.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]*Entry
}

// NewRegistry constructs an empty Registry. The worker process owns
// one of these for the lifetime of the runtime.
func NewRegistry() *Registry {
	return &Registry{entries: make(map[string]*Entry)}
}

// Register installs entry under sandboxID and returns a closer that
// clears the slot if (and only if) it still points at the same entry.
// Idempotent on the closer side — repeat calls after the first
// successful clear are no-ops.
func (r *Registry) Register(sandboxID string, entry *Entry) func() {
	r.mu.Lock()
	r.entries[sandboxID] = entry
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		if r.entries[sandboxID] == entry {
			delete(r.entries, sandboxID)
		}
		r.mu.Unlock()
	}
}

// Lookup returns the live *Entry for sandboxID or nil.
func (r *Registry) Lookup(sandboxID string) *Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.entries[sandboxID]
}

// Entry is the per-execution context shared between the dispatcher
// (writer) and the metadata HTTP server (reader). One Entry per
// Metadata-mode dispatch; never reused across executions.
type Entry struct {
	// CEID is the CloudEvents `id` attribute resolved by the
	// dispatcher (X-Clrk-Execution-ID -> x-request-id -> generated).
	CEID string

	// Attrs holds the binary-mode CloudEvents attributes (without the
	// `ce-` prefix on the keys: e.g. "source", "type", "subject",
	// "datacontenttype", and any extension attrs). Keys are
	// lowercase per the CE binding spec.
	Attrs map[string]string

	// ContentType is the original request's Content-Type, served as
	// the body's Content-Type from GET /v1/event in binary mode.
	ContentType string

	// Body is the buffered request body served from GET /v1/event.
	// Sized by the original Content-Length / spec.timeout; we
	// trust the dispatcher to enforce sane bounds.
	Body []byte

	// Done is closed when POST /v1/response delivers the response or
	// the dispatch goroutine cancels the entry. Either side reads
	// Done to know the exchange is over.
	Done chan struct{}

	mu        sync.Mutex
	respBody  []byte
	respCT    string
	delivered bool
	cancelled bool
}

// NewEntry constructs an Entry with a fresh Done channel.
func NewEntry(ceID, contentType string, body []byte, attrs map[string]string) *Entry {
	return &Entry{
		CEID:        ceID,
		Attrs:       attrs,
		ContentType: contentType,
		Body:        body,
		Done:        make(chan struct{}),
	}
}

// SetResponse records the agent's response body + content-type and
// signals Done. Idempotent: subsequent calls are silently dropped so
// a misbehaving agent posting twice can't crash the dispatcher.
// Returns true on first delivery, false on duplicates / cancel-races.
func (e *Entry) SetResponse(body []byte, contentType string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.delivered || e.cancelled {
		return false
	}
	e.respBody = body
	e.respCT = contentType
	e.delivered = true
	close(e.Done)
	return true
}

// Response returns the recorded response body, content-type, and
// whether the agent actually delivered one. Callers should select
// on Done before reading.
func (e *Entry) Response() (body []byte, contentType string, delivered bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.respBody, e.respCT, e.delivered
}

// CancelIfPending closes Done without recording a response if the
// agent never POSTed one. The dispatcher calls this on sandbox exit
// so any goroutine still selecting on Done unblocks. Distinct from
// SetResponse so the dispatcher can tell "agent posted empty body"
// (delivered=true, body nil) from "agent never posted" (delivered
// stays false).
func (e *Entry) CancelIfPending() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.delivered || e.cancelled {
		return
	}
	e.cancelled = true
	close(e.Done)
}
