// Package metadata implements the IMDS-style HTTP service exposed
// inside each sandbox. Agents that prefer not to (or can't) read
// CloudEvents from stdin fetch the request via $CLRK_METADATA_URL/event
// and POST the response back to $CLRK_METADATA_URL/response.
//
// The HTTP listener is owned per-RevisionStack and shared across all
// sandboxes of a given (TaskAgent, revision) on a worker; the per-
// dispatch *Entry is resolved on each request by source IP via the
// EntryLookup function the worker installs.
package metadata

import (
	"net/netip"
	"sync"
)

// EntryLookup resolves the live *Entry for a sandbox identified by
// its per-NIC source IP. Returns nil when no dispatch is in progress
// for that sandbox (e.g. warm sandbox waiting to be acquired) so the
// HTTP handler can answer 404 instead of serving a stale entry.
type EntryLookup func(srcIP netip.Addr) *Entry

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
