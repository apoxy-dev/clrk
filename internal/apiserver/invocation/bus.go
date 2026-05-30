package invocation

import (
	"context"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/apoxy-dev/clrk/internal/invevent"
)

// Header names for the test-write door on the top-level Invocation
// resource. A Create/Update is permitted only when the apiserver was
// started with test writes enabled AND the request carries this header
// set to TestWriteHeaderValue. The handler chain (manager.go) reads it
// and stamps the request context via WithTestWrite.
const (
	TestWriteHeader      = "X-Clrk-Invocation-Write"
	TestWriteHeaderValue = "allow"
)

// NATSProvider is the subset of the embedded NATS server the apiserver
// needs to bring up the Invocation pub/sub: wait for readiness, then
// open an in-process connection. *internal/nats.Server satisfies it.
type NATSProvider interface {
	Ready(ctx context.Context) error
	Connect(name string) (*nats.Conn, error)
}

// Bus lazily holds the JetStream handle and Publisher once the embedded
// NATS server is ready. The three Storage GVRs hold a shared *Bus and
// degrade gracefully while it is unresolved: Watch returns an empty
// watch and the test-write door returns 405. The manager's background
// goroutine calls Set once it has connected, mirroring LazyPool's role
// for ClickHouse. Safe for concurrent use.
type Bus struct {
	mu    sync.RWMutex
	js    jetstream.JetStream
	pub   *invevent.Publisher
	ready bool
}

// NewBus returns an unresolved Bus.
func NewBus() *Bus { return &Bus{} }

// Set resolves the Bus with a live JetStream handle and Publisher.
// Idempotent in effect; the last writer wins (the manager calls it once).
func (b *Bus) Set(js jetstream.JetStream, pub *invevent.Publisher) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.js = js
	b.pub = pub
	b.ready = true
}

// JS returns the JetStream handle, or nil if the Bus is unresolved.
func (b *Bus) JS() jetstream.JetStream {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.js
}

// Publisher returns the Publisher, or nil if the Bus is unresolved.
func (b *Bus) Publisher() *invevent.Publisher {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.pub
}
