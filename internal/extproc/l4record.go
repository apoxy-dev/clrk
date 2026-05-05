package extproc

import "time"

// L4Record is the structured representation of one TCP/TLS connection
// observed through the network ext_proc filter on the egress
// listener's TCP-fallback chain. Distinct from Record (HTTP) because
// at L4 there are no headers, no body, and no GenAI parser hooks —
// just connection-level facts.
//
// Bytes counted are the read direction only: ProcessingMode is
// STREAMED on read and SKIP on write, which means we observe agent →
// server bytes (the audit direction) and stay out of the server →
// agent path. Connection duration is wall-clock between the first
// ReadData event and stream close.
type L4Record struct {
	Timestamp time.Time
	EndAt     time.Time

	// Agent identity, populated from the clrk.apoxy.dev dynamic
	// metadata namespace forwarded by Envoy on the first
	// ProcessingRequest (same shape the HTTP path reads).
	AgentKind      string
	AgentNamespace string
	AgentName      string
	AgentUID       string
	AgentRevision  string
	InvocationID   string

	// BytesUpstream is the count of agent → server bytes observed on
	// this connection. Server → agent bytes are unobserved by design.
	BytesUpstream int64
}
