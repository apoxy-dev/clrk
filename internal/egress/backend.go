package egress

// BackendListener describes one EgressGateway listener the worker can
// dial. Each EgressListener in a CRD becomes one entry; the dialer
// picks at connect time based on the sandbox's destination port and
// the per-listener shape.
//
// Lives in a non-build-tagged file so callers compiled for darwin (the
// CLI, helpers in internal/worker that mix with linux-only sandbox
// code via separate files) can reference the type for plumbing.
type BackendListener struct {
	// Name mirrors the EgressListener name from the CRD; used for
	// logs and for EgressL4Route parentRef.sectionName matching.
	Name string

	// Addr is the host:port the worker dials. Empty means "this
	// listener exists but its data plane isn't ready yet" — the
	// dialer skips it and tries the next candidate.
	Addr string

	// Shape is the on-the-wire protocol selector
	// ("tls-terminate", "tls-passthrough", "tcp", "http", "https").
	// Used as a tiebreaker when multiple listeners match the dst
	// port and as the EgressL4Route resolution key.
	Shape string

	// Port, when non-nil, narrows this listener to the sandbox's
	// destination port. Nil = catch-all for this shape.
	Port *int32
}
