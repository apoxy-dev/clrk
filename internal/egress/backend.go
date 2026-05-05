package egress

// BackendListener describes one EgressGateway listener the worker can
// dial. Each EgressListener in a CRD becomes one entry; the dialer
// picks at connect time based on the sandbox's destination port and
// the per-listener priority.
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
	// Carried for log/diagnostic surface; the dialer uses Priority
	// for tie-breaking, not Shape.
	Shape string

	// MatchPort, when non-zero, narrows this listener to the
	// sandbox's destination port. Zero = catch-all for this shape.
	// (Distinct from EgressListenerStatus.Port, which is the *bind*
	// port the EG-managed Envoy listens on.)
	MatchPort int32

	// Priority is the precomputed shape rank used as a tiebreaker
	// when multiple listeners match the same sandbox dst port.
	// Higher wins. Set from clrkv1alpha1.ShapePriority at config time
	// so the dial path doesn't pay a string-switch per connection.
	Priority int
}
