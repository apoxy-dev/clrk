package agents

import "k8s.io/apimachinery/pkg/types"

// Cross-platform test seams for internal/worker. Linux-only seams
// live in testseams_linux.go alongside the gvisor-/libcontainer-
// dependent surface.

// ActiveCounter aliases the unexported activeCounter so tests at
// apoxy-cloud//clrk/worker/ can name the value type. The exported
// methods (Snapshot, Notifier) are on the underlying type already;
// callers just invoke them directly.
type ActiveCounter = activeCounter

// ActiveCounterIncForTest exposes (*activeCounter).inc so tests at
// apoxy-cloud//clrk/worker/ can drive the inc/dec semantics without
// going through Dispatcher.ServeHTTP. inc/dec stay unexported in
// production callsites — only the request hot path is meant to touch
// them.
func ActiveCounterIncForTest(c *activeCounter, key types.NamespacedName) {
	c.inc(key)
}

// ActiveCounterDecForTest exposes (*activeCounter).dec.
func ActiveCounterDecForTest(c *activeCounter, key types.NamespacedName) {
	c.dec(key)
}
