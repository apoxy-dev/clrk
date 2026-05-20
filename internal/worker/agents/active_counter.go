package agents

import (
	"sync"

	"k8s.io/apimachinery/pkg/types"
)

// activeCounter tracks in-flight TaskAgent executions per
// (taskAgentNS, taskAgentName). The WorkerStatusService streams the
// snapshot to the controller-manager which feeds it into the ingress
// ext_proc cluster-wide MaxConcurrent enforcement.
//
// Kept in an untagged file so cross-platform test seams in
// testseams.go can reference the type without dragging in the
// libcontainer/gvisor surface that lives behind //go:build linux.
type activeCounter struct {
	mu       sync.Mutex
	counts   map[types.NamespacedName]int32
	notifier *changeNotifier
}

func newActiveCounter() *activeCounter {
	return &activeCounter{
		counts:   make(map[types.NamespacedName]int32),
		notifier: newChangeNotifier(),
	}
}

func (c *activeCounter) inc(key types.NamespacedName) {
	c.mu.Lock()
	c.counts[key]++
	c.mu.Unlock()
	c.notifier.broadcast()
}

func (c *activeCounter) dec(key types.NamespacedName) {
	c.mu.Lock()
	if c.counts[key] > 0 {
		c.counts[key]--
	}
	if c.counts[key] == 0 {
		delete(c.counts, key)
	}
	c.mu.Unlock()
	c.notifier.broadcast()
}

// get returns the current count for key (0 if absent).
func (c *activeCounter) get(key types.NamespacedName) int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[key]
}

// Snapshot returns a copy of all (key, count) pairs. Used by the
// WorkerStatusService to build a stream message.
func (c *activeCounter) Snapshot() map[types.NamespacedName]int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[types.NamespacedName]int32, len(c.counts))
	for k, v := range c.counts {
		out[k] = v
	}
	return out
}

// Notifier returns the change notifier for this counter so external
// publishers (e.g. the WorkerStatusService) can subscribe to inc/dec
// events.
func (c *activeCounter) Notifier() *changeNotifier { return c.notifier }

// NewActiveCounter returns an empty activeCounter usable as the
// `active` argument to NewDispatcher. Exported so tests can inspect
// the counter independently.
func NewActiveCounter() *activeCounter { return newActiveCounter() }
