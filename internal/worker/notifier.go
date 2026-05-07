package worker

import "sync"

// changeNotifier is a tiny pub/sub primitive used by the worker's
// in-process state holders (activeCounter, SandboxManager) to wake
// the WorkerStatusService stream loop on every state mutation.
//
// Subscribers receive a coalesced edge-trigger: each subscriber's
// channel is a 1-buffer chan struct{}. broadcast() sends a
// non-blocking signal so a slow consumer that hasn't drained yet
// just sees the latest pending wakeup, never a backlog.
type changeNotifier struct {
	mu   sync.Mutex
	subs map[chan struct{}]struct{}
}

func newChangeNotifier() *changeNotifier {
	return &changeNotifier{subs: make(map[chan struct{}]struct{})}
}

// Subscribe returns a 1-buffer channel that receives a wakeup on
// every broadcast(). Caller MUST eventually call Unsubscribe with
// the same channel.
func (n *changeNotifier) Subscribe() chan struct{} {
	ch := make(chan struct{}, 1)
	n.mu.Lock()
	n.subs[ch] = struct{}{}
	n.mu.Unlock()
	return ch
}

// Unsubscribe removes ch from the subscriber set.
func (n *changeNotifier) Unsubscribe(ch chan struct{}) {
	n.mu.Lock()
	delete(n.subs, ch)
	n.mu.Unlock()
}

// broadcast wakes every current subscriber. Non-blocking: a
// subscriber whose buffer is full just keeps its existing pending
// signal — the consumer will re-snapshot state regardless of how
// many edges fired.
func (n *changeNotifier) broadcast() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for ch := range n.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
