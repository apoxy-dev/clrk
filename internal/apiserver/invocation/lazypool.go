package invocation

import (
	"context"
	"errors"
	"sync"

	"github.com/ClickHouse/ch-go"
)

// LazyPool is a Doer whose underlying connection is supplied
// asynchronously via Set. Calls to Do block until Set lands or ctx
// fires, whichever comes first. Used by the controller-manager so
// /healthz and the rest of the apiserver come up immediately while
// the embedded ClickHouse supervisor is still binding 9000 in
// parallel; without this, the apiserver's CH dial would hold the
// /healthz bind, the kubelet would trip liveness, and the pod would
// crash-loop before CH ever got a chance to start.
type LazyPool struct {
	ready chan struct{}
	once  sync.Once
	pool  Doer
	err   error
}

// NewLazyPool returns an unresolved LazyPool. Call Set once with the
// final Doer (or an error) to release any Do callers blocked on it.
func NewLazyPool() *LazyPool {
	return &LazyPool{ready: make(chan struct{})}
}

// Set installs the resolved Doer or a permanent dial error. Idempotent
// — only the first call wins, subsequent calls are no-ops.
func (l *LazyPool) Set(pool Doer, err error) {
	l.once.Do(func() {
		l.pool = pool
		l.err = err
		close(l.ready)
	})
}

// Do blocks until the pool is resolved, then forwards to it. If Set
// resolved with an error, Do returns that error wrapped with context
// so callers see a coherent "storage not ready" surface rather than a
// nil-pool panic.
func (l *LazyPool) Do(ctx context.Context, q ch.Query) error {
	select {
	case <-l.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	if l.err != nil {
		return errors.New("invocation storage unavailable: " + l.err.Error())
	}
	return l.pool.Do(ctx, q)
}

// Closer is the subset of *chpool.Pool we close at shutdown. Pulled
// out so Close works against the real pool without dragging chpool
// into every Doer implementation (e.g. unit-test fakes).
type Closer interface {
	Close()
}

// Close shuts down the resolved pool if Set landed a non-nil one;
// otherwise no-op. Safe to call after Start exits even if the dial
// goroutine never resolved — Close polls non-blockingly so it doesn't
// stall shutdown waiting for a failing dial.
func (l *LazyPool) Close() {
	select {
	case <-l.ready:
	default:
		return
	}
	if l.err != nil || l.pool == nil {
		return
	}
	if c, ok := l.pool.(Closer); ok {
		c.Close()
	}
}
