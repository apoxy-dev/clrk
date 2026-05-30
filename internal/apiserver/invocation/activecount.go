package invocation

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// nonTerminalPhases are the Invocation phases that count as in-flight for
// ActiveExecutions accounting. Succeeded / Failed / Timeout / Rejected
// are terminal and excluded.
var nonTerminalPhases = []clrkv1alpha1.InvocationPhase{
	clrkv1alpha1.InvocationPhasePending,
	clrkv1alpha1.InvocationPhaseDispatched,
	clrkv1alpha1.InvocationPhaseRunning,
}

// activeCountTimeout bounds a single count query. The shared LazyPool's
// Do blocks until the embedded ClickHouse dial resolves; this cap keeps
// a not-yet-ready (or absent) engine from stalling a reconcile, so the
// caller falls back to its prior source promptly.
const activeCountTimeout = 2 * time.Second

// activeRecencyWindow bounds how stale a non-terminal invocation's latest
// event may be before it stops counting as in-flight. Lifecycle events
// carry no heartbeat, so an invocation that never reaches a terminal
// phase — a worker that rejects a request ingress already published as
// Dispatched, a worker crash before the terminal event flushes, or a
// terminal event dropped after the worker's retry budget expires — would
// otherwise inflate ActiveExecutions until the days-scale ClickHouse TTL.
// The window must exceed the maximum execution time (worker hardTimeoutCap
// is 5m, since a legitimately-running invocation's newest event is its
// Running event); 15m gives a 3x margin for cold-start + clock skew while
// still aging orphans out promptly.
const activeRecencyWindow = 15 * time.Minute

// ActiveCounter answers "how many non-terminal invocations does this
// parent have right now" against the invocation_events read model. It is
// the source for TaskAgent.Status.ActiveExecutions (APO-620), replacing
// the per-worker gRPC-stream sum. Results are cached per parent for a
// short TTL so a reconcile storm doesn't hammer ClickHouse — sub-second
// staleness is irrelevant for a periodically-reconciled status field.
type ActiveCounter struct {
	pool Doer
	ttl  time.Duration

	mu    sync.Mutex
	cache map[string]cachedCount
}

type cachedCount struct {
	count   int32
	expires time.Time
}

// NewActiveCounter wraps a Doer (the shared LazyPool in prod). ttl<=0
// disables caching.
func NewActiveCounter(pool Doer, ttl time.Duration) *ActiveCounter {
	return &ActiveCounter{
		pool:  pool,
		ttl:   ttl,
		cache: make(map[string]cachedCount),
	}
}

// CountActive returns the number of non-terminal invocations for the
// given parent. The query reconstructs each invocation's current phase
// as argMax(phase, stream_seq) and counts those still in flight — the
// same highest-seq-wins rule the read path's Get/List use, so the count
// is consistent with what `clrk agents invocations` shows.
func (c *ActiveCounter) CountActive(ctx context.Context, namespace string, kind clrkv1alpha1.InvocationParentKind, name string) (int32, error) {
	key := namespace + "/" + string(kind) + "/" + name
	now := time.Now()
	if c.ttl > 0 {
		c.mu.Lock()
		if e, ok := c.cache[key]; ok && now.Before(e.expires) {
			c.mu.Unlock()
			return e.count, nil
		}
		c.mu.Unlock()
	}

	phases := make([]string, len(nonTerminalPhases))
	for i, p := range nonTerminalPhases {
		phases[i] = sqlString(string(p))
	}
	body := fmt.Sprintf(
		"SELECT count() AS n FROM ("+
			"SELECT invocation_id, argMax(phase, stream_seq) AS p, max(event_time) AS last_evt FROM %s.%s "+
			"WHERE namespace = %s AND parent_kind = %s AND parent_name = %s "+
			"GROUP BY invocation_id) "+
			"WHERE p IN (%s) AND last_evt >= now64(3, 'UTC') - toIntervalSecond(%d)",
		Database, Table,
		sqlString(namespace), sqlString(string(kind)), sqlString(name),
		strings.Join(phases, ", "),
		int(activeRecencyWindow/time.Second),
	)

	qctx, cancel := context.WithTimeout(ctx, activeCountTimeout)
	defer cancel()
	var n proto.ColUInt64
	if err := c.pool.Do(qctx, ch.Query{
		Body:   body,
		Result: proto.Results{{Name: "n", Data: &n}},
	}); err != nil {
		return 0, fmt.Errorf("count active invocations: %w", err)
	}
	var count int32
	if n.Rows() > 0 {
		count = int32(n.Row(0))
	}
	if c.ttl > 0 {
		c.mu.Lock()
		c.cache[key] = cachedCount{count: count, expires: now.Add(c.ttl)}
		c.mu.Unlock()
	}
	return count, nil
}
