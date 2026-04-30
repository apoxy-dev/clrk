package extproc

import (
	"sync"
	"time"
)

// budgetKey identifies one daily counter bucket. Per route, per EG —
// two routes targeting the same provider on the same EG get separate
// budgets so operators can scope quotas independently.
type budgetKey struct {
	egNamespace    string
	egName         string
	routeNamespace string
	routeName      string
}

// budgetStore is the in-memory daily token counter.
//
// Scope: process-local. The controller-manager runs as a single replica
// for MVP (no leader election around ext_proc; an HA story needs a
// shared store and is explicitly out of scope per the plan). Restarts
// reset all counters — this is consistent with "very basic" and is
// observable to operators (a sudden ride to zero in dashboards lines
// up with restarts).
//
// Day boundary: the counter is partitioned by UTC date string. A call
// that crosses midnight UTC will see Allow() return true for the new
// day's bucket while the previous day's bucket still exists in the map
// for accounting. Old buckets are reaped on the next access of any
// key (cheap; the map is small).
type budgetStore struct {
	mu       sync.Mutex
	now      func() time.Time // injected for tests
	counters map[bucketKey]int64
}

// bucketKey is budgetKey plus the UTC date — this is what the map is
// actually keyed on, so day-old data ages out as soon as nothing
// references it.
type bucketKey struct {
	budgetKey
	date string // YYYY-MM-DD UTC
}

func newBudgetStore() *budgetStore {
	return &budgetStore{
		now:      time.Now,
		counters: make(map[bucketKey]int64),
	}
}

// Allow reports whether the daily counter for key is below cap. cap<=0
// is treated as "unlimited" — the budget filter has no daily cap to
// enforce. Callers that pass cap=0 (filter unset) get true.
func (b *budgetStore) Allow(key budgetKey, cap int64) bool {
	if cap <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	bk := b.bucket(key)
	return b.counters[bk] < cap
}

// Add increments key's daily counter by tokens. Negative or zero
// tokens are no-ops (defensive against parser quirks where usage was
// never present). Returns the new total for caller-side logging.
func (b *budgetStore) Add(key budgetKey, tokens int64) int64 {
	if tokens <= 0 {
		return b.snapshot(key)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	bk := b.bucket(key)
	b.counters[bk] += tokens
	return b.counters[bk]
}

// snapshot returns the current daily total for key without mutating.
// Locking is identical to Allow — this is for the slog deny path.
func (b *budgetStore) snapshot(key budgetKey) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.counters[b.bucket(key)]
}

// bucket resolves the per-day bucket and lazily reaps any bucket
// whose date is older than today. Caller must hold b.mu.
func (b *budgetStore) bucket(key budgetKey) bucketKey {
	today := b.now().UTC().Format("2006-01-02")
	for k := range b.counters {
		if k.date != today {
			delete(b.counters, k)
		}
	}
	return bucketKey{budgetKey: key, date: today}
}
