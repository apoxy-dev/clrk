package extproc

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/apoxy-dev/clrk/internal/extproc/parsers"
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

// evaluateBudget runs the pre-flight TokenBudget check at request-
// headers time. Returns ("", 0, 0) when nothing should block the
// request; otherwise returns the route name plus current daily total
// and cap, and the caller is expected to short-circuit with a 429.
//
// Pre-flight matches with model="" because the request body hasn't
// been buffered yet — model-scoped rules don't enforce here (see
// routeTable.match for the rationale).
func (s *Server) evaluateBudget(routes *routeTable, eg types.NamespacedName, headers map[string]string) (route string, used, max int64) {
	if s.budget == nil || routes == nil || eg.Name == "" {
		return "", 0, 0
	}
	host, _ := splitHostPort(headers[":authority"])
	system := parsers.SystemFor(host)
	if system == "" {
		return "", 0, 0
	}
	rr := routes.match(system, headers[":path"], "")
	if rr == nil || rr.tokenBudget == nil || rr.tokenBudget.MaxTokensPerDay == nil {
		return "", 0, 0
	}
	cap := *rr.tokenBudget.MaxTokensPerDay
	if cap <= 0 {
		return "", 0, 0
	}
	bk := budgetKey{
		egNamespace:    eg.Namespace,
		egName:         eg.Name,
		routeNamespace: rr.routeNamespace,
		routeName:      rr.routeName,
	}
	if s.budget.Allow(bk, cap) {
		return "", 0, 0
	}
	return rr.routeName, s.budget.snapshot(bk), cap
}

// chargeBudget increments the daily counter for the matched route by
// the parsed input+output token total. No-op when the route has no
// TokenBudget, the parser found no usage, or the request was denied
// at pre-flight (in which case nothing reached the upstream).
func (s *Server) chargeBudget(rr *routeRule, eg types.NamespacedName, rec Record) {
	if s.budget == nil || rr == nil || rr.tokenBudget == nil {
		return
	}
	if rec.BudgetDenied || rec.Provider == nil {
		return
	}
	tokens := rec.Provider.InputTokens + rec.Provider.OutputTokens
	if tokens <= 0 {
		return
	}
	s.budget.Add(budgetKey{
		egNamespace:    eg.Namespace,
		egName:         eg.Name,
		routeNamespace: rr.routeNamespace,
		routeName:      rr.routeName,
	}, tokens)
}
