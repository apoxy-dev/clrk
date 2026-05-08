//go:build linux

package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
)

const (
	// warmReconcileInterval is the cadence of the periodic top-up +
	// drift-eviction sweep. Burst-time refill rides on Acquire-kicks
	// rather than this tick.
	warmReconcileInterval = 10 * time.Second

	// warmFillTimeout caps a single Create call (image pull + libcontainer
	// container Create + netns provisioning). Image pulls dominate.
	warmFillTimeout = 60 * time.Second

	// warmDeleteTimeout bounds a Delete call during shutdown / late-stop
	// cleanup so a hung libcontainer destroy can't stall the worker.
	warmDeleteTimeout = 10 * time.Second

	// defaultMaxWarmPerKey is the per-(ns,agent,revision) ceiling
	// applied when the WorkerPool's MaxExecutionsPerWorker is unset.
	defaultMaxWarmPerKey = 16
)

// WarmPool keeps a per-(ns,agent,revision) LIFO of pre-Created
// sandboxes. A warm sandbox skips image pull, libcontainer Create,
// rootfs mount, TAP+netns provisioning, and CA staging on the request
// hot path; the dispatcher only has to set egress + Start + pump
// stdin.
//
// Lifecycle: warm sandboxes sit in the SandboxReady phase. They
// remain Acquire-able until either the dispatcher consumes one (it
// becomes Running, then Stopped, then deleted on the standard
// one-shot teardown) or the reconciler evicts because the (ns,agent)
// no longer points at this revision.
//
// v1 limitations (filed in clrk-improvements.md):
//   - Egress backends/policy are re-resolved at consume time, but the
//     CA cert mounted into the rootfs is captured at warm time. EG CA
//     rotation invalidates warm sandboxes only at the next revision
//     bump.
//   - Spec changes that don't bump the revision (e.g. EgressRefs
//     change without an image change) don't evict warm sandboxes.
type WarmPool struct {
	sandboxMgr SandboxRuntime
	client     client.Client
	router     *egress.Router
	notifier   *changeNotifier
	poolName   string
	podName    string
	maxPerKey  int

	mu      sync.Mutex
	pools   map[WarmKey][]*SandboxInstance
	targets map[WarmKey]int
	filling map[WarmKey]bool

	stopOnce sync.Once
	stopCh   chan struct{}
}

// NewWarmPool constructs a WarmPool. The notifier is shared with the
// dispatcher's activeCounter so warm-pool inserts/evictions push
// immediately to the WorkerStatusService stream — same edge-trigger
// pattern in-flight changes use today.
func NewWarmPool(
	sandboxMgr SandboxRuntime,
	c client.Client,
	router *egress.Router,
	notifier *changeNotifier,
	poolName, podName string,
	maxPerKey int,
) *WarmPool {
	if maxPerKey <= 0 {
		maxPerKey = defaultMaxWarmPerKey
	}
	return &WarmPool{
		sandboxMgr: sandboxMgr,
		client:     c,
		router:     router,
		notifier:   notifier,
		poolName:   poolName,
		podName:    podName,
		maxPerKey:  maxPerKey,
		pools:      make(map[WarmKey][]*SandboxInstance),
		targets:    make(map[WarmKey]int),
		filling:    make(map[WarmKey]bool),
		stopCh:     make(chan struct{}),
	}
}

// Run blocks until ctx is cancelled or StopFill is called. Runs an
// immediate reconcile then a periodic sweep at warmReconcileInterval.
func (w *WarmPool) Run(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx).WithName("warmpool")
	log.Info("Starting warm pool reconciler", "interval", warmReconcileInterval, "maxPerKey", w.maxPerKey)

	w.reconcile(ctx)

	tick := time.NewTicker(warmReconcileInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-w.stopCh:
			return nil
		case <-tick.C:
			w.reconcile(ctx)
		}
	}
}

// Acquire returns a warm sandbox for key, or nil if the pool is
// empty. Caller owns the returned sandbox lifecycle (must Delete it
// after use, same as a cold-path Create). Triggers a background fill
// so the pool is ready for the next request before the next
// reconcile tick.
func (w *WarmPool) Acquire(key WarmKey) *SandboxInstance {
	w.mu.Lock()
	pool := w.pools[key]
	if len(pool) == 0 {
		w.mu.Unlock()
		return nil
	}
	// LIFO: most-recently-warmed sandbox is the most-likely-fresh
	// (kernel state, page cache, etc.).
	last := len(pool) - 1
	sb := pool[last]
	pool[last] = nil
	w.pools[key] = pool[:last]
	w.mu.Unlock()

	w.notifier.broadcast()
	w.kickFill(key)
	return sb
}

// kickFill ensures a fillLoop is running for key. A fillLoop tops the
// pool up toward target one sandbox at a time and exits when the
// pool reaches target or any error occurs. Concurrent kickFill calls
// for the same key dedupe to a single goroutine — the running loop
// re-reads target on each iteration so subsequent Acquires are
// covered without spawning more goroutines.
func (w *WarmPool) kickFill(key WarmKey) {
	w.mu.Lock()
	select {
	case <-w.stopCh:
		w.mu.Unlock()
		return
	default:
	}
	if w.filling[key] {
		w.mu.Unlock()
		return
	}
	if w.targets[key] == 0 {
		w.mu.Unlock()
		return
	}
	w.filling[key] = true
	w.mu.Unlock()
	go w.fillLoop(key)
}

// fillLoop creates warm sandboxes one at a time until pool reaches
// target, target drops to zero (revision drift), an error occurs, or
// stopCh fires. Bails on error rather than tight-looping; the next
// Acquire or reconcile tick will re-kick.
func (w *WarmPool) fillLoop(key WarmKey) {
	log := ctrl.Log.WithName("warmpool.fill").WithValues("warmKey", key.String())
	defer func() {
		w.mu.Lock()
		delete(w.filling, key)
		w.mu.Unlock()
	}()

	for {
		select {
		case <-w.stopCh:
			return
		default:
		}

		w.mu.Lock()
		have := len(w.pools[key])
		target := w.targets[key]
		w.mu.Unlock()
		if target == 0 || have >= target {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), warmFillTimeout)
		sb, err := w.fillOne(ctx, key)
		cancel()
		if err != nil {
			log.V(1).Info("Warm fill skipped", "err", err)
			return
		}

		w.mu.Lock()
		select {
		case <-w.stopCh:
			w.mu.Unlock()
			w.deleteWarm(sb.ID)
			return
		default:
		}
		w.pools[key] = append(w.pools[key], sb)
		w.mu.Unlock()
		w.notifier.broadcast()
		log.Info("Warmed sandbox", "sandboxID", sb.ID)
	}
}

// reconcile lists TaskAgents in this WorkerPool, updates per-key
// targets, evicts pool entries whose key dropped out of the expected
// set, and kicks fills toward target.
func (w *WarmPool) reconcile(ctx context.Context) {
	log := ctrl.LoggerFrom(ctx).WithName("warmpool.reconcile")

	select {
	case <-w.stopCh:
		return
	default:
	}

	var tas clrkv1alpha1.TaskAgentList
	if err := w.client.List(ctx, &tas); err != nil {
		log.Error(err, "Listing TaskAgents")
		return
	}

	expected := make(map[WarmKey]int, len(tas.Items))
	for i := range tas.Items {
		ta := &tas.Items[i]
		if ta.Spec.WorkerPoolRef != w.poolName {
			continue
		}
		if ta.Spec.WarmPoolSize == nil || *ta.Spec.WarmPoolSize <= 0 {
			continue
		}
		if ta.Status.LatestReadyRevisionName == "" {
			continue
		}
		size := int(*ta.Spec.WarmPoolSize)
		if size > w.maxPerKey {
			size = w.maxPerKey
		}
		key := WarmKey{Namespace: ta.Namespace, Agent: ta.Name, Revision: ta.Status.LatestReadyRevisionName}
		expected[key] = size
	}

	// Snapshot drift-evict candidates and refresh targets in a single
	// lock acquisition.
	w.mu.Lock()
	var toEvict []*SandboxInstance
	for key, pool := range w.pools {
		if _, want := expected[key]; want {
			continue
		}
		toEvict = append(toEvict, pool...)
		delete(w.pools, key)
	}
	w.targets = expected
	w.mu.Unlock()

	for _, sb := range toEvict {
		w.deleteWarm(sb.ID)
	}
	if len(toEvict) > 0 {
		w.notifier.broadcast()
	}

	for key := range expected {
		w.kickFill(key)
	}
}

// fillOne resolves the TA's current spec and creates one warm
// sandbox for key. Does NOT touch w.pools — caller appends.
func (w *WarmPool) fillOne(ctx context.Context, key WarmKey) (*SandboxInstance, error) {
	var ta clrkv1alpha1.TaskAgent
	if err := w.client.Get(ctx, types.NamespacedName{Namespace: key.Namespace, Name: key.Agent}, &ta); err != nil {
		return nil, fmt.Errorf("get TaskAgent: %w", err)
	}
	// Drift check: TA may have rolled to a new revision since we
	// computed the expected set.
	if ta.Status.LatestReadyRevisionName != key.Revision {
		return nil, fmt.Errorf("revision drift: TA at %q, key at %q", ta.Status.LatestReadyRevisionName, key.Revision)
	}

	var rev clrkv1alpha1.AgentSandboxRevision
	if err := w.client.Get(ctx, types.NamespacedName{Namespace: key.Namespace, Name: key.Revision}, &rev); err != nil {
		return nil, fmt.Errorf("get AgentSandboxRevision: %w", err)
	}

	// Egress backends/policy aren't pinned — the dispatcher re-resolves
	// and applies them at consume time. The CA cert IS pinned via the
	// rootfs trust mount, hence resolved here.
	caPEM, _, _, err := resolveEgressForExecution(ctx, w.client, w.router, key.Namespace, ta.Spec.EgressRefs)
	if err != nil {
		return nil, fmt.Errorf("resolve egress for warm fill: %w", err)
	}

	sandboxID, err := newSandboxID(sandboxIDPrefixWarm, key.Namespace, key.Agent)
	if err != nil {
		return nil, err
	}
	identity := newAgentIdentity(proxyproto.AgentKindTask, key.Namespace, key.Agent, string(ta.UID), rev.Name)

	w.sandboxMgr.Purge(ctx, sandboxID)
	sb, err := w.sandboxMgr.Create(ctx, sandboxID, key.Agent, identity, caPEM, rev.Spec.AgentSandbox, ta.Spec.Resources, ta.Spec.State, true)
	if err != nil {
		return nil, fmt.Errorf("sandbox Create: %w", err)
	}
	return sb, nil
}

// StopFill prevents new warm sandboxes from being created. Existing
// warm sandboxes stay Acquire-able until DestroyAll runs — late-
// arriving in-flight requests can still hit the warm tier while we
// wait for active executions to finish.
func (w *WarmPool) StopFill() {
	w.stopOnce.Do(func() { close(w.stopCh) })
}

// DestroyAll tears down every remaining warm sandbox in parallel.
// Called during drain after StopFill + in-flight wait, so all pooled
// sandboxes are guaranteed to be unconsumed.
func (w *WarmPool) DestroyAll(ctx context.Context) {
	w.mu.Lock()
	var all []*SandboxInstance
	for _, pool := range w.pools {
		all = append(all, pool...)
	}
	w.pools = make(map[WarmKey][]*SandboxInstance)
	w.mu.Unlock()

	if len(all) == 0 {
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(all))
	for _, sb := range all {
		sb := sb
		go func() {
			defer wg.Done()
			w.deleteWarm(sb.ID)
		}()
	}
	wg.Wait()
	w.notifier.broadcast()
}

// deleteWarm wraps SandboxManager.Delete with a bounded context so a
// hung libcontainer destroy can't stall a fill loop or shutdown.
func (w *WarmPool) deleteWarm(id SandboxID) {
	ctx, cancel := context.WithTimeout(context.Background(), warmDeleteTimeout)
	defer cancel()
	if err := w.sandboxMgr.Delete(ctx, id); err != nil && !errors.Is(err, ErrNotFound) {
		ctrl.Log.WithName("warmpool").Error(err, "Deleting warm sandbox", "sandboxID", id)
	}
}
