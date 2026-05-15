package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
)

const (
	// warmFillTimeout caps a single Create call (image pull + libcontainer
	// container Create + netns provisioning). Image pulls dominate.
	warmFillTimeout = 60 * time.Second

	// warmDeleteTimeout bounds a Delete call during shutdown / late-stop
	// cleanup so a hung libcontainer destroy can't stall the worker.
	warmDeleteTimeout = 10 * time.Second

	// defaultMaxWarmPerKey is the per-(ns,agent,revision) ceiling
	// applied when the WorkerPool's MaxExecutionsPerWorker is unset.
	defaultMaxWarmPerKey = 16

	// defaultWarmIdleTTL caps how long a warm sandbox can sit unused
	// before it's recycled. Bounds resource-leak accumulation in
	// long-idle agent processes. Applied when
	// TaskAgent.Spec.WarmPoolIdleTTL is unset or non-positive.
	defaultWarmIdleTTL = 10 * time.Minute
)

// warmTarget is the per-key desired state. size is the pool target;
// idleTTL is the TTL stamped on slots filled while this target was
// in effect.
type warmTarget struct {
	size    int
	idleTTL time.Duration
}

// WarmPool keeps a per-(ns,agent,revision) LIFO of pre-Created
// sandboxes. A warm sandbox skips image pull, libcontainer Create,
// rootfs mount, TAP+netns provisioning, and CA staging on the request
// hot path; the dispatcher only has to set egress + Start + pump
// stdin.
//
// Reconciliation is event-driven: WarmPool implements
// reconcile.Reconciler for TaskAgent and is wired to the
// controller-runtime Manager via SetupWithManager. A TA event (create,
// status update bumping LatestReadyRevisionName, spec change, delete)
// reconciles that single key. Per-slot freshness is enforced by a
// time.AfterFunc scheduled at fill time and cleared on Acquire.
//
// Lifecycle: warm sandboxes sit in the SandboxReady phase. They
// remain Acquire-able until either the dispatcher consumes one (it
// becomes Running, then Stopped, then deleted on the standard
// one-shot teardown), the reconciler evicts because the (ns,agent)
// no longer points at this revision, or the idle timer fires.
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

	mu       sync.Mutex
	pools    map[WarmKey][]*SandboxInstance
	targets  map[WarmKey]warmTarget
	filling  map[WarmKey]bool
	knownKey map[types.NamespacedName]WarmKey // TA NN → last-seen WarmKey

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
		targets:    make(map[WarmKey]warmTarget),
		filling:    make(map[WarmKey]bool),
		knownKey:   make(map[types.NamespacedName]WarmKey),
		stopCh:     make(chan struct{}),
	}
}

// SetupWithManager registers the WarmPool as a TaskAgent reconciler.
// Pool predicate filters to TAs whose WorkerPoolRef matches this
// worker's pool — same shape as the sandbox watcher's pool filter.
func (w *WarmPool) SetupWithManager(mgr ctrl.Manager) error {
	poolFilter := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		ta, ok := obj.(*clrkv1alpha1.TaskAgent)
		if !ok {
			return false
		}
		return ta.Spec.WorkerPoolRef == w.poolName
	})
	return ctrl.NewControllerManagedBy(mgr).
		For(&clrkv1alpha1.TaskAgent{}, builder.WithPredicates(poolFilter)).
		Named("warm-pool").
		Complete(w)
}

// Reconcile is the per-TaskAgent reconcile entry. Drives per-key
// pool state forward by evicting stale slots and kicking fills
// toward target.
func (w *WarmPool) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var ta clrkv1alpha1.TaskAgent
	if err := w.client.Get(ctx, req.NamespacedName, &ta); err != nil {
		if apierrors.IsNotFound(err) {
			w.evictByName(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Pool predicate filters at watch time; defend against a stale
	// cached object whose pool ref was changed.
	if ta.Spec.WorkerPoolRef != w.poolName {
		w.evictByName(req.NamespacedName)
		return ctrl.Result{}, nil
	}
	if ta.Spec.WarmPoolSize == nil || *ta.Spec.WarmPoolSize <= 0 {
		w.evictByName(req.NamespacedName)
		return ctrl.Result{}, nil
	}
	if ta.Status.LatestReadyRevisionName == "" {
		w.evictByName(req.NamespacedName)
		return ctrl.Result{}, nil
	}

	size := int(*ta.Spec.WarmPoolSize)
	if size > w.maxPerKey {
		size = w.maxPerKey
	}
	idleTTL := defaultWarmIdleTTL
	if ta.Spec.WarmPoolIdleTTL != nil && ta.Spec.WarmPoolIdleTTL.Duration > 0 {
		idleTTL = ta.Spec.WarmPoolIdleTTL.Duration
	}
	newKey := WarmKey{Namespace: ta.Namespace, Agent: ta.Name, Revision: ta.Status.LatestReadyRevisionName}

	var (
		toEvict []*SandboxInstance
		toFill  bool
	)

	w.mu.Lock()
	select {
	case <-w.stopCh:
		w.mu.Unlock()
		return ctrl.Result{}, nil
	default:
	}
	if old, ok := w.knownKey[req.NamespacedName]; ok && old != newKey {
		toEvict = append(toEvict, w.pools[old]...)
		delete(w.pools, old)
		delete(w.targets, old)
	}
	w.knownKey[req.NamespacedName] = newKey
	w.targets[newKey] = warmTarget{size: size, idleTTL: idleTTL}
	if len(w.pools[newKey]) < size {
		toFill = true
	}
	w.mu.Unlock()

	for _, sb := range toEvict {
		w.teardownSlot(sb)
	}
	if len(toEvict) > 0 {
		w.notifier.broadcast()
	}
	if toFill {
		w.kickFill(newKey)
	}
	return ctrl.Result{}, nil
}

// evictByName drains the pool tied to a TaskAgent. Used on TA delete,
// pool-ref change, WarmPoolSize→0, and LatestReadyRevisionName→"".
func (w *WarmPool) evictByName(nn types.NamespacedName) {
	w.mu.Lock()
	key, ok := w.knownKey[nn]
	if !ok {
		w.mu.Unlock()
		return
	}
	pool := w.pools[key]
	delete(w.pools, key)
	delete(w.targets, key)
	delete(w.knownKey, nn)
	w.mu.Unlock()

	for _, sb := range pool {
		w.teardownSlot(sb)
	}
	if len(pool) > 0 {
		w.notifier.broadcast()
	}
}

// teardownSlot is the single place that releases a warm sandbox:
// cancel its idle timer (if any) and tear down the underlying
// libcontainer state. Callers must have already removed the slot
// from w.pools so the slot can't be Acquired or expired in parallel.
func (w *WarmPool) teardownSlot(sb *SandboxInstance) {
	if sb.idleTimer != nil {
		sb.idleTimer.Stop()
	}
	w.deleteWarm(sb.ID)
}

// Acquire returns a warm sandbox for key, or nil if the pool is
// empty. Caller owns the returned sandbox lifecycle (must Delete it
// after use, same as a cold-path Create). Triggers a background fill
// so the pool is ready for the next request before the next
// reconcile.
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

	// Cancel idle expiry. If the timer beat us, expireWarm finds the
	// slot gone (we already removed it) and bails.
	if sb.idleTimer != nil {
		sb.idleTimer.Stop()
		sb.idleTimer = nil
	}

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
	if w.targets[key].size == 0 {
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
// Acquire or Reconcile will re-kick.
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
		if target.size == 0 || have >= target.size {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), warmFillTimeout)
		sb, err := w.fillOne(ctx, key, target.idleTTL)
		cancel()
		if err != nil {
			log.V(1).Info("Warm fill skipped", "err", err)
			return
		}

		w.mu.Lock()
		select {
		case <-w.stopCh:
			w.mu.Unlock()
			w.teardownSlot(sb)
			return
		default:
		}
		// Pool might have been drained by Reconcile (TA deleted /
		// revision rolled) while fillOne ran. Drop the freshly-warmed
		// sandbox on the floor rather than reviving a now-stale key.
		if _, ok := w.targets[key]; !ok {
			w.mu.Unlock()
			w.teardownSlot(sb)
			return
		}
		w.pools[key] = append(w.pools[key], sb)
		w.mu.Unlock()
		w.notifier.broadcast()
		log.Info("Warmed sandbox", "sandboxID", sb.ID)
	}
}

// fillOne resolves the TA's current spec, creates one warm sandbox
// for key, and schedules its idle-expiry timer. Does NOT touch
// w.pools — caller appends.
func (w *WarmPool) fillOne(ctx context.Context, key WarmKey, idleTTL time.Duration) (*SandboxInstance, error) {
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
	sb, err := w.sandboxMgr.Create(ctx, sandboxID, key.Agent, identity, caPEM, rev.Spec.AgentSandbox, ta.Spec.Resources, ta.Spec.State, true, 0)
	if err != nil {
		return nil, fmt.Errorf("sandbox Create: %w", err)
	}

	sb.idleTimer = time.AfterFunc(idleTTL, func() { w.expireWarm(key, sb) })
	return sb, nil
}

// expireWarm is the AfterFunc callback for a warm slot's idle timer.
// If the slot is still in the pool, remove it and tear it down; if
// Acquire (or a Reconcile evict) already pulled it, no-op.
func (w *WarmPool) expireWarm(key WarmKey, sb *SandboxInstance) {
	w.mu.Lock()
	pool := w.pools[key]
	idx := -1
	for i, p := range pool {
		if p == sb {
			idx = i
			break
		}
	}
	if idx < 0 {
		w.mu.Unlock()
		return
	}
	w.pools[key] = append(pool[:idx], pool[idx+1:]...)
	w.mu.Unlock()

	ctrl.Log.WithName("warmpool").Info("Warm slot expired", "warmKey", key.String(), "sandboxID", sb.ID)
	w.deleteWarm(sb.ID)
	w.notifier.broadcast()
	w.kickFill(key)
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
			w.teardownSlot(sb)
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
