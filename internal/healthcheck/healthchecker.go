// Package healthcheck maintains the controller-manager's view of every
// worker pod's routing state and exposes Pick for the ingress ext_proc to
// choose a per-execution worker.
//
// Two inputs are joined by pod name:
//
//   - EndpointSlices of each WorkerPool's "<pool>-workers" Service give the
//     routable pod IP + readiness (the IPs Envoy Gateway routes to). A
//     worker is multi-homed in general, so the dispatch IP must come from
//     here, never a worker self-report.
//   - The "WORKER_STATUS" JetStream KV bucket gives each worker's status
//     payload (warm sandboxes, in-flight dispatches, cached images), which
//     workers Put on every change + a 5s floor and Delete on graceful
//     shutdown. The cm Watches the bucket once (no per-pod connections).
//
// Pick joins the two: for each Ready endpoint it looks up the worker's KV
// status by reconstructing the key from (pool ns, pool name, pod name).
//
// Lives in its own package — distinct from internal/controller — so the
// proto dependency only pollutes the bazel-built controller-manager
// binary, not anything cmd/clrk transitively imports. Per memory
// feedback_clrk_standalone_build, generated *.pb.go are not committed to
// this repo; only cmd/clrk must build standalone.
package healthcheck

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/invevent"
	"github.com/apoxy-dev/clrk/internal/ports"
	workerstatusv1alpha1 "github.com/apoxy-dev/clrk/internal/proto/clrk/v1alpha1"
)

// healthcheckerResyncFallback is the slow safety-net interval for the
// endpoint reconcile. Real WorkerPool/EndpointSlice changes drive the
// reconcile event-driven via the manager cache's informers; this fallback
// only guards against a missed informer event and gives the KV-eviction
// sweep a periodic tick on an otherwise-idle cluster. It is deliberately
// far longer than the old 5s poll — the reconcile no longer paces routing
// freshness, the informer does.
const healthcheckerResyncFallback = 30 * time.Second

// healthcheckerDeadAfter marks a worker dead when no KV update (real Put or
// floor heartbeat) has arrived in this window. Three times the worker-side
// floor (5s). This lastSeen staleness is the authoritative liveness signal
// — the KV bucket TTL is deliberately not relied on, since a MaxAge expiry
// is silent to live watchers without LimitMarkerTTL.
const healthcheckerDeadAfter = 15 * time.Second

// kvEvictAfter bounds the in-memory KV map: an entry whose worker stopped
// Putting (ungraceful death, no Delete) lingers only until this window
// (past the server-side bucket TTL of 20s) before the sync sweep drops it.
// Routing already ignores it after healthcheckerDeadAfter; this is just to
// stop the map growing with pod-name churn.
const kvEvictAfter = 30 * time.Second

// kvWatchBackoff paces re-establishing the KV watch after it drops (or
// before the bucket is created on a fresh store).
const kvWatchBackoff = 2 * time.Second

// NATSProvider is the subset of the embedded NATS server the healthchecker
// needs to watch the WORKER_STATUS KV bucket: wait for readiness, then open
// a connection. *internal/nats.Server satisfies it (in-process). Declared
// locally to avoid importing the apiserver package.
type NATSProvider interface {
	Ready(ctx context.Context) error
	Connect(name string) (*nats.Conn, error)
}

// WorkerHealthChecker maintains a joined view of every worker pod's routing
// state across all WorkerPools. The ingress ext_proc consults Pick to route
// per-execution traffic to a worker that already has the revision warm or
// cached, and InFlight to enforce cluster-wide MaxConcurrent.
//
// One instance per controller-manager. Implements manager.Runnable so it
// slots into the existing controller-runtime startup. Not leader-gated —
// every replica needs a live state map for its own ext_proc, even if only
// the leader runs the EG-CR reconcilers; each replica Watches the shared KV
// bucket independently (native NATS fan-out, no dedup).
type WorkerHealthChecker struct {
	client client.Client
	cache  cache.Cache  // source of WorkerPool/EndpointSlice informers for event-driven reconcile
	nats   NATSProvider // nil => worker status disabled (NATS off); Pick finds no candidates

	// resync coalesces informer events into endpoint reconciles. Buffered
	// to depth 1 with a non-blocking send: a burst of EndpointSlice updates
	// during a rollout collapses into a single follow-up reconcile.
	resync chan struct{}

	mu    sync.RWMutex
	pools map[types.NamespacedName]map[string]*endpoint // pool -> podName -> endpoint
	kv    map[string]*kvEntry                           // KV key -> status
}

// endpoint is a Ready worker pod discovered from a pool's EndpointSlices.
type endpoint struct {
	podName string
	podIP   string
}

// kvEntry is one worker's last-seen status from the KV watch. lastSeen is
// the receive time of the most recent update (Put or initial replay).
type kvEntry struct {
	snapshot *workerstatusv1alpha1.WorkerStatus
	lastSeen time.Time
}

// NewWorkerHealthChecker constructs a WorkerHealthChecker. c is the cached
// controller-runtime client used for the (cheap, local) endpoint Lists; ca
// is the manager cache the endpoint reconcile subscribes to for change
// events (pass cm.GetCache()). nats may be a true nil interface when the
// embedded NATS server is disabled; the caller must pass nil (not a typed
// nil *nats.Server) so the disabled branch is taken cleanly.
func NewWorkerHealthChecker(c client.Client, ca cache.Cache, nats NATSProvider) *WorkerHealthChecker {
	return &WorkerHealthChecker{
		client: c,
		cache:  ca,
		nats:   nats,
		resync: make(chan struct{}, 1),
		pools:  make(map[types.NamespacedName]map[string]*endpoint),
		kv:     make(map[string]*kvEntry),
	}
}

// NeedLeaderElection lets every replica run its own checker. The state map
// feeds this replica's ext_proc; without local state, the replica can't
// route incoming requests.
func (h *WorkerHealthChecker) NeedLeaderElection() bool { return false }

// Start runs the endpoint reconcile (event-driven off the manager cache's
// WorkerPool/EndpointSlice informers, plus a slow fallback tick), and (when
// NATS is enabled) a KV watch goroutine, until ctx is cancelled.
func (h *WorkerHealthChecker) Start(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx).WithName("worker-healthchecker")
	log.Info("Starting worker health checker")

	if h.nats != nil {
		go h.runKVWatch(ctx, log)
	} else {
		log.Info("WARNING: Worker status routing disabled: embedded NATS is off; ingress will 503")
	}

	// Subscribe to WorkerPool + EndpointSlice changes so an endpoint going
	// Ready/NotReady, a pod rolling, or a pool appearing reconciles routing
	// immediately instead of on the next poll tick. Registering a handler on
	// an already-synced informer also replays the current objects as adds,
	// so this primes the first reconcile too.
	if err := h.subscribeEndpoints(ctx, log); err != nil {
		return err
	}

	// Slow fallback: guards against a missed informer event and ticks the
	// KV-eviction sweep on an otherwise-idle cluster. Real changes drive
	// h.resync.
	ticker := time.NewTicker(healthcheckerResyncFallback)
	defer ticker.Stop()

	// Run one reconcile immediately so first-request routing doesn't have to
	// wait for the informers' add-replay or a fallback tick after start.
	h.syncOnce(ctx, log)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-h.resync:
			h.syncOnce(ctx, log)
		case <-ticker.C:
			h.syncOnce(ctx, log)
		}
	}
}

// subscribeEndpoints registers a coalescing event handler on the manager
// cache's WorkerPool and EndpointSlice informers. Every add/update/delete
// nudges h.resync; the Start loop reconciles at most once per pending nudge.
func (h *WorkerHealthChecker) subscribeEndpoints(ctx context.Context, log logr.Logger) error {
	handler := toolscache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { h.triggerResync() },
		UpdateFunc: func(_, _ any) { h.triggerResync() },
		DeleteFunc: func(any) { h.triggerResync() },
	}
	for _, obj := range []client.Object{
		&clrkv1alpha1.WorkerPool{},
		&discoveryv1.EndpointSlice{},
	} {
		inf, err := h.cache.GetInformer(ctx, obj)
		if err != nil {
			return fmt.Errorf("get informer for %T: %w", obj, err)
		}
		if _, err := inf.AddEventHandler(handler); err != nil {
			return fmt.Errorf("add event handler for %T: %w", obj, err)
		}
	}
	log.V(1).Info("Subscribed to WorkerPool + EndpointSlice changes")
	return nil
}

// triggerResync requests a reconcile without blocking the informer
// goroutine. The depth-1 buffer coalesces bursts: if a reconcile is already
// pending the nudge is dropped, since the next syncOnce reads the latest
// cache state anyway.
func (h *WorkerHealthChecker) triggerResync() {
	select {
	case h.resync <- struct{}{}:
	default:
	}
}

// syncOnce reconciles the pool/endpoint membership from WorkerPools +
// EndpointSlices, then prunes long-gone KV entries.
func (h *WorkerHealthChecker) syncOnce(ctx context.Context, log logr.Logger) {
	var wps clrkv1alpha1.WorkerPoolList
	if err := h.client.List(ctx, &wps); err != nil {
		log.Error(err, "List WorkerPools failed")
		return
	}

	pools := make(map[types.NamespacedName]map[string]*endpoint, len(wps.Items))
	for i := range wps.Items {
		wp := &wps.Items[i]
		key := types.NamespacedName{Namespace: wp.Namespace, Name: wp.Name}
		workers := h.poolEndpoints(ctx, key, log)
		if workers == nil {
			// EndpointSlice List failed for this pool — retain the prior
			// snapshot rather than flapping every worker out of routing.
			h.mu.RLock()
			workers = h.pools[key]
			h.mu.RUnlock()
			if workers == nil {
				workers = map[string]*endpoint{}
			}
		}
		pools[key] = workers
	}

	h.mu.Lock()
	h.pools = pools
	now := time.Now()
	for k, e := range h.kv {
		if now.Sub(e.lastSeen) > kvEvictAfter {
			delete(h.kv, k)
		}
	}
	h.mu.Unlock()
}

// poolEndpoints reads the EndpointSlices for "<pool>-workers" and returns
// the Ready worker pods keyed by pod name. Returns nil on List error so the
// caller can retain prior state. Only the IPv4 address family is consumed
// (the family Envoy Gateway routes on); the dispatch IP is whatever the
// endpoint advertises, never a worker self-report.
func (h *WorkerHealthChecker) poolEndpoints(ctx context.Context, poolKey types.NamespacedName, log logr.Logger) map[string]*endpoint {
	svcName := poolKey.Name + "-workers"
	var slices discoveryv1.EndpointSliceList
	if err := h.client.List(ctx, &slices,
		client.InNamespace(poolKey.Namespace),
		client.MatchingLabels{discoveryv1.LabelServiceName: svcName},
	); err != nil {
		log.V(1).Info("List EndpointSlices failed", "pool", poolKey, "err", err)
		return nil
	}

	workers := make(map[string]*endpoint)
	for i := range slices.Items {
		s := &slices.Items[i]
		if s.AddressType != discoveryv1.AddressTypeIPv4 {
			continue
		}
		for _, ep := range s.Endpoints {
			if ep.Conditions.Ready != nil && !*ep.Conditions.Ready {
				continue
			}
			if ep.TargetRef == nil || ep.TargetRef.Name == "" {
				continue
			}
			var ip string
			for _, a := range ep.Addresses {
				if a != "" {
					ip = a
					break
				}
			}
			if ip == "" {
				continue
			}
			workers[ep.TargetRef.Name] = &endpoint{podName: ep.TargetRef.Name, podIP: ip}
		}
	}
	return workers
}

// runKVWatch connects to the embedded NATS and Watches the WORKER_STATUS
// bucket, feeding kvEntry updates into the map until ctx is cancelled. On
// any failure (bucket not yet created, dropped watch) it retries after a
// short backoff.
func (h *WorkerHealthChecker) runKVWatch(ctx context.Context, log logr.Logger) {
	log = log.WithName("kv")
	if err := h.nats.Ready(ctx); err != nil {
		return // ctx done
	}
	nc, err := h.nats.Connect("clrk-cm-worker-status")
	if err != nil {
		log.Error(err, "Connect NATS for worker-status watch failed")
		return
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Error(err, "JetStream for worker-status watch failed")
		return
	}

	for {
		if ctx.Err() != nil {
			return
		}
		if err := h.watchOnce(ctx, js); err != nil && ctx.Err() == nil {
			log.V(1).Info("Worker-status KV watch ended; reconnecting", "err", err, "after", kvWatchBackoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(kvWatchBackoff):
		}
	}
}

func (h *WorkerHealthChecker) watchOnce(ctx context.Context, js jetstream.JetStream) error {
	kv, err := js.KeyValue(ctx, invevent.WorkerStatusBucket)
	if err != nil {
		return fmt.Errorf("bind %s kv: %w", invevent.WorkerStatusBucket, err)
	}
	w, err := kv.WatchAll(ctx)
	if err != nil {
		return fmt.Errorf("watch %s kv: %w", invevent.WorkerStatusBucket, err)
	}
	defer func() { _ = w.Stop() }()

	for entry := range w.Updates() {
		if entry == nil {
			// Marks end of the initial-values replay; subsequent entries
			// are live updates. A cold replica now has the full fleet.
			continue
		}
		switch entry.Operation() {
		case jetstream.KeyValuePut:
			h.upsertKV(entry)
		case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
			h.removeKV(entry.Key())
		}
	}
	return nil // Updates() closed -> reconnect
}

func (h *WorkerHealthChecker) upsertKV(entry jetstream.KeyValueEntry) {
	var ws workerstatusv1alpha1.WorkerStatus
	if err := proto.Unmarshal(entry.Value(), &ws); err != nil {
		// Drop a malformed value; the next Put supersedes it.
		return
	}
	// lastSeen is the receive time (now), NOT entry.Created(): an initial
	// replay of a quiet-but-live worker could carry a Put older than
	// healthcheckerDeadAfter and would otherwise be dropped immediately.
	h.mu.Lock()
	h.kv[entry.Key()] = &kvEntry{snapshot: &ws, lastSeen: time.Now()}
	h.mu.Unlock()
}

func (h *WorkerHealthChecker) removeKV(key string) {
	h.mu.Lock()
	delete(h.kv, key)
	h.mu.Unlock()
}

// PickResult is what Pick returns to the ingress ext_proc.
type PickResult struct {
	// Addr is "<podIP>:<DispatchPort>". The ext_proc sets this on
	// the request authority so the EG cluster routes here.
	Addr string
	// AlreadyAtCap is true when every live worker for this pool
	// already has spec.maxConcurrent in flight for this (ns, agent).
	// The ext_proc maps this to 429.
	AlreadyAtCap bool
}

// Pick returns a worker IP:port for an inbound TaskAgent request.
// Selection policy: revision-warm > revision-cached > spare capacity,
// then lowest in-flight, ties broken by stable hash on tieBreaker
// (typically the execution-id) to spread thundering-herd.
//
// maxConcurrent is the TaskAgent's spec.maxConcurrent (0 = unlimited).
// When > 0 and every live worker is at the cap for this (ns, agent),
// returns ok=false with AlreadyAtCap=true so the ext_proc can return
// 429 instead of 503.
func (h *WorkerHealthChecker) Pick(pool types.NamespacedName, ns, agent, revision string, maxConcurrent uint32, tieBreaker string) (PickResult, bool) {
	h.mu.RLock()
	workers := h.pools[pool]
	states := make([]CandidateState, 0, len(workers))
	for _, ep := range workers {
		key := invevent.WorkerStatusKey(pool.Namespace, pool.Name, ep.podName)
		var snap *workerstatusv1alpha1.WorkerStatus
		var seen time.Time
		if e := h.kv[key]; e != nil {
			snap = e.snapshot
			seen = e.lastSeen
		}
		states = append(states, CandidateState{PodIP: ep.podIP, Snapshot: snap, LastSeen: seen})
	}
	h.mu.RUnlock()

	return PickFromCandidates(states, ns, agent, revision, maxConcurrent, tieBreaker, time.Now())
}

// CandidateState is the per-worker input to PickFromCandidates. The KV
// watch + endpoint join produces these in production; tests construct them
// directly. Snapshot may be nil (no KV status yet); LastSeen zero means no
// status has been observed.
type CandidateState struct {
	PodIP    string
	Snapshot *workerstatusv1alpha1.WorkerStatus
	LastSeen time.Time
}

// PickFromCandidates applies the worker selection policy to a
// caller-supplied slice. Same policy as Pick:
//
//   - drop snap==nil and stale (lastSeen older than healthcheckerDeadAfter
//     relative to now);
//   - if maxConcurrent > 0 and the (ns, agent) cluster-wide in-flight
//     sum >= maxConcurrent, return {AlreadyAtCap: true}, false;
//   - filter workers individually at maxConcurrent;
//   - sort: workers with a warm sandbox first, then less in-flight,
//     ties broken by FNV(tieBreaker, ip) so concurrent picks with the
//     same tieBreaker pin to the same worker while different
//     tieBreakers spread.
//
// Exported so unit tests can drive the policy without spinning up a
// KV watch.
func PickFromCandidates(states []CandidateState, ns, agent, revision string, maxConcurrent uint32, tieBreaker string, now time.Time) (PickResult, bool) {
	type candidate struct {
		ip       string
		warm     uint32
		inFlight uint32
	}
	cands := make([]candidate, 0, len(states))
	clusterInFlight := uint32(0)

	for _, st := range states {
		if st.Snapshot == nil {
			continue
		}
		if !st.LastSeen.IsZero() && now.Sub(st.LastSeen) > healthcheckerDeadAfter {
			continue
		}
		c := candidate{ip: st.PodIP}
		for _, w := range st.Snapshot.GetWarmRevisions() {
			if w.GetNamespace() == ns && w.GetAgent() == agent && w.GetRevision() == revision {
				c.warm = w.GetCount()
				break
			}
		}
		for _, f := range st.Snapshot.GetInFlight() {
			if f.GetNamespace() == ns && f.GetAgent() == agent {
				c.inFlight = f.GetCount()
				clusterInFlight += f.GetCount()
				break
			}
		}
		cands = append(cands, c)
	}

	if len(cands) == 0 {
		return PickResult{}, false
	}

	if maxConcurrent > 0 && clusterInFlight >= maxConcurrent {
		return PickResult{AlreadyAtCap: true}, false
	}

	// Filter out workers individually at MaxConcurrent. The cluster
	// check above is the cap; this just avoids dogpiling one worker
	// when others have headroom.
	if maxConcurrent > 0 {
		filtered := cands[:0]
		for _, c := range cands {
			if c.inFlight < maxConcurrent {
				filtered = append(filtered, c)
			}
		}
		if len(filtered) == 0 {
			return PickResult{AlreadyAtCap: true}, false
		}
		cands = filtered
	}

	sort.SliceStable(cands, func(i, j int) bool {
		// Warmer first.
		if (cands[i].warm > 0) != (cands[j].warm > 0) {
			return cands[i].warm > 0
		}
		// Then less in-flight.
		if cands[i].inFlight != cands[j].inFlight {
			return cands[i].inFlight < cands[j].inFlight
		}
		// Tie-break by stable hash on tieBreaker + ip so concurrent
		// requests with the same tieBreaker pin to the same worker
		// while different tieBreakers spread.
		hi := pickHash(tieBreaker, cands[i].ip)
		hj := pickHash(tieBreaker, cands[j].ip)
		return hi < hj
	})

	return PickResult{Addr: fmt.Sprintf("%s:%d", cands[0].ip, ports.DispatchPort)}, true
}

func pickHash(tieBreaker, ip string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(tieBreaker))
	h.Write([]byte{0})
	h.Write([]byte(ip))
	return h.Sum64()
}

// Compile-time guard.
var _ manager.Runnable = (*WorkerHealthChecker)(nil)
