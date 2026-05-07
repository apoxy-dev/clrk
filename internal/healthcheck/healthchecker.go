// Package healthcheck streams every worker pod's WorkerStatusService
// state into an in-memory map and exposes Pick for the ingress
// ext_proc to choose a per-execution worker.
//
// Lives in its own package — distinct from internal/controller — so
// the proto dependency only pollutes the bazel-built controller-
// manager binary, not anything cmd/clrk transitively imports. Per
// memory feedback_clrk_standalone_build, generated *.pb.go are not
// committed to this repo; only cmd/clrk must build standalone.
package healthcheck

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/go-logr/logr"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/ports"
	workerstatusv1alpha1 "github.com/apoxy-dev/clrk/internal/proto/clrk/v1alpha1"
)

// healthcheckerSyncInterval is how often the top-level loop reconciles
// the live set of WorkerPools and their endpoints. Per-endpoint Watch
// streams are long-lived and only opened/torn down on membership
// changes; this interval just bounds how quickly the checker reacts
// to a new pool or a churned EndpointSlice.
const healthcheckerSyncInterval = 5 * time.Second

// healthcheckerDeadAfter marks a worker dead when no message (real or
// heartbeat) has arrived in this window. Three times the worker-side
// heartbeat (5s) per the plan.
const healthcheckerDeadAfter = 15 * time.Second

// WorkerHealthChecker maintains a streaming view of every worker pod's
// state across all WorkerPools. The ingress ext_proc consults Pick to
// route per-execution traffic to a worker that already has the
// revision warm or cached, and InFlight to enforce cluster-wide
// MaxConcurrent.
//
// One instance per controller-manager. Implements manager.Runnable so
// it slots into the existing controller-runtime startup. Not leader-
// gated — every replica needs a live state map for its own ext_proc,
// even if only the leader runs the EG-CR reconcilers.
type WorkerHealthChecker struct {
	client client.Client

	mu    sync.RWMutex
	pools map[types.NamespacedName]*workerPoolState
}

// workerPoolState holds per-pool state. Only the top-level loop
// mutates pools; individual workers are added/removed inside the
// pool's syncEndpoints sweep.
type workerPoolState struct {
	name types.NamespacedName

	mu      sync.RWMutex
	workers map[string]*workerState // key: podIP
}

// workerState tracks one worker pod's stream. snapshot is replaced
// in place by the stream goroutine; lastSeen is the timestamp of the
// most recent message (real or heartbeat).
type workerState struct {
	podIP    string
	cancel   context.CancelFunc
	mu       sync.RWMutex
	snapshot *workerstatusv1alpha1.WorkerStatus
	lastSeen time.Time
}

// NewWorkerHealthChecker constructs a WorkerHealthChecker.
func NewWorkerHealthChecker(c client.Client) *WorkerHealthChecker {
	return &WorkerHealthChecker{
		client: c,
		pools:  make(map[types.NamespacedName]*workerPoolState),
	}
}

// NeedLeaderElection lets every replica run its own checker. The
// state map feeds this replica's ext_proc; without local state, the
// replica can't route incoming requests.
func (h *WorkerHealthChecker) NeedLeaderElection() bool { return false }

// Start runs the top-level sync loop until ctx is cancelled. Per-pool
// goroutines and per-worker stream goroutines are descendants of ctx
// so they all unwind on shutdown.
func (h *WorkerHealthChecker) Start(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx).WithName("worker-healthchecker")
	log.Info("Starting worker health checker")

	ticker := time.NewTicker(healthcheckerSyncInterval)
	defer ticker.Stop()

	// Run one sync immediately so first-request routing doesn't have
	// to wait a full interval after process start.
	h.syncOnce(ctx, log)

	for {
		select {
		case <-ctx.Done():
			h.shutdown()
			return nil
		case <-ticker.C:
			h.syncOnce(ctx, log)
		}
	}
}

// syncOnce reconciles the pool/worker membership. New WorkerPools
// get a workerPoolState; gone pools have their streams cancelled.
func (h *WorkerHealthChecker) syncOnce(ctx context.Context, log logr.Logger) {
	var wps clrkv1alpha1.WorkerPoolList
	if err := h.client.List(ctx, &wps); err != nil {
		log.Error(err, "List WorkerPools failed")
		return
	}
	live := make(map[types.NamespacedName]struct{}, len(wps.Items))
	for i := range wps.Items {
		wp := &wps.Items[i]
		key := types.NamespacedName{Namespace: wp.Namespace, Name: wp.Name}
		live[key] = struct{}{}
		h.ensurePool(ctx, key)
	}

	h.mu.Lock()
	for key, ps := range h.pools {
		if _, ok := live[key]; ok {
			continue
		}
		ps.shutdown()
		delete(h.pools, key)
	}
	pools := make([]*workerPoolState, 0, len(h.pools))
	for _, ps := range h.pools {
		pools = append(pools, ps)
	}
	h.mu.Unlock()

	for _, ps := range pools {
		h.syncPoolEndpoints(ctx, ps, log)
	}
}

func (h *WorkerHealthChecker) ensurePool(ctx context.Context, key types.NamespacedName) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.pools[key]; ok {
		return
	}
	h.pools[key] = &workerPoolState{
		name:    key,
		workers: make(map[string]*workerState),
	}
}

// syncPoolEndpoints reads the EndpointSlices for {pool}-workers and
// reconciles per-podIP stream goroutines.
func (h *WorkerHealthChecker) syncPoolEndpoints(ctx context.Context, ps *workerPoolState, log logr.Logger) {
	svcName := ps.name.Name + "-workers"
	var slices discoveryv1.EndpointSliceList
	if err := h.client.List(ctx, &slices,
		client.InNamespace(ps.name.Namespace),
		client.MatchingLabels{discoveryv1.LabelServiceName: svcName},
	); err != nil {
		log.V(1).Info("List EndpointSlices failed", "pool", ps.name, "err", err)
		return
	}

	live := make(map[string]struct{})
	for i := range slices.Items {
		s := &slices.Items[i]
		for _, ep := range s.Endpoints {
			if ep.Conditions.Ready != nil && !*ep.Conditions.Ready {
				continue
			}
			for _, ip := range ep.Addresses {
				if ip == "" {
					continue
				}
				live[ip] = struct{}{}
			}
		}
	}

	ps.mu.Lock()
	for ip := range live {
		if _, ok := ps.workers[ip]; ok {
			continue
		}
		wctx, cancel := context.WithCancel(ctx)
		ws := &workerState{podIP: ip, cancel: cancel}
		ps.workers[ip] = ws
		go h.runWorkerStream(wctx, ps.name, ws)
	}
	for ip, ws := range ps.workers {
		if _, ok := live[ip]; ok {
			continue
		}
		ws.cancel()
		delete(ps.workers, ip)
	}
	ps.mu.Unlock()
}

// runWorkerStream opens a Watch stream to the worker and feeds
// snapshots into ws.snapshot until ctx is cancelled or the stream
// errors. On any failure, sleeps a short backoff before retrying.
// Exits cleanly on ctx cancel.
func (h *WorkerHealthChecker) runWorkerStream(ctx context.Context, pool types.NamespacedName, ws *workerState) {
	log := ctrl.LoggerFrom(ctx).WithName("worker-healthchecker.stream").
		WithValues("pool", pool, "ip", ws.podIP)

	addr := fmt.Sprintf("%s:%d", ws.podIP, ports.WorkerStatusPort)
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		err := h.streamOnce(ctx, addr, ws, log)
		if ctx.Err() != nil {
			return
		}
		log.V(1).Info("Watch stream ended; reconnecting", "err", err, "after", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

func (h *WorkerHealthChecker) streamOnce(ctx context.Context, addr string, ws *workerState, log logr.Logger) error {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	cli := workerstatusv1alpha1.NewWorkerStatusServiceClient(conn)
	stream, err := cli.Watch(ctx, &workerstatusv1alpha1.WatchRequest{})
	if err != nil {
		return fmt.Errorf("open Watch: %w", err)
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		ws.mu.Lock()
		ws.lastSeen = time.Now()
		if !msg.GetHeartbeat() {
			ws.snapshot = msg
		}
		ws.mu.Unlock()
	}
}

func (h *WorkerHealthChecker) shutdown() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ps := range h.pools {
		ps.shutdown()
	}
	h.pools = make(map[types.NamespacedName]*workerPoolState)
}

func (ps *workerPoolState) shutdown() {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for _, ws := range ps.workers {
		ws.cancel()
	}
	ps.workers = make(map[string]*workerState)
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
	ps, ok := h.pools[pool]
	h.mu.RUnlock()
	if !ok {
		return PickResult{}, false
	}

	type candidate struct {
		ip       string
		warm     uint32
		cached   bool
		inFlight uint32
	}
	now := time.Now()
	cands := make([]candidate, 0)
	clusterInFlight := uint32(0)

	ps.mu.RLock()
	defer ps.mu.RUnlock()

	for ip, ws := range ps.workers {
		ws.mu.RLock()
		snap := ws.snapshot
		seen := ws.lastSeen
		ws.mu.RUnlock()
		if snap == nil {
			continue
		}
		if !seen.IsZero() && now.Sub(seen) > healthcheckerDeadAfter {
			continue
		}
		c := candidate{ip: ip}
		for _, w := range snap.GetWarmRevisions() {
			if w.GetNamespace() == ns && w.GetAgent() == agent && w.GetRevision() == revision {
				c.warm = w.GetCount()
				break
			}
		}
		for _, f := range snap.GetInFlight() {
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
