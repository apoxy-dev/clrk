//go:build linux

package agents

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/worker/sandbox"
)

// heartbeatInterval is the cadence at which Watcher refreshes
// LastHeartbeat on every AgentSandboxRevision targeting this worker's
// pool, and also the staleness floor electedFor uses to ignore dead
// peer heartbeats.
const heartbeatInterval = 30 * time.Second

// Watcher reconciles AgentSandboxRevision objects targeted at this
// worker's pool. It ensures images are pulled and updates per-worker
// status.
type Watcher struct {
	client.Client
	sandboxMgr *sandbox.Manager
	daemonMgr  *DaemonLifecycle
	poolName   string
	podName    string
	namespace  string
}

// NewWatcher constructs a Watcher wired to a sandbox.Manager and the
// daemon supervisor. Fields are unexported so callers must go through
// this constructor.
func NewWatcher(c client.Client, sandboxMgr *sandbox.Manager, daemonMgr *DaemonLifecycle, poolName, podName, namespace string) *Watcher {
	return &Watcher{
		Client:     c,
		sandboxMgr: sandboxMgr,
		daemonMgr:  daemonMgr,
		poolName:   poolName,
		podName:    podName,
		namespace:  namespace,
	}
}

func (w *Watcher) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	var rev clrkv1alpha1.AgentSandboxRevision
	if err := w.Get(ctx, req.NamespacedName, &rev); err != nil {
		if apierrors.IsNotFound(err) {
			// Revision gone — controller-runtime won't deliver a
			// second event for it, and the in-process daemon
			// supervisor only learns about deletions via this
			// reconcile path. Tear it down explicitly so the
			// sandbox doesn't leak. Safe no-op for TaskAgent
			// revisions: daemonMgr only holds DaemonAgent loops.
			w.daemonMgr.StopByRevision(req.Namespace, req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	log.Info("Reconciling AgentSandboxRevision")

	// Ensure image is pulled.
	_, err := w.sandboxMgr.EnsureImage(ctx, rev.Spec.Image)
	imagePulled := err == nil
	if err != nil {
		log.Error(err, "Failed to pull image")
	}

	// Count warm sandboxes for this revision's parent agent. AgentRef on
	// SandboxInstance is populated from the agent label projected by the
	// agent controller.
	agentRef := rev.Labels[clrkv1alpha1.LabelAgent]
	var warmCount int32
	for _, sb := range w.sandboxMgr.List() {
		if sb.AgentRef == agentRef && sb.Phase == sandbox.SandboxReady {
			warmCount++
		}
	}

	// ActiveExecutions intentionally NOT written here. In-flight counts
	// flow through the worker's WORKER_STATUS KV (sub-second), not the
	// apiserver. Status.Workers[] stays a low-frequency heartbeat aid for
	// `kubectl get` debugging only.
	if err := w.updateWorkerStatus(ctx, &rev, imagePulled, warmCount); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating worker status: %w", err)
	}

	if rev.Labels[clrkv1alpha1.LabelAgentKind] == clrkv1alpha1.AgentKindDaemon {
		if err := w.handleDaemon(ctx, &rev); err != nil {
			return ctrl.Result{}, fmt.Errorf("daemon lifecycle: %w", err)
		}
	}

	return ctrl.Result{}, nil
}

// handleDaemon drives the daemon supervisor for a DaemonAgent revision. The
// elected worker (lowest-named pod with a fresh heartbeat) runs the loop;
// every other worker tears down any loop it might still be holding.
func (w *Watcher) handleDaemon(ctx context.Context, rev *clrkv1alpha1.AgentSandboxRevision) error {
	agentName := rev.Labels[clrkv1alpha1.LabelAgent]
	if agentName == "" {
		return nil
	}
	key := types.NamespacedName{Namespace: rev.Namespace, Name: agentName}

	var da clrkv1alpha1.DaemonAgent
	if err := w.Get(ctx, key, &da); err != nil {
		if apierrors.IsNotFound(err) {
			w.daemonMgr.Stop(key)
			return nil
		}
		return fmt.Errorf("getting DaemonAgent: %w", err)
	}

	// Only run the latest revision; older revisions get drained.
	if da.Status.LatestCreatedRevisionName != "" && da.Status.LatestCreatedRevisionName != rev.Name {
		return nil
	}

	if !w.electedFor(rev) {
		w.daemonMgr.Stop(key)
		return nil
	}

	w.daemonMgr.Ensure(&da, rev)
	return nil
}

// electedFor picks the lowest-named worker pod with a recent heartbeat from
// rev.Status.Workers. Single-replica MVP — not a real lease.
func (w *Watcher) electedFor(rev *clrkv1alpha1.AgentSandboxRevision) bool {
	staleAfter := 2 * heartbeatInterval
	cutoff := time.Now().Add(-staleAfter)

	leader := w.podName
	for _, ws := range rev.Status.Workers {
		if ws.PodName == w.podName {
			continue
		}
		if ws.LastHeartbeat.Time.Before(cutoff) {
			continue
		}
		if ws.PodName < leader {
			leader = ws.PodName
		}
	}
	return leader == w.podName
}

// updateWorkerStatus upserts this worker's entry in
// AgentSandboxRevision.Status.Workers. ActiveExecutions is no longer
// written here — in-flight counts flow through the WORKER_STATUS KV.
func (w *Watcher) updateWorkerStatus(ctx context.Context, rev *clrkv1alpha1.AgentSandboxRevision, imagePulled bool, warmCount int32) error {
	now := metav1.NewTime(time.Now())

	found := false
	for i := range rev.Status.Workers {
		if rev.Status.Workers[i].PodName == w.podName {
			rev.Status.Workers[i].ImagePulled = imagePulled
			rev.Status.Workers[i].WarmCount = warmCount
			rev.Status.Workers[i].LastHeartbeat = now
			found = true
			break
		}
	}
	if !found {
		rev.Status.Workers = append(rev.Status.Workers, clrkv1alpha1.WorkerSandboxStatus{
			PodName:       w.podName,
			ImagePulled:   imagePulled,
			WarmCount:     warmCount,
			LastHeartbeat: now,
		})
	}

	var ready int32
	for _, ws := range rev.Status.Workers {
		if ws.ImagePulled {
			ready++
		}
	}
	rev.Status.ReadyWorkers = ready

	return w.Status().Update(ctx, rev)
}

// setupWithManager registers the Watcher with the controller manager.
// AgentSandboxRevisions are filtered to those carrying this worker's
// pool label, projected by the agent controller from
// {TaskAgent,DaemonAgent}.Spec.WorkerPoolRef.
func (w *Watcher) SetupWithManager(mgr ctrl.Manager) error {
	poolFilter := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetLabels()[clrkv1alpha1.LabelWorkerPool] == w.poolName
	})

	return ctrl.NewControllerManagedBy(mgr).
		For(&clrkv1alpha1.AgentSandboxRevision{}, builder.WithPredicates(poolFilter)).
		Named("sandbox-watcher").
		Complete(reconcile.Func(w.reconcile))
}

// HeartbeatLoop periodically updates LastHeartbeat for all
// AgentSandboxRevisions managed by this worker. Cadence is
// heartbeatInterval (package-level).
func (w *Watcher) HeartbeatLoop(ctx context.Context) {
	interval := heartbeatInterval
	log := ctrl.LoggerFrom(ctx).WithName("heartbeat")
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var revList clrkv1alpha1.AgentSandboxRevisionList
			if err := w.List(ctx, &revList,
				client.InNamespace(w.namespace),
				client.MatchingLabels{clrkv1alpha1.LabelWorkerPool: w.poolName},
			); err != nil {
				log.Error(err, "Failed to list AgentSandboxRevisions")
				continue
			}

			// GC any daemon loops whose revisions no longer
			// exist. Catches missed delete events from
			// controller-runtime informer reconnects /
			// resyncs that would otherwise leave orphan
			// sandboxes hammering deleted egress gateways.
			liveRevs := make(map[types.NamespacedName]struct{}, len(revList.Items))
			for i := range revList.Items {
				r := &revList.Items[i]
				liveRevs[types.NamespacedName{Namespace: r.Namespace, Name: r.Name}] = struct{}{}
			}
			w.daemonMgr.GCMissing(liveRevs)

			now := metav1.NewTime(time.Now())
			for i := range revList.Items {
				rev := &revList.Items[i]
				for j := range rev.Status.Workers {
					if rev.Status.Workers[j].PodName == w.podName {
						rev.Status.Workers[j].LastHeartbeat = now
						break
					}
				}
				if err := w.Status().Update(ctx, rev); err != nil {
					log.Error(err, "Failed to update heartbeat", "agentSandboxRevision", rev.Name)
				}
			}
		}
	}
}
