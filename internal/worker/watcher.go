//go:build linux

package worker

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
)

// sandboxWatcher reconciles AgentSandboxRevision objects targeted at this
// worker's pool. It ensures images are pulled and updates per-worker status.
type sandboxWatcher struct {
	client.Client
	sandboxMgr *SandboxManager
	daemonMgr  *daemonLifecycleManager
	dispatcher *Dispatcher // optional; nil if dispatcher disabled
	poolName   string
	podName    string
	namespace  string
}

func (w *sandboxWatcher) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	var rev clrkv1alpha1.AgentSandboxRevision
	if err := w.Get(ctx, req.NamespacedName, &rev); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.Info("Reconciling AgentSandboxRevision")

	// Ensure image is pulled.
	_, err := w.sandboxMgr.imageStore.EnsureImage(ctx, rev.Spec.Image)
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
		if sb.AgentRef == agentRef && sb.Phase == SandboxReady {
			warmCount++
		}
	}

	var activeExecutions int32
	if w.dispatcher != nil {
		activeExecutions = w.dispatcher.ActiveCountFor(&rev)
	}

	if err := w.updateWorkerStatus(ctx, &rev, imagePulled, warmCount, activeExecutions); err != nil {
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
func (w *sandboxWatcher) handleDaemon(ctx context.Context, rev *clrkv1alpha1.AgentSandboxRevision) error {
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
func (w *sandboxWatcher) electedFor(rev *clrkv1alpha1.AgentSandboxRevision) bool {
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
// AgentSandboxRevision.Status.Workers.
func (w *sandboxWatcher) updateWorkerStatus(ctx context.Context, rev *clrkv1alpha1.AgentSandboxRevision, imagePulled bool, warmCount, activeExecutions int32) error {
	now := metav1.NewTime(time.Now())

	found := false
	for i := range rev.Status.Workers {
		if rev.Status.Workers[i].PodName == w.podName {
			rev.Status.Workers[i].ImagePulled = imagePulled
			rev.Status.Workers[i].WarmCount = warmCount
			rev.Status.Workers[i].ActiveExecutions = activeExecutions
			rev.Status.Workers[i].LastHeartbeat = now
			found = true
			break
		}
	}
	if !found {
		rev.Status.Workers = append(rev.Status.Workers, clrkv1alpha1.WorkerSandboxStatus{
			PodName:          w.podName,
			ImagePulled:      imagePulled,
			WarmCount:        warmCount,
			ActiveExecutions: activeExecutions,
			LastHeartbeat:    now,
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

// setupWithManager registers the sandboxWatcher with the controller manager.
// AgentSandboxRevisions are filtered to those carrying this worker's
// pool label, projected by the agent controller from
// {TaskAgent,DaemonAgent}.Spec.WorkerPoolRef.
func (w *sandboxWatcher) setupWithManager(mgr ctrl.Manager) error {
	poolFilter := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetLabels()[clrkv1alpha1.LabelWorkerPool] == w.poolName
	})

	return ctrl.NewControllerManagedBy(mgr).
		For(&clrkv1alpha1.AgentSandboxRevision{}, builder.WithPredicates(poolFilter)).
		Named("sandbox-watcher").
		Complete(reconcile.Func(w.reconcile))
}

// heartbeatLoop periodically updates LastHeartbeat for all
// AgentSandboxRevisions managed by this worker.
func (w *sandboxWatcher) heartbeatLoop(ctx context.Context, interval time.Duration) {
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

			now := metav1.NewTime(time.Now())
			for i := range revList.Items {
				rev := &revList.Items[i]
				var active int32
				if w.dispatcher != nil {
					active = w.dispatcher.ActiveCountFor(rev)
				}
				for j := range rev.Status.Workers {
					if rev.Status.Workers[j].PodName == w.podName {
						rev.Status.Workers[j].LastHeartbeat = now
						rev.Status.Workers[j].ActiveExecutions = active
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
