//go:build linux

package worker

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	if err := w.updateWorkerStatus(ctx, &rev, imagePulled, warmCount); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating worker status: %w", err)
	}

	return ctrl.Result{}, nil
}

// updateWorkerStatus upserts this worker's entry in
// AgentSandboxRevision.Status.Workers.
func (w *sandboxWatcher) updateWorkerStatus(ctx context.Context, rev *clrkv1alpha1.AgentSandboxRevision, imagePulled bool, warmCount int32) error {
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
