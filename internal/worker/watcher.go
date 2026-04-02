//go:build linux

package worker

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// sandboxWatcher reconciles SandboxState objects for this worker's pool.
// It ensures images are pulled and updates per-worker status.
type sandboxWatcher struct {
	client.Client
	sandboxMgr *SandboxManager
	poolName   string
	podName    string
	namespace  string
}

func (w *sandboxWatcher) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	var ss clrkv1alpha1.SandboxState
	if err := w.Get(ctx, req.NamespacedName, &ss); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.Info("Reconciling SandboxState")

	// Ensure image is pulled.
	_, err := w.sandboxMgr.imageStore.EnsureImage(ctx, ss.Spec.Sandbox.Image)
	imagePulled := err == nil
	if err != nil {
		log.Error(err, "Failed to pull image")
	}

	// Count warm sandboxes for this agent.
	var warmCount int32
	for _, sb := range w.sandboxMgr.List() {
		if sb.AgentRef == ss.Spec.AgentRef && sb.Phase == SandboxReady {
			warmCount++
		}
	}

	if err := w.updateWorkerStatus(ctx, &ss, imagePulled, warmCount); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating worker status: %w", err)
	}

	return ctrl.Result{}, nil
}

// updateWorkerStatus upserts this worker's entry in SandboxState.Status.Workers.
func (w *sandboxWatcher) updateWorkerStatus(ctx context.Context, ss *clrkv1alpha1.SandboxState, imagePulled bool, warmCount int32) error {
	now := metav1.NewTime(time.Now())

	found := false
	for i := range ss.Status.Workers {
		if ss.Status.Workers[i].PodName == w.podName {
			ss.Status.Workers[i].ImagePulled = imagePulled
			ss.Status.Workers[i].WarmCount = warmCount
			ss.Status.Workers[i].LastHeartbeat = now
			found = true
			break
		}
	}
	if !found {
		ss.Status.Workers = append(ss.Status.Workers, clrkv1alpha1.WorkerSandboxStatus{
			PodName:       w.podName,
			ImagePulled:   imagePulled,
			WarmCount:     warmCount,
			LastHeartbeat: now,
		})
	}

	var ready int32
	for _, ws := range ss.Status.Workers {
		if ws.ImagePulled {
			ready++
		}
	}
	ss.Status.ReadyWorkers = ready

	return w.Status().Update(ctx, ss)
}

// setupWithManager registers the sandboxWatcher with the controller manager,
// filtering SandboxState objects to only those matching this worker's pool
// via a field selector on spec.poolRef.
func (w *sandboxWatcher) setupWithManager(mgr ctrl.Manager) error {
	// Index spec.poolRef so we can filter at the API level.
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&clrkv1alpha1.SandboxState{},
		"spec.poolRef",
		func(obj client.Object) []string {
			ss := obj.(*clrkv1alpha1.SandboxState)
			return []string{ss.Spec.PoolRef}
		},
	); err != nil {
		return fmt.Errorf("indexing spec.poolRef: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&clrkv1alpha1.SandboxState{}).
		Named("sandbox-watcher").
		Complete(reconcile.Func(w.reconcile))
}

// heartbeatLoop periodically updates LastHeartbeat for all SandboxStates
// managed by this worker.
func (w *sandboxWatcher) heartbeatLoop(ctx context.Context, interval time.Duration) {
	log := ctrl.LoggerFrom(ctx).WithName("heartbeat")
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var ssList clrkv1alpha1.SandboxStateList
			if err := w.List(ctx, &ssList,
				client.InNamespace(w.namespace),
				client.MatchingFields{"spec.poolRef": w.poolName},
			); err != nil {
				log.Error(err, "Failed to list SandboxStates")
				continue
			}

			now := metav1.NewTime(time.Now())
			for i := range ssList.Items {
				ss := &ssList.Items[i]
				for j := range ss.Status.Workers {
					if ss.Status.Workers[j].PodName == w.podName {
						ss.Status.Workers[j].LastHeartbeat = now
						break
					}
				}
				if err := w.Status().Update(ctx, ss); err != nil {
					log.Error(err, "Failed to update heartbeat", "sandboxState", ss.Name)
				}
			}
		}
	}
}
