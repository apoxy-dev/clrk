package install

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// Rollout bumps the clrk.apoxy.dev/restartedAt annotation on a Deployment's pod
// template, triggering the same rolling restart as `kubectl rollout restart`. A
// strategic-merge patch (no Get+Update) means concurrent reconciles can't lose
// the rollout to a 409. Returns the Deployment's post-patch metadata.generation
// so the caller can wait for status.observedGeneration to catch up
// (WaitDeploymentRolledOut) instead of racing a stale Available condition.
// Ported from drivers.ClusterDriver.Rollout so the install/upgrade path can roll
// the controller-manager through its RemoteCluster client without pulling in the
// k3d driver.
func Rollout(ctx context.Context, c client.Client, ns, name string) (int64, error) {
	body := fmt.Sprintf(`{"spec":{"template":{"metadata":{"annotations":{%q:%q}}}}}`,
		clrkv1alpha1.RestartedAtAnnotation, nowRFC3339())
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	if err := c.Patch(ctx, dep, client.RawPatch(types.StrategicMergePatchType, []byte(body))); err != nil {
		return 0, fmt.Errorf("patching %s/%s: %w", ns, name, err)
	}
	return dep.Generation, nil
}

// WaitDeploymentRolledOut blocks until ns/name has reconciled at least
// wantGeneration and its pods are fully rolled over to the new ReplicaSet
// (updated == replicas == available == spec.replicas). Gating on
// observedGeneration is what makes this safe after a Rollout: immediately after
// the restartedAt patch the Deployment's Available condition still reflects the
// OLD generation, so a bare Available=True poll would return before the Recreate
// even begins (reporting an upgrade "done" while the old cm pod is still up). The
// updated==replicas==available equality also closes the Recreate gap, where the
// old pod is torn down (replicas drops to 0) before the new one is created.
func WaitDeploymentRolledOut(ctx context.Context, c client.Client, ns, name string, wantGeneration int64, timeout time.Duration) error {
	var lastErr error
	pollErr := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		var dep appsv1.Deployment
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &dep); err != nil {
			lastErr = err
			return false, nil
		}
		if dep.Status.ObservedGeneration < wantGeneration {
			lastErr = fmt.Errorf("deployment %s/%s not yet observed (observedGeneration=%d, want >=%d)",
				ns, name, dep.Status.ObservedGeneration, wantGeneration)
			return false, nil
		}
		var want int32 = 1
		if dep.Spec.Replicas != nil {
			want = *dep.Spec.Replicas
		}
		if dep.Status.UpdatedReplicas != want || dep.Status.Replicas != want || dep.Status.AvailableReplicas != want {
			lastErr = fmt.Errorf("deployment %s/%s rolling out (updated=%d, replicas=%d, available=%d, want=%d)",
				ns, name, dep.Status.UpdatedReplicas, dep.Status.Replicas, dep.Status.AvailableReplicas, want)
			return false, nil
		}
		return true, nil
	})
	if wait.Interrupted(pollErr) {
		if lastErr != nil {
			return fmt.Errorf("deployment %s/%s not rolled out within %s: %w", ns, name, timeout, lastErr)
		}
		return fmt.Errorf("deployment %s/%s not rolled out within %s", ns, name, timeout)
	}
	return pollErr
}

// RolloutWorkerPool triggers a rolling restart of a WorkerPool's worker
// Deployment by bumping RestartedAtAnnotation on the WorkerPool's
// spec.template.metadata.annotations — NOT on the Deployment. The Deployment is
// controller-owned: WorkerPoolDeploymentReconciler rebuilds its pod template
// from wp.spec.template every reconcile, so an annotation patched straight onto
// the Deployment is wiped on the next pass and the rollout silently no-ops.
// Patching the WorkerPool makes the controller propagate the annotation into the
// Deployment template itself. Returns the WorkerPool's post-patch
// metadata.generation so the caller can wait for status.observedGeneration to
// catch up (WaitWorkerPoolConverged). Ported from
// drivers.ClusterDriver.RolloutWorkerPool.
func RolloutWorkerPool(ctx context.Context, c client.Client, ns, name string) (int64, error) {
	body := fmt.Sprintf(`{"spec":{"template":{"metadata":{"annotations":{%q:%q}}}}}`,
		clrkv1alpha1.RestartedAtAnnotation, nowRFC3339())
	wp := &clrkv1alpha1.WorkerPool{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	if err := c.Patch(ctx, wp, client.RawPatch(types.MergePatchType, []byte(body))); err != nil {
		return 0, fmt.Errorf("patching WorkerPool %s/%s: %w", ns, name, err)
	}
	return wp.Generation, nil
}

// WaitWorkerPoolConverged blocks until the WorkerPool has reconciled at least
// wantGeneration and reports its workers rolled out and ready (Available=True
// and Progressing=False). Waiting on the WorkerPool's status — which the
// controller derives from the Deployment — rather than polling the Deployment
// directly can't observe the pre-reconcile converged state; the
// observedGeneration floor ensures we read the controller's verdict on THIS
// rollout. Ported from drivers.ClusterDriver.WaitWorkerPoolConverged.
func WaitWorkerPoolConverged(ctx context.Context, c client.Client, ns, name string, wantGeneration int64, timeout time.Duration) error {
	var lastErr error
	pollErr := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		var wp clrkv1alpha1.WorkerPool
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &wp); err != nil {
			lastErr = err
			return false, nil
		}
		if wp.Status.ObservedGeneration < wantGeneration {
			lastErr = fmt.Errorf("WorkerPool %s/%s not yet observed (observedGeneration=%d, want >=%d)",
				ns, name, wp.Status.ObservedGeneration, wantGeneration)
			return false, nil
		}
		avail := meta.FindStatusCondition(wp.Status.Conditions, "Available")
		prog := meta.FindStatusCondition(wp.Status.Conditions, "Progressing")
		if avail == nil || avail.Status != metav1.ConditionTrue ||
			prog == nil || prog.Status != metav1.ConditionFalse {
			lastErr = fmt.Errorf("WorkerPool %s/%s rolling out (available=%s, progressing=%s)",
				ns, name, conditionStatus(avail), conditionStatus(prog))
			return false, nil
		}
		return true, nil
	})
	if wait.Interrupted(pollErr) {
		if lastErr != nil {
			return fmt.Errorf("WorkerPool %s/%s not converged within %s: %w", ns, name, timeout, lastErr)
		}
		return fmt.Errorf("WorkerPool %s/%s not converged within %s", ns, name, timeout)
	}
	return pollErr
}

// conditionStatus renders a possibly-absent condition's status for error
// messages: "Unknown" when the condition isn't set yet.
func conditionStatus(c *metav1.Condition) string {
	if c == nil {
		return string(metav1.ConditionUnknown)
	}
	return string(c.Status)
}

// nowRFC3339 is the rollout-annotation timestamp. A var so a future need to make
// rollout deterministic in tests can stub it.
var nowRFC3339 = func() string { return time.Now().UTC().Format(time.RFC3339) }
