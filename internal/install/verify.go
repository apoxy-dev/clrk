package install

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// verifyCMDeployment returns a comparator that checks the cm Deployment the
// apiserver stored matches what we sent on the fields that matter: the
// container image (so an out-of-band patch swapping the image gets reclaimed by
// our ForceOwnership) and the env count (so a stripped PodSpec is detectable).
func verifyCMDeployment(wantImage string, wantEnvCount int) func(*appsv1.Deployment) error {
	return func(dep *appsv1.Deployment) error {
		containers := dep.Spec.Template.Spec.Containers
		if len(containers) != 1 {
			return fmt.Errorf("expected 1 container, found %d", len(containers))
		}
		if containers[0].Image != wantImage {
			return fmt.Errorf("image: want %q, got %q", wantImage, containers[0].Image)
		}
		if got, want := len(containers[0].Env), wantEnvCount; got != want {
			return fmt.Errorf("env count: want %d, got %d (PodSpec may have been stripped)", want, got)
		}
		return nil
	}
}

// verifyWorkerPool returns a comparator that checks the WorkerPool the
// aggregated apiserver stored matches what we sent. We assert the overlay's
// image (the original bug: a stripped spec persisted clrk/worker:latest across
// reapplies). The gVisor/runsc invariants no longer need a guard here: they are
// not in the CR at all — the controller's pod builder (internal/workerpod) owns
// them — so an edit can't demote a worker pod out of privileged. One indicative
// field; a full deep-equal would tilt at status/managedFields.
func verifyWorkerPool(wantImage string) func(*clrkv1alpha1.WorkerPool) error {
	return func(wp *clrkv1alpha1.WorkerPool) error {
		if got := wp.Spec.Template.Image; got != wantImage {
			return fmt.Errorf("image: want %q, got %q", wantImage, got)
		}
		return nil
	}
}
