package install

import (
	"context"
	"fmt"
)

// ApplyControllerManager applies the cm control-plane objects through a, then
// applies-and-verifies the cm Deployment (env-count/image stripping guard).
// Mirrors the previous dev bootstrap: plain SSA for SA/RBAC/Services/APIService/
// PVCs, ApplyAndVerify for the Deployment. Idempotent.
func ApplyControllerManager(ctx context.Context, a Applier, p Profile) error {
	pre, deploy := controllerManagerObjects(p)
	if err := a.ApplyObjects(ctx, pre...); err != nil {
		return err
	}
	envCount := len(deploy.Spec.Template.Spec.Containers[0].Env)
	return ApplyAndVerify(ctx, a, deploy, verifyCMDeployment(p.ControllerImage, envCount))
}

// ApplyWorkerPool applies the worker SA + ClusterRoleBinding, then
// applies-and-verifies the `default` WorkerPool CR (image + privileged
// SecurityContext stripping guard). WorkerPoolDeploymentReconciler in the cm
// turns the CR into the worker Deployment + Service. Idempotent.
func ApplyWorkerPool(ctx context.Context, a Applier, p Profile) error {
	pre, wp := workerPoolObjects(p)
	if err := a.ApplyObjects(ctx, pre...); err != nil {
		return fmt.Errorf("applying worker SA/CRB: %w", err)
	}
	if err := ApplyAndVerify(ctx, a, wp, verifyWorkerPool(p.WorkerImage)); err != nil {
		return fmt.Errorf("applying default WorkerPool: %w", err)
	}
	return nil
}
