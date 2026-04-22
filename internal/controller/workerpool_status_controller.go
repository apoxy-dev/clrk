package controller

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// WorkerPoolStatusReconciler owns the portable half of WorkerPool: it
// aggregates execution capacity and per-agent active-execution counts, and
// writes the "Configured" condition. It never reads or writes Deployment /
// Service — that lives in WorkerPoolDeploymentReconciler and only runs in
// cluster mode. The `ReadyReplicas` and `Progressing` fields are left to the
// deployment reconciler; in dev they stay zero / absent.
type WorkerPoolStatusReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=workerpools,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=workerpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=taskagents,verbs=get;list;watch

func (r *WorkerPoolStatusReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var wp clrkv1alpha1.WorkerPool
	if err := r.Get(ctx, req.NamespacedName, &wp); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Compute optimistic capacity from spec. The deployment reconciler
	// (when running) replaces this with a ReadyReplicas-based figure.
	specReplicas := int32(1)
	if wp.Spec.Replicas != nil {
		specReplicas = *wp.Spec.Replicas
	}
	maxExecPerWorker := int32(10)
	if wp.Spec.MaxExecutionsPerWorker != nil {
		maxExecPerWorker = *wp.Spec.MaxExecutionsPerWorker
	}

	// Sum active executions across all TaskAgents referencing this pool.
	var agents clrkv1alpha1.TaskAgentList
	if err := r.List(ctx, &agents, client.InNamespace(wp.Namespace)); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing task agents: %w", err)
	}
	var activeExec int32
	for _, a := range agents.Items {
		if a.Spec.WorkerPoolRef == wp.Name {
			activeExec += a.Status.ActiveExecutions
		}
	}

	// If ReadyReplicas has been populated by the deployment reconciler use
	// that; otherwise trust the spec. This keeps Capacity honest in cluster
	// mode and optimistic in dev.
	replicasForCapacity := wp.Status.ReadyReplicas
	if replicasForCapacity == 0 {
		replicasForCapacity = specReplicas
	}
	maxExecutions := replicasForCapacity * maxExecPerWorker
	availableExecutions := maxExecutions - activeExec
	if availableExecutions < 0 {
		availableExecutions = 0
	}

	wp.Status.ActiveExecutions = activeExec
	wp.Status.Capacity = clrkv1alpha1.WorkerPoolCapacity{
		MaxExecutions:       maxExecutions,
		AvailableExecutions: availableExecutions,
	}

	// Configured is a structural check — the pool has a valid spec. The
	// deployment reconciler (when present) separately sets Available based
	// on observed ready replicas.
	configured := metav1.Condition{
		Type:               "Configured",
		Status:             metav1.ConditionTrue,
		ObservedGeneration: wp.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             "SpecValid",
		Message:            fmt.Sprintf("replicas=%d maxExecPerWorker=%d", specReplicas, maxExecPerWorker),
	}
	meta.SetStatusCondition(&wp.Status.Conditions, configured)

	if err := r.Status().Update(ctx, &wp); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *WorkerPoolStatusReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("workerpool-status").
		For(&clrkv1alpha1.WorkerPool{}).
		Complete(r)
}
