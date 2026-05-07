package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// TaskAgentRevisionReconciler manages AgentSandboxRevision snapshots for a
// TaskAgent. Gateway / HTTPRoute live in TaskAgentIngressReconciler so they
// can be skipped where gateway-api isn't installed.
type TaskAgentRevisionReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=taskagents,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=taskagents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=workerpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=egressgateways,verbs=get;list;watch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=agentsandboxrevisions,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=agentsandboxrevisions/status,verbs=get;update;patch

func (r *TaskAgentRevisionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var ta clrkv1alpha1.TaskAgent
	if err := r.Get(ctx, req.NamespacedName, &ta); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	statusBase := ta.DeepCopy()

	now := metav1.Now()

	wpReady := metav1.Condition{
		Type:               condWorkerPoolReady,
		ObservedGeneration: ta.Generation,
		LastTransitionTime: now,
	}
	var wp clrkv1alpha1.WorkerPool
	wpKey := types.NamespacedName{Name: ta.Spec.WorkerPoolRef, Namespace: ta.Namespace}
	if err := r.Get(ctx, wpKey, &wp); err != nil {
		if apierrors.IsNotFound(err) {
			wpReady.Status = metav1.ConditionFalse
			wpReady.Reason = "WorkerPoolNotFound"
			wpReady.Message = fmt.Sprintf("WorkerPool %q not found", ta.Spec.WorkerPoolRef)
		} else {
			return ctrl.Result{}, fmt.Errorf("looking up WorkerPool: %w", err)
		}
	} else {
		wpReady.Status = metav1.ConditionTrue
		wpReady.Reason = "WorkerPoolFound"
		wpReady.Message = fmt.Sprintf("WorkerPool %q exists", ta.Spec.WorkerPoolRef)
	}
	meta.SetStatusCondition(&ta.Status.Conditions, wpReady)

	egressConfigured := metav1.Condition{
		Type:               condEgressConfigured,
		ObservedGeneration: ta.Generation,
		LastTransitionTime: now,
	}
	if len(ta.Spec.EgressRefs) == 0 {
		egressConfigured.Status = metav1.ConditionTrue
		egressConfigured.Reason = "NoEgressRefs"
		egressConfigured.Message = "No egress refs configured"
	} else {
		missing, err := validateEgressRefs(ctx, r.Client, ta.Namespace, ta.Spec.EgressRefs)
		if err != nil {
			return ctrl.Result{}, err
		}
		if len(missing) == 0 {
			egressConfigured.Status = metav1.ConditionTrue
			egressConfigured.Reason = "AllEgressRefsFound"
			egressConfigured.Message = "All egress gateway refs resolved"
		} else {
			egressConfigured.Status = metav1.ConditionFalse
			egressConfigured.Reason = "EgressRefsNotFound"
			egressConfigured.Message = fmt.Sprintf("Missing EgressGateway(s): %v", missing)
		}
	}
	meta.SetStatusCondition(&ta.Status.Conditions, egressConfigured)

	revName := revisionName(ta.Name, ta.Generation)
	var rev clrkv1alpha1.AgentSandboxRevision
	revKey := types.NamespacedName{Name: revName, Namespace: ta.Namespace}
	created := false
	if err := r.Get(ctx, revKey, &rev); apierrors.IsNotFound(err) {
		rev = clrkv1alpha1.AgentSandboxRevision{
			ObjectMeta: metav1.ObjectMeta{
				Name:      revName,
				Namespace: ta.Namespace,
				Labels: map[string]string{
					labelAgent:      ta.Name,
					labelAgentKind:  clrkv1alpha1.AgentKindTask,
					labelGeneration: strconv.FormatInt(ta.Generation, 10),
					labelWorkerPool: ta.Spec.WorkerPoolRef,
				},
				Annotations: ta.Spec.Template.Annotations,
			},
			Spec: ta.Spec.Template.Spec,
		}
		if err := ctrl.SetControllerReference(&ta, &rev, r.Scheme); err != nil {
			return ctrl.Result{}, fmt.Errorf("setting AgentSandboxRevision owner reference: %w", err)
		}
		logger.Info("Creating AgentSandboxRevision", "name", revName)
		if err := r.Create(ctx, &rev); err != nil {
			return ctrl.Result{}, fmt.Errorf("creating AgentSandboxRevision: %w", err)
		}
		created = true
	} else if err != nil {
		return ctrl.Result{}, fmt.Errorf("getting AgentSandboxRevision: %w", err)
	}
	ta.Status.LatestCreatedRevisionName = revName

	revisionReady := metav1.Condition{
		Type:               condRevisionReady,
		ObservedGeneration: ta.Generation,
		LastTransitionTime: now,
	}
	if rev.Status.ReadyWorkers >= 1 {
		ta.Status.LatestReadyRevisionName = revName
		revisionReady.Status = metav1.ConditionTrue
		revisionReady.Reason = "RevisionReady"
		revisionReady.Message = fmt.Sprintf("Revision %q has %d ready worker(s)", revName, rev.Status.ReadyWorkers)
	} else {
		// Keep previous latestReadyRevisionName if the new revision isn't ready yet.
		revisionReady.Status = metav1.ConditionFalse
		revisionReady.Reason = "NoWorkersReady"
		revisionReady.Message = fmt.Sprintf("Revision %q has no ready workers", revName)
	}
	meta.SetStatusCondition(&ta.Status.Conditions, revisionReady)

	// Aggregate ActiveExecutions across workers for the latest-ready
	// revision. We only count the latest-ready slice because executions
	// against older revisions drain quickly (one-shot semantics) and
	// summing every revision would double-count during a rollover.
	if ta.Status.LatestReadyRevisionName != "" {
		var activeRev clrkv1alpha1.AgentSandboxRevision
		activeKey := types.NamespacedName{Name: ta.Status.LatestReadyRevisionName, Namespace: ta.Namespace}
		if err := r.Get(ctx, activeKey, &activeRev); err == nil {
			var sum int32
			for _, ws := range activeRev.Status.Workers {
				sum += ws.ActiveExecutions
			}
			ta.Status.ActiveExecutions = sum
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("getting active revision for ActiveExecutions sum: %w", err)
		}
	} else {
		ta.Status.ActiveExecutions = 0
	}

	accepted := metav1.Condition{
		Type:               condAccepted,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: ta.Generation,
		LastTransitionTime: now,
		Reason:             "SpecValid",
		Message:            "Spec is structurally valid",
	}
	meta.SetStatusCondition(&ta.Status.Conditions, accepted)

	// Only GC after a fresh revision lands — old revisions don't accumulate
	// on subsequent reconciles for the same generation, so nothing changed.
	if created && ta.Generation > maxRevisionHistory {
		if err := r.gcRevisions(ctx, &ta); err != nil {
			logger.Error(err, "Revision GC failed")
		}
	}

	ta.Status.ObservedGeneration = ta.Generation
	if err := r.Status().Patch(ctx, &ta, client.MergeFrom(statusBase)); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	return ctrl.Result{}, nil
}

// gcRevisions deletes old AgentSandboxRevisions beyond the history limit,
// keeping the latest created and latest ready revisions.
func (r *TaskAgentRevisionReconciler) gcRevisions(ctx context.Context, ta *clrkv1alpha1.TaskAgent) error {
	var revList clrkv1alpha1.AgentSandboxRevisionList
	if err := r.List(ctx, &revList, &client.ListOptions{
		Namespace:     ta.Namespace,
		LabelSelector: labels.SelectorFromSet(map[string]string{labelAgent: ta.Name}),
	}); err != nil {
		return fmt.Errorf("listing revisions: %w", err)
	}

	keep := map[string]bool{
		ta.Status.LatestCreatedRevisionName: true,
		ta.Status.LatestReadyRevisionName:   true,
	}

	// Sort oldest first by creation timestamp.
	sort.Slice(revList.Items, func(i, j int) bool {
		return revList.Items[i].CreationTimestamp.Before(&revList.Items[j].CreationTimestamp)
	})

	excess := len(revList.Items) - maxRevisionHistory
	if excess <= 0 {
		return nil
	}

	logger := log.FromContext(ctx)
	deleted := 0
	for i := range revList.Items {
		if deleted >= excess {
			break
		}
		name := revList.Items[i].Name
		if keep[name] {
			continue
		}
		logger.Info("Deleting old AgentSandboxRevision", "name", name)
		if err := r.Delete(ctx, &revList.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting revision %q: %w", name, err)
		}
		deleted++
	}
	return nil
}

func (r *TaskAgentRevisionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("taskagent-revision").
		For(&clrkv1alpha1.TaskAgent{}).
		Owns(&clrkv1alpha1.AgentSandboxRevision{}).
		Complete(r)
}
