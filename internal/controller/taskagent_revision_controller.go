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

// TaskAgentRevisionReconciler owns the portable half of TaskAgent
// reconciliation: validating refs, creating AgentSandboxRevision snapshots,
// GC'ing old revisions, and maintaining Status.Latest{Created,Ready}
// RevisionName + ObservedGeneration. It does not touch Gateway or HTTPRoute;
// that lives in TaskAgentIngressReconciler and only runs where gateway-api is
// installed.
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

	now := metav1.Now()

	// 1. Validate WorkerPool ref.
	wpReady := metav1.Condition{
		Type:               "WorkerPoolReady",
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

	// 2. Validate egress refs.
	egressConfigured := metav1.Condition{
		Type:               "EgressConfigured",
		ObservedGeneration: ta.Generation,
		LastTransitionTime: now,
	}
	if len(ta.Spec.EgressRefs) == 0 {
		egressConfigured.Status = metav1.ConditionTrue
		egressConfigured.Reason = "NoEgressRefs"
		egressConfigured.Message = "No egress refs configured"
	} else {
		allFound := true
		var missing []string
		for _, ref := range ta.Spec.EgressRefs {
			var egw clrkv1alpha1.EgressGateway
			key := types.NamespacedName{Name: ref.GatewayRef, Namespace: ta.Namespace}
			if err := r.Get(ctx, key, &egw); err != nil {
				if apierrors.IsNotFound(err) {
					allFound = false
					missing = append(missing, ref.GatewayRef)
				} else {
					return ctrl.Result{}, fmt.Errorf("looking up EgressGateway %q: %w", ref.GatewayRef, err)
				}
			}
		}
		if allFound {
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

	// 3. Create or get AgentSandboxRevision.
	revisionName := fmt.Sprintf("%s-%05d", ta.Name, ta.Generation)
	var rev clrkv1alpha1.AgentSandboxRevision
	revKey := types.NamespacedName{Name: revisionName, Namespace: ta.Namespace}
	if err := r.Get(ctx, revKey, &rev); apierrors.IsNotFound(err) {
		rev = clrkv1alpha1.AgentSandboxRevision{
			ObjectMeta: metav1.ObjectMeta{
				Name:      revisionName,
				Namespace: ta.Namespace,
				Labels: map[string]string{
					labelAgent:      ta.Name,
					labelAgentKind:  "TaskAgent",
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
		logger.Info("Creating AgentSandboxRevision", "name", revisionName)
		if err := r.Create(ctx, &rev); err != nil {
			return ctrl.Result{}, fmt.Errorf("creating AgentSandboxRevision: %w", err)
		}
	} else if err != nil {
		return ctrl.Result{}, fmt.Errorf("getting AgentSandboxRevision: %w", err)
	}
	ta.Status.LatestCreatedRevisionName = revisionName

	// 4. Set RevisionReady condition and update LatestReadyRevisionName.
	revisionReady := metav1.Condition{
		Type:               "RevisionReady",
		ObservedGeneration: ta.Generation,
		LastTransitionTime: now,
	}
	if rev.Status.ReadyWorkers >= 1 {
		ta.Status.LatestReadyRevisionName = revisionName
		revisionReady.Status = metav1.ConditionTrue
		revisionReady.Reason = "RevisionReady"
		revisionReady.Message = fmt.Sprintf("Revision %q has %d ready worker(s)", revisionName, rev.Status.ReadyWorkers)
	} else {
		// Keep previous latestReadyRevisionName if the new revision isn't ready yet.
		revisionReady.Status = metav1.ConditionFalse
		revisionReady.Reason = "NoWorkersReady"
		revisionReady.Message = fmt.Sprintf("Revision %q has no ready workers", revisionName)
	}
	meta.SetStatusCondition(&ta.Status.Conditions, revisionReady)

	// 5. Accepted condition — spec is structurally valid if we got this far.
	accepted := metav1.Condition{
		Type:               "Accepted",
		Status:             metav1.ConditionTrue,
		ObservedGeneration: ta.Generation,
		LastTransitionTime: now,
		Reason:             "SpecValid",
		Message:            "Spec is structurally valid",
	}
	meta.SetStatusCondition(&ta.Status.Conditions, accepted)

	// 6. Revision GC.
	if ta.Generation > maxRevisionHistory {
		if err := r.gcRevisions(ctx, &ta); err != nil {
			logger.Error(err, "Revision GC failed")
		}
	}

	ta.Status.ObservedGeneration = ta.Generation
	if err := r.Status().Update(ctx, &ta); err != nil {
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
