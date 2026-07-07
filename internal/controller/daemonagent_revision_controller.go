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
	"github.com/apoxy-dev/clrk/internal/notify"
)

// DaemonAgentRevisionReconciler manages AgentSandboxRevision snapshots for a
// DaemonAgent. Phase / UpSince / RestartCount are written by the worker
// daemon loop — only the worker knows when its sandbox is alive.
type DaemonAgentRevisionReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Recorder emits rollout notifications (events.k8s.io/v1) on the
	// RevisionReady False->True crossing. Nil-safe.
	Recorder *notify.Recorder
}

// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=daemonagents,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=daemonagents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=workerpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=egressgateways,verbs=get;list;watch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=agentsandboxrevisions,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=agentsandboxrevisions/status,verbs=get;update;patch

func (r *DaemonAgentRevisionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var da clrkv1alpha1.DaemonAgent
	if err := r.Get(ctx, req.NamespacedName, &da); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	// Snapshot before any mutation so the closing Status().Patch sends a
	// merge-patch of just our Status diff. Avoids the resourceVersion
	// conflict storm we'd get from a full Status().Update — sibling
	// reconcilers (egress watcher, revision watcher) bump our object's
	// RV between Get and Update on every reconcile burst.
	statusBase := da.DeepCopy()

	now := metav1.Now()

	wpReady := metav1.Condition{
		Type:               condWorkerPoolReady,
		ObservedGeneration: da.Generation,
		LastTransitionTime: now,
	}
	var wp clrkv1alpha1.WorkerPool
	wpKey := types.NamespacedName{Name: da.Spec.WorkerPoolRef, Namespace: da.Namespace}
	if err := r.Get(ctx, wpKey, &wp); err != nil {
		if apierrors.IsNotFound(err) {
			wpReady.Status = metav1.ConditionFalse
			wpReady.Reason = "WorkerPoolNotFound"
			wpReady.Message = fmt.Sprintf("WorkerPool %q not found", da.Spec.WorkerPoolRef)
		} else {
			return ctrl.Result{}, fmt.Errorf("looking up WorkerPool: %w", err)
		}
	} else {
		wpReady.Status = metav1.ConditionTrue
		wpReady.Reason = "WorkerPoolFound"
		wpReady.Message = fmt.Sprintf("WorkerPool %q exists", da.Spec.WorkerPoolRef)
	}
	meta.SetStatusCondition(&da.Status.Conditions, wpReady)

	egressConfigured := metav1.Condition{
		Type:               condEgressConfigured,
		ObservedGeneration: da.Generation,
		LastTransitionTime: now,
	}
	if len(da.Spec.EgressRefs) == 0 {
		egressConfigured.Status = metav1.ConditionTrue
		egressConfigured.Reason = "NoEgressRefs"
		egressConfigured.Message = "No egress refs configured"
	} else {
		missing, err := validateEgressRefs(ctx, r.Client, da.Namespace, da.Spec.EgressRefs)
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
	meta.SetStatusCondition(&da.Status.Conditions, egressConfigured)

	revName := revisionName(da.Name, da.Generation)
	var rev clrkv1alpha1.AgentSandboxRevision
	revKey := types.NamespacedName{Name: revName, Namespace: da.Namespace}
	created := false
	if err := r.Get(ctx, revKey, &rev); apierrors.IsNotFound(err) {
		rev = clrkv1alpha1.AgentSandboxRevision{
			ObjectMeta: metav1.ObjectMeta{
				Name:      revName,
				Namespace: da.Namespace,
				Labels: map[string]string{
					labelAgent:      da.Name,
					labelAgentKind:  clrkv1alpha1.AgentKindDaemon,
					labelGeneration: strconv.FormatInt(da.Generation, 10),
					labelWorkerPool: da.Spec.WorkerPoolRef,
				},
				Annotations: da.Spec.Template.Annotations,
			},
			Spec: da.Spec.Template.Spec,
		}
		if err := ctrl.SetControllerReference(&da, &rev, r.Scheme); err != nil {
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
	da.Status.LatestCreatedRevisionName = revName

	revisionReady := metav1.Condition{
		Type:               condRevisionReady,
		ObservedGeneration: da.Generation,
		LastTransitionTime: now,
	}
	if rev.Status.ReadyWorkers >= 1 {
		da.Status.LatestReadyRevisionName = revName
		revisionReady.Status = metav1.ConditionTrue
		revisionReady.Reason = "RevisionReady"
		revisionReady.Message = fmt.Sprintf("Revision %q has %d ready worker(s)", revName, rev.Status.ReadyWorkers)
	} else {
		// Keep previous latestReadyRevisionName if the new revision isn't ready yet.
		revisionReady.Status = metav1.ConditionFalse
		revisionReady.Reason = "NoWorkersReady"
		revisionReady.Message = fmt.Sprintf("Revision %q has no ready workers", revName)
	}
	meta.SetStatusCondition(&da.Status.Conditions, revisionReady)

	accepted := metav1.Condition{
		Type:               condAccepted,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: da.Generation,
		LastTransitionTime: now,
		Reason:             "SpecValid",
		Message:            "Spec is structurally valid",
	}
	meta.SetStatusCondition(&da.Status.Conditions, accepted)

	// Only GC after a fresh revision lands — old revisions don't accumulate
	// on subsequent reconciles for the same generation, so nothing changed.
	if created && da.Generation > maxRevisionHistory {
		if err := r.gcRevisions(ctx, &da); err != nil {
			logger.Error(err, "Revision GC failed")
		}
	}

	da.Status.ObservedGeneration = da.Generation
	if err := r.Status().Patch(ctx, &da, client.MergeFrom(statusBase)); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	// Notify once, on the RevisionReady False->True crossing.
	if r.Recorder != nil && revisionReady.Status == metav1.ConditionTrue {
		old := meta.FindStatusCondition(statusBase.Status.Conditions, condRevisionReady)
		if old == nil || old.Status != metav1.ConditionTrue {
			da.TypeMeta = metav1.TypeMeta{Kind: "DaemonAgent", APIVersion: clrkv1alpha1.SchemeGroupVersion.String()}
			r.Recorder.Eventf(&da, nil, notify.TypeNormal, notify.ReasonRevisionReady, notify.ActionRollout,
				"Revision %s is ready", revName)
		}
	}

	return ctrl.Result{}, nil
}

// gcRevisions deletes old AgentSandboxRevisions beyond the history limit,
// keeping the latest created and latest ready revisions.
func (r *DaemonAgentRevisionReconciler) gcRevisions(ctx context.Context, da *clrkv1alpha1.DaemonAgent) error {
	var revList clrkv1alpha1.AgentSandboxRevisionList
	if err := r.List(ctx, &revList, &client.ListOptions{
		Namespace:     da.Namespace,
		LabelSelector: labels.SelectorFromSet(map[string]string{labelAgent: da.Name}),
	}); err != nil {
		return fmt.Errorf("listing revisions: %w", err)
	}

	keep := map[string]bool{
		da.Status.LatestCreatedRevisionName: true,
		da.Status.LatestReadyRevisionName:   true,
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

func (r *DaemonAgentRevisionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("daemonagent-revision").
		For(&clrkv1alpha1.DaemonAgent{}).
		Owns(&clrkv1alpha1.AgentSandboxRevision{}).
		Complete(r)
}
