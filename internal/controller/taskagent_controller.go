package controller

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// TaskAgentReconciler reconciles TaskAgent objects.
// It validates WorkerPool and EgressGateway refs, and manages CronJobs
// for cron-triggered agents.
type TaskAgentReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=taskagents,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=taskagents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=workerpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=egressgateways,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete

func (r *TaskAgentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var ta clrkv1alpha1.TaskAgent
	if err := r.Get(ctx, req.NamespacedName, &ta); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	now := metav1.Now()

	// Validate WorkerPool ref.
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
	setCondition(&ta.Status.Conditions, wpReady)

	// Validate egress refs.
	egressConfigured := metav1.Condition{
		Type:               "EgressConfigured",
		ObservedGeneration: ta.Generation,
		LastTransitionTime: now,
	}
	if len(ta.Spec.EgressRefs) == 0 {
		egressConfigured.Status = metav1.ConditionTrue
		egressConfigured.Reason = "NoEgressRefs"
		egressConfigured.Message = "no egress refs configured"
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
			egressConfigured.Message = "all egress gateway refs resolved"
		} else {
			egressConfigured.Status = metav1.ConditionFalse
			egressConfigured.Reason = "EgressRefsNotFound"
			egressConfigured.Message = fmt.Sprintf("missing EgressGateway(s): %v", missing)
		}
	}
	setCondition(&ta.Status.Conditions, egressConfigured)

	// Accepted condition — spec is structurally valid.
	accepted := metav1.Condition{
		Type:               "Accepted",
		Status:             metav1.ConditionTrue,
		ObservedGeneration: ta.Generation,
		LastTransitionTime: now,
		Reason:             "SpecValid",
		Message:            "spec is structurally valid",
	}
	setCondition(&ta.Status.Conditions, accepted)

	// Cron trigger management.
	cronJobName := ta.Name + "-cron"
	cronJobKey := types.NamespacedName{Name: cronJobName, Namespace: ta.Namespace}

	if ta.Spec.Schedule != nil && *ta.Spec.Schedule != "" {
		// Build and apply CronJob.
		cronJob := r.desiredCronJob(&ta, cronJobName)
		if err := ctrl.SetControllerReference(&ta, cronJob, r.Scheme); err != nil {
			return ctrl.Result{}, fmt.Errorf("setting CronJob owner reference: %w", err)
		}

		var existing batchv1.CronJob
		err := r.Get(ctx, cronJobKey, &existing)
		if apierrors.IsNotFound(err) {
			logger.Info("Creating CronJob", "name", cronJobName)
			if err := r.Create(ctx, cronJob); err != nil {
				return ctrl.Result{}, fmt.Errorf("creating CronJob: %w", err)
			}
		} else if err != nil {
			return ctrl.Result{}, fmt.Errorf("getting CronJob: %w", err)
		} else {
			existing.Spec = cronJob.Spec
			if err := r.Update(ctx, &existing); err != nil {
				return ctrl.Result{}, fmt.Errorf("updating CronJob: %w", err)
			}
		}
	} else {
		// Ensure no CronJob exists.
		var existing batchv1.CronJob
		if err := r.Get(ctx, cronJobKey, &existing); err == nil {
			logger.Info("Deleting CronJob (schedule removed)", "name", cronJobName)
			if err := r.Delete(ctx, &existing); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("deleting CronJob: %w", err)
			}
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("checking for CronJob: %w", err)
		}
	}

	// Patch status.
	if err := r.Status().Update(ctx, &ta); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *TaskAgentReconciler) desiredCronJob(ta *clrkv1alpha1.TaskAgent, name string) *batchv1.CronJob {
	labels := map[string]string{
		"clrk.apoxy.dev/taskagent": ta.Name,
		"clrk.apoxy.dev/component": "cron-trigger",
	}

	// Build the input argument for the curl command.
	inputArg := "{}"
	if ta.Spec.ScheduleInput != nil && len(ta.Spec.ScheduleInput.Raw) > 0 {
		inputArg = string(ta.Spec.ScheduleInput.Raw)
	}

	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ta.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.CronJobSpec{
			Schedule: *ta.Spec.Schedule,
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: labels,
						},
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{
								{
									Name:  "trigger",
									Image: "curlimages/curl:8.5.0",
									Command: []string{
										"curl", "-sf",
										"-X", "POST",
										"-H", "Content-Type: application/json",
										"-d", inputArg,
										// The ingress API service URL will be resolved via
										// cluster DNS. The service name follows the convention
										// clrk-ingress.<namespace>.svc.cluster.local.
										fmt.Sprintf(
											"http://clrk-ingress.%s.svc.cluster.local/v1/agents/%s/execute",
											ta.Namespace, ta.Name,
										),
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func (r *TaskAgentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clrkv1alpha1.TaskAgent{}).
		Owns(&batchv1.CronJob{}).
		Complete(r)
}
