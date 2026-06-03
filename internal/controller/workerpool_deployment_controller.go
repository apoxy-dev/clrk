package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/workerpod"
)

// WorkerPoolDeploymentReconciler owns the k8s-side half of WorkerPool: it
// creates/updates the Deployment + Service that host worker pods and
// reports their health back as ReadyReplicas + Available/Progressing
// conditions.
type WorkerPoolDeploymentReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// CMOTLPEndpoint is the OTLP/HTTP URL of the controller-manager's
	// receiver. The reconciler stamps it on every worker container as
	// CLRK_CM_OTLP_ENDPOINT so the worker's OTLP emitter knows where
	// to ship signals. Empty disables injection — the worker emitter
	// falls back to noop.
	CMOTLPEndpoint string

	// CMNATSAddr is the controller-manager's NATS/JetStream client
	// address (host:port). The reconciler stamps it on every worker
	// container as CLRK_CM_NATS_ADDR so the dispatcher's invocation
	// publisher can dial the cm's embedded JetStream over TCP. Empty
	// disables injection — the worker simply doesn't publish lifecycle
	// events.
	CMNATSAddr string
}

// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch

func (r *WorkerPoolDeploymentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var wp clrkv1alpha1.WorkerPool
	if err := r.Get(ctx, req.NamespacedName, &wp); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	statusBase := wp.DeepCopy()

	deploy := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: wp.Name + "-workers", Namespace: wp.Namespace}}
	if err := createOrUpdateWithRetry(ctx, r.Client, deploy, func() error {
		desired := r.desiredDeployment(&wp)
		deploy.Labels = desired.Labels
		deploy.Spec = desired.Spec
		return ctrl.SetControllerReference(&wp, deploy, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling Deployment: %w", err)
	}

	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: wp.Name + "-workers", Namespace: wp.Namespace}}
	if err := createOrUpdateWithRetry(ctx, r.Client, svc, func() error {
		desired := r.desiredService(&wp)
		svc.Spec.Selector = desired.Spec.Selector
		svc.Spec.Ports = desired.Spec.Ports
		svc.Spec.Type = desired.Spec.Type
		return ctrl.SetControllerReference(&wp, svc, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling Service: %w", err)
	}

	replicas := int32(1)
	if wp.Spec.Replicas != nil {
		replicas = *wp.Spec.Replicas
	}
	readyReplicas := deploy.Status.ReadyReplicas
	wp.Status.ReadyReplicas = readyReplicas
	wp.Status.ObservedGeneration = wp.Generation

	now := metav1.Now()

	available := metav1.Condition{
		Type:               condAvailable,
		ObservedGeneration: wp.Generation,
		LastTransitionTime: now,
	}
	if readyReplicas >= 1 {
		available.Status = metav1.ConditionTrue
		available.Reason = "WorkersReady"
		available.Message = fmt.Sprintf("%d worker(s) ready", readyReplicas)
	} else {
		available.Status = metav1.ConditionFalse
		available.Reason = "NoWorkersReady"
		available.Message = "no worker pods are ready"
	}

	progressing := metav1.Condition{
		Type:               condProgressing,
		ObservedGeneration: wp.Generation,
		LastTransitionTime: now,
	}
	// A rollout is in progress until the Deployment's own controller has
	// observed the latest template (ObservedGeneration caught up to
	// Generation) AND every replica is updated, available, and none are
	// unavailable. The ObservedGeneration check is what closes the
	// propagation race: when a spec change (e.g. a `restartedAt` bump from
	// `clrk dev reload`) lands, we patch the Deployment's template, which
	// bumps deploy.Generation immediately — but deploy.Status still reflects
	// the previous, fully-converged ReplicaSet for a beat. Without this check
	// the condition would briefly report DeploymentComplete mid-roll, and a
	// client waiting on it (or the new RS) would act on a stale pod set.
	if deploy.Status.ObservedGeneration < deploy.Generation ||
		deploy.Status.UpdatedReplicas < replicas ||
		deploy.Status.AvailableReplicas < replicas ||
		deploy.Status.UnavailableReplicas > 0 {
		progressing.Status = metav1.ConditionTrue
		progressing.Reason = "DeploymentRollingOut"
		progressing.Message = "deployment is rolling out"
	} else {
		progressing.Status = metav1.ConditionFalse
		progressing.Reason = "DeploymentComplete"
		progressing.Message = "deployment rollout complete"
	}
	meta.SetStatusCondition(&wp.Status.Conditions, available)
	meta.SetStatusCondition(&wp.Status.Conditions, progressing)

	if err := r.Status().Patch(ctx, &wp, client.MergeFrom(statusBase)); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *WorkerPoolDeploymentReconciler) desiredDeployment(wp *clrkv1alpha1.WorkerPool) *appsv1.Deployment {
	replicas := int32(1)
	if wp.Spec.Replicas != nil {
		replicas = *wp.Spec.Replicas
	}

	lbls := map[string]string{
		"clrk.apoxy.dev/workerpool": wp.Name,
		labelComponent:              "worker",
	}

	// Expand the curated overlay onto the fixed gVisor/runsc base. The builder
	// owns every load-bearing field, so whatever the WorkerPool spec carries
	// the worker pods always satisfy the sandbox-runtime invariants. The pool
	// selector labels are stamped via Injections so the Service selector
	// matches; the CLRK_CM_* env is wired from the reconciler's runtime flags.
	podTemplate := workerpod.BuildWorkerPodTemplate(wp.Spec.Template, workerpod.Injections{
		CMOTLPEndpoint: r.CMOTLPEndpoint,
		CMNATSAddr:     r.CMNATSAddr,
		SelectorLabels: lbls,
	})

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      wp.Name + "-workers",
			Namespace: wp.Namespace,
			Labels:    lbls,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: lbls,
			},
			Template: podTemplate,
		},
	}
}

func (r *WorkerPoolDeploymentReconciler) desiredService(wp *clrkv1alpha1.WorkerPool) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      wp.Name + "-workers",
			Namespace: wp.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"clrk.apoxy.dev/workerpool": wp.Name,
				labelComponent:              "worker",
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       8080,
					TargetPort: intstr.FromInt32(8080),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "dispatch",
					Port:       DispatchPort,
					TargetPort: intstr.FromInt32(DispatchPort),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			Type: corev1.ServiceTypeClusterIP,
		},
	}
}

func (r *WorkerPoolDeploymentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("workerpool-deployment").
		For(&clrkv1alpha1.WorkerPool{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
