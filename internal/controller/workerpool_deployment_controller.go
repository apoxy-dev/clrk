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
	"github.com/apoxy-dev/clrk/internal/otelemit"
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
	if deploy.Status.UpdatedReplicas < replicas ||
		deploy.Status.AvailableReplicas < replicas {
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

	// Merge the pool labels into the pod template so the Service selector
	// matches.
	podTemplate := wp.Spec.PodTemplate.DeepCopy()
	if podTemplate.Labels == nil {
		podTemplate.Labels = make(map[string]string)
	}
	for k, v := range lbls {
		podTemplate.Labels[k] = v
	}

	// Inject CLRK_POOL_NAME into every container in the template via
	// downward API off the workerpool label we just stamped. The worker
	// binary reads this to advertise itself into WorkerPool.status.workers.
	// Skipped per-container if the operator already set CLRK_POOL_NAME
	// explicitly (manual override wins).
	for i := range podTemplate.Spec.Containers {
		c := &podTemplate.Spec.Containers[i]
		if !hasEnv(c.Env, "CLRK_POOL_NAME") {
			c.Env = append(c.Env, corev1.EnvVar{
				Name: "CLRK_POOL_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.labels['clrk.apoxy.dev/workerpool']",
					},
				},
			})
		}
		// CLRK_CM_OTLP_ENDPOINT points the worker's OTLP emitter at
		// the controller-manager's receiver, which persists every
		// signal to the embedded ClickHouse and re-exports per-EG to
		// the customer's collector. Empty when the reconciler has no
		// CM endpoint configured (single-binary mode); the worker
		// falls back to a noop emitter in that case.
		if r.CMOTLPEndpoint != "" && !hasEnv(c.Env, otelemit.CMOTLPEndpointEnv) {
			c.Env = append(c.Env, corev1.EnvVar{
				Name:  otelemit.CMOTLPEndpointEnv,
				Value: r.CMOTLPEndpoint,
			})
		}
	}

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
			Template: *podTemplate,
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

func hasEnv(env []corev1.EnvVar, name string) bool {
	for _, e := range env {
		if e.Name == name {
			return true
		}
	}
	return false
}

func (r *WorkerPoolDeploymentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("workerpool-deployment").
		For(&clrkv1alpha1.WorkerPool{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
