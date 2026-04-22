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
	"sigs.k8s.io/controller-runtime/pkg/log"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// WorkerPoolDeploymentReconciler owns the k8s-side half of WorkerPool: it
// creates/updates the Deployment + Service that host worker pods and
// reports their health back as ReadyReplicas + Available/Progressing
// conditions. Only wired in cluster mode; clrk dev runs workers directly
// via docker on the host (libcontainer inside a k3s-pod inside docker
// doesn't work cleanly with nested namespaces), so this reconciler is
// skipped there.
type WorkerPoolDeploymentReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch

func (r *WorkerPoolDeploymentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var wp clrkv1alpha1.WorkerPool
	if err := r.Get(ctx, req.NamespacedName, &wp); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 1. CreateOrUpdate Deployment.
	deploy := r.desiredDeployment(&wp)
	if err := ctrl.SetControllerReference(&wp, deploy, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting owner reference: %w", err)
	}
	var existing appsv1.Deployment
	err := r.Get(ctx, client.ObjectKeyFromObject(deploy), &existing)
	if apierrors.IsNotFound(err) {
		logger.Info("Creating Deployment", "name", deploy.Name)
		if err := r.Create(ctx, deploy); err != nil {
			return ctrl.Result{}, fmt.Errorf("creating deployment: %w", err)
		}
		existing = *deploy
	} else if err != nil {
		return ctrl.Result{}, fmt.Errorf("getting deployment: %w", err)
	} else {
		existing.Spec = deploy.Spec
		if err := r.Update(ctx, &existing); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating deployment: %w", err)
		}
	}

	// 2. CreateOrUpdate Service.
	svc := r.desiredService(&wp)
	if err := ctrl.SetControllerReference(&wp, svc, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting Service owner reference: %w", err)
	}
	var existingSvc corev1.Service
	err = r.Get(ctx, client.ObjectKeyFromObject(svc), &existingSvc)
	if apierrors.IsNotFound(err) {
		logger.Info("Creating Service", "name", svc.Name)
		if err := r.Create(ctx, svc); err != nil {
			return ctrl.Result{}, fmt.Errorf("creating service: %w", err)
		}
	} else if err != nil {
		return ctrl.Result{}, fmt.Errorf("getting service: %w", err)
	} else {
		existingSvc.Spec.Selector = svc.Spec.Selector
		existingSvc.Spec.Ports = svc.Spec.Ports
		existingSvc.Spec.Type = svc.Spec.Type
		if err := r.Update(ctx, &existingSvc); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating service: %w", err)
		}
	}

	// 3. Status: ReadyReplicas + Available/Progressing conditions.
	replicas := int32(1)
	if wp.Spec.Replicas != nil {
		replicas = *wp.Spec.Replicas
	}
	readyReplicas := existing.Status.ReadyReplicas
	wp.Status.ReadyReplicas = readyReplicas

	now := metav1.Now()

	available := metav1.Condition{
		Type:               "Available",
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
		Type:               "Progressing",
		ObservedGeneration: wp.Generation,
		LastTransitionTime: now,
	}
	if existing.Status.UpdatedReplicas < replicas ||
		existing.Status.AvailableReplicas < replicas {
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

	if err := r.Status().Update(ctx, &wp); err != nil {
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
