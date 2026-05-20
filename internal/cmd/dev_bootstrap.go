package cmd

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"k8s.io/utils/ptr"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/drivers"
	"github.com/apoxy-dev/clrk/internal/ports"
)

// cmAccountName is the name shared by the cm ServiceAccount,
// ClusterRoleBinding, Deployment, and Service. Single source of truth so
// downstream wiring (apiservice ref, --ingress-extproc-host,
// --grpc-advertise-uri) can't drift.
const cmAccountName = "clrk-controller-manager"

// cmLabels are the selector labels stamped on the cm Deployment's pod
// template and matched by its Service. Stable across reloads so a
// rollout never orphans Endpoints.
var cmLabels = map[string]string{
	"app.kubernetes.io/name":      "clrk-controller-manager",
	"app.kubernetes.io/component": "controller-manager",
}

// bootstrapControllerManager creates everything the in-cluster cm needs
// — ServiceAccount + cluster-admin ClusterRoleBinding (dev shortcut) +
// Deployment + Service — and registers the aggregated APIService that
// routes clrk.apoxy.dev/v1alpha1 to the cm pod. Idempotent.
//
// The Service exposes four named ports so a single in-cluster DNS name
// fronts the apiserver (8443), extension gRPC (9443), ingress ext_proc
// (9444), control-plane health (8082) — matching the flags the cm reads.
// The APIService points at port 8443 of this Service; kube-aggregator
// follows the selector to the live cm pod, retiring the hand-managed
// Endpoints object the docker-bridge flow used to need.
func bootstrapControllerManager(ctx context.Context, cluster *drivers.ClusterDriver, image, pullPolicy string) error {
	pull := corev1.PullPolicy(pullPolicy)
	if pull == "" {
		pull = corev1.PullIfNotPresent
	}

	sa := &corev1.ServiceAccount{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
		ObjectMeta: metav1.ObjectMeta{Name: cmAccountName, Namespace: devClrkNamespace},
	}
	// Dev shortcut: cluster-admin gives the cm enough RBAC to install
	// gateway-api CRDs, watch Pods for healthcheck, and own
	// Deployments/Services for WorkerPool. Production deploys swap in a
	// scoped ClusterRole; this binding is named with a `clrk-dev-` prefix
	// so a prod reconcile doesn't fight us.
	crb := &rbacv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "clrk-dev-controller-manager"},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      cmAccountName,
			Namespace: devClrkNamespace,
		}},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "cluster-admin",
		},
	}

	deploy := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmAccountName,
			Namespace: devClrkNamespace,
			Labels:    cmLabels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To[int32](1),
			Selector: &metav1.LabelSelector{MatchLabels: cmLabels},
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RecreateDeploymentStrategyType,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: cmLabels},
				Spec: corev1.PodSpec{
					ServiceAccountName: cmAccountName,
					Containers: []corev1.Container{{
						Name:            "controller-manager",
						Image:           image,
						ImagePullPolicy: pull,
						Args: []string{
							"--db=/var/lib/clrk/data.db",
							"--bind-addr=0.0.0.0",
							"--bind-port=8443",
							// Auth is disabled in dev. The apiserver guard refuses to
							// bind a non-loopback address without this explicit ack —
							// matches the cm-on-docker invocation we're replacing.
							"--insecure-allow-public",
							"--ingress-controller=true",
							"--ingress-extproc-host=" + cmAccountName + "." + devClrkNamespace + ".svc.cluster.local",
							"--egressgateway-controller=true",
							"--grpc-advertise-uri=" + cmAccountName + "." + devClrkNamespace + ".svc.cluster.local:9443",
						},
						Env: []corev1.EnvVar{
							// envoy-go and envoyproxy/go-control-plane both register
							// TLS proto files because envoyproxy/gateway's extension
							// proto transitively imports go-control-plane's TLS types.
							{Name: "GOLANG_PROTOBUF_REGISTRATION_CONFLICT", Value: "ignore"},
							{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
							}},
							{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
							}},
						},
						Ports: []corev1.ContainerPort{
							{Name: "apiserver", ContainerPort: 8443, Protocol: corev1.ProtocolTCP},
							{Name: "grpc", ContainerPort: 9443, Protocol: corev1.ProtocolTCP},
							{Name: "extproc", ContainerPort: 9444, Protocol: corev1.ProtocolTCP},
							{Name: "health", ContainerPort: 8082, Protocol: corev1.ProtocolTCP},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/var/lib/clrk"},
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path:   "/readyz",
									Port:   intstr.FromInt(8082),
									Scheme: corev1.URISchemeHTTP,
								},
							},
							InitialDelaySeconds: 2,
							PeriodSeconds:       2,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path:   "/healthz",
									Port:   intstr.FromInt(8082),
									Scheme: corev1.URISchemeHTTP,
								},
							},
							InitialDelaySeconds: 10,
							PeriodSeconds:       10,
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("100m"),
								corev1.ResourceMemory: resource.MustParse("128Mi"),
							},
						},
					}},
					Volumes: []corev1.Volume{{
						Name:         "data",
						VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
					}},
				},
			},
		},
	}

	svc := &corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmAccountName,
			Namespace: devClrkNamespace,
			Labels:    cmLabels,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: cmLabels,
			Ports: []corev1.ServicePort{
				{Name: "apiserver", Port: 8443, TargetPort: intstr.FromInt(8443), Protocol: corev1.ProtocolTCP},
				{Name: "grpc", Port: 9443, TargetPort: intstr.FromInt(9443), Protocol: corev1.ProtocolTCP},
				{Name: "extproc", Port: 9444, TargetPort: intstr.FromInt(9444), Protocol: corev1.ProtocolTCP},
				{Name: "health", Port: 8082, TargetPort: intstr.FromInt(8082), Protocol: corev1.ProtocolTCP},
			},
		},
	}

	// EG's data-plane bootstrap dials `envoy-gateway.<ns>.svc:18000` for
	// xDS. The cm hosts that xDS listener internally (it supervises an
	// envoy-gateway child); we point a second ClusterIP Service at the
	// same pods to satisfy the well-known name.
	egSvc := &corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "envoy-gateway",
			Namespace: devClrkNamespace,
			Labels:    cmLabels,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: cmLabels,
			Ports: []corev1.ServicePort{
				{Name: "grpc", Port: 18000, TargetPort: intstr.FromInt(18000), Protocol: corev1.ProtocolTCP},
			},
		},
	}

	apiSvc := &apiregistrationv1.APIService{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apiregistration.k8s.io/v1", Kind: "APIService"},
		ObjectMeta: metav1.ObjectMeta{Name: "v1alpha1.clrk.apoxy.dev"},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:                 "clrk.apoxy.dev",
			Version:               "v1alpha1",
			GroupPriorityMinimum:  1000,
			VersionPriority:       15,
			InsecureSkipTLSVerify: true,
			Service: &apiregistrationv1.ServiceReference{
				Name:      cmAccountName,
				Namespace: devClrkNamespace,
				Port:      ptr.To[int32](8443),
			},
		},
	}

	return cluster.ApplyObjects(ctx, sa, crb, deploy, svc, egSvc, apiSvc)
}

// workerAccountName is the ServiceAccount the worker Pods run under.
// The clrk-dev-worker ClusterRoleBinding gives it cluster-admin so the
// worker can watch clrk.apoxy.dev CRDs (TaskAgent, EgressGateway, etc.)
// and update WorkerPool.status. Production deploys swap this for a
// scoped ClusterRole.
const workerAccountName = "clrk-worker"

// bootstrapDefaultWorkerPool applies the worker ServiceAccount +
// ClusterRoleBinding and a `default` WorkerPool whose podTemplate is
// the dev-shaped privileged Pod that runs the worker inside k3d.
// WorkerPoolDeploymentReconciler picks the WorkerPool up and creates
// the matching Deployment + Service. Idempotent (server-side applied
// with field manager `clrk-dev`).
func bootstrapDefaultWorkerPool(ctx context.Context, cluster *drivers.ClusterDriver, image string, replicas int32, pullPolicy string) error {
	pull := corev1.PullPolicy(pullPolicy)
	if pull == "" {
		pull = corev1.PullIfNotPresent
	}

	workerSA := &corev1.ServiceAccount{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
		ObjectMeta: metav1.ObjectMeta{Name: workerAccountName, Namespace: "default"},
	}
	workerCRB := &rbacv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "clrk-dev-worker"},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      workerAccountName,
			Namespace: "default",
		}},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "cluster-admin",
		},
	}
	if err := cluster.ApplyObjects(ctx, workerSA, workerCRB); err != nil {
		return fmt.Errorf("applying worker SA/CRB: %w", err)
	}

	podTemplate := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				// AppArmor unconfined for runsc. K3s on Linux honors this;
				// k3d-on-mac silently ignores it. Setting it covers both.
				"container.apparmor.security.beta.kubernetes.io/worker": "unconfined",
			},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: workerAccountName,
			Containers: []corev1.Container{{
				Name:            "worker",
				Image:           image,
				ImagePullPolicy: pull,
				SecurityContext: &corev1.SecurityContext{
					Privileged: ptr.To(true),
					SeccompProfile: &corev1.SeccompProfile{
						Type: corev1.SeccompProfileTypeUnconfined,
					},
				},
				Env: []corev1.EnvVar{
					{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
					}},
					{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
					}},
					// CLRK_POOL_NAME is auto-injected by
					// WorkerPoolDeploymentReconciler.
				},
				Ports: []corev1.ContainerPort{
					{Name: "dispatch", ContainerPort: ports.DispatchPort, Protocol: corev1.ProtocolTCP},
					{Name: "status", ContainerPort: ports.WorkerStatusPort, Protocol: corev1.ProtocolTCP},
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "state", MountPath: "/var/lib/clrk/state"},
					{Name: "run", MountPath: "/run/clrk"},
					{Name: "varlog", MountPath: "/var/log/clrk"},
				},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/readyz",
							Port: intstr.FromInt(8083),
						},
					},
					InitialDelaySeconds: 2,
					PeriodSeconds:       2,
				},
				LivenessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/healthz",
							Port: intstr.FromInt(8083),
						},
					},
					InitialDelaySeconds: 10,
					PeriodSeconds:       10,
				},
			}},
			Volumes: []corev1.Volume{
				{Name: "state", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				{Name: "run", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				{Name: "varlog", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			},
		},
	}

	wp := &clrkv1alpha1.WorkerPool{
		TypeMeta: metav1.TypeMeta{APIVersion: "clrk.apoxy.dev/v1alpha1", Kind: "WorkerPool"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: "default",
		},
		Spec: clrkv1alpha1.WorkerPoolSpec{
			Replicas:    ptr.To(replicas),
			PodTemplate: podTemplate,
		},
	}
	if err := cluster.ApplyObjects(ctx, wp); err != nil {
		return fmt.Errorf("applying default WorkerPool: %w", err)
	}
	return nil
}
