package install

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/clickhouse"
	clrknats "github.com/apoxy-dev/clrk/internal/nats"
	"github.com/apoxy-dev/clrk/internal/otelemit"
	"github.com/apoxy-dev/clrk/internal/ports"
)

// svcDNS returns the in-cluster DNS name of the cm Service for p's namespace.
func (p Profile) svcDNS() string {
	return ControllerManagerName + "." + p.Namespace + ".svc.cluster.local"
}

// BuildControllerManager returns the full ordered control-plane object set
// (SA, RBAC, Services, APIService, PVCs, Deployment) for rendering/planning.
// The Deployment is last so callers that apply-and-verify it can split the
// slice if they prefer; ApplyControllerManager does that split internally.
func BuildControllerManager(p Profile) []client.Object {
	pre, deploy := controllerManagerObjects(p)
	return append(pre, deploy)
}

// BuildWorkerPool returns the worker RBAC + WorkerPool CR for
// rendering/planning.
func BuildWorkerPool(p Profile) []client.Object {
	pre, wp := workerPoolObjects(p)
	return append(pre, wp)
}

// controllerManagerObjects builds the cm control-plane objects. preDeploy are
// applied with plain SSA; deploy goes through ApplyAndVerify (env-count/image
// stripping guard). Faithful refactor of the dev bootstrap, parameterized by
// Profile — a Dev profile reproduces the previous objects byte-for-byte.
func controllerManagerObjects(p Profile) (preDeploy []client.Object, deploy *appsv1.Deployment) {
	pull := p.pullPolicy()

	sa := &corev1.ServiceAccount{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
		ObjectMeta: metav1.ObjectMeta{Name: ControllerManagerName, Namespace: p.Namespace},
	}

	// Apiserver/cm arguments. The base set is identical across dev + cluster
	// (auth is disabled in v1, so --insecure-allow-public is required to bind
	// 0.0.0.0 in both); the dev-only flags are appended conditionally so a
	// cluster profile omits them.
	args := []string{
		"--db=/var/lib/clrk/data.db",
		"--bind-addr=0.0.0.0",
		"--bind-port=8443",
		// Auth is disabled in v1. The apiserver guard refuses to bind a
		// non-loopback address without this explicit ack.
		"--insecure-allow-public",
		"--ingress-controller=true",
		"--ingress-extproc-host=" + p.svcDNS(),
		"--egressgateway-controller=true",
		"--grpc-advertise-uri=" + p.svcDNS() + ":9443",
		// Worker pods dial these for Invocation lifecycle publishing — the cm
		// Service DNS, not the default in-cluster controller-manager name.
		fmt.Sprintf("--nats-advertise-addr=%s:%d", p.svcDNS(), ports.NATSClientPort),
		"--otlp-advertise-uri=http://" + p.svcDNS() + ":4318",
	}
	if p.OTLPFallbackEndpoint != "" {
		args = append(args, "--dev-otlp-fallback-endpoint="+p.OTLPFallbackEndpoint)
	}
	if p.EnableTestWrites {
		args = append(args, "--enable-invocation-test-writes=true")
	}
	if p.EgressBackendHost != "" {
		args = append(args, "--dev-egress-backend-host="+p.EgressBackendHost)
	}
	// On a cluster the cm serves the aggregated API with a real cert mounted at
	// --cert-dir (cert-manager- or installer-minted); dev leaves it empty and
	// self-signs in-memory (with InsecureSkipTLSVerify on the APIService).
	if p.TLS != TLSInsecureSkipVerify {
		args = append(args, "--cert-dir="+servingCertMountPath)
	}

	// Volumes/mounts: the three embedded-store PVCs always; the serving-cert
	// Secret when a real cert is wired.
	volumeMounts := []corev1.VolumeMount{
		{Name: "data", MountPath: "/var/lib/clrk"},
		{Name: clickhouseDataPVC, MountPath: clickhouse.DefaultDataDir},
		{Name: natsDataPVC, MountPath: clrknats.DefaultStoreDir},
	}
	volumes := []corev1.Volume{
		{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: kineDataPVC}}},
		{Name: clickhouseDataPVC, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: clickhouseDataPVC}}},
		{Name: natsDataPVC, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: natsDataPVC}}},
	}
	if p.TLS != TLSInsecureSkipVerify {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: servingCertVolumeName, MountPath: servingCertMountPath, ReadOnly: true})
		volumes = append(volumes, corev1.Volume{Name: servingCertVolumeName, VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: servingCertSecretName},
		}})
	}

	var hostAliases []corev1.HostAlias
	if p.HostAliasIP != "" {
		// Docker Desktop injects host.docker.internal into the /etc/hosts of
		// containers it spawns directly, but not into k3d-nested Pods — so we
		// plant it ourselves.
		hostAliases = []corev1.HostAlias{{
			IP:        p.HostAliasIP,
			Hostnames: []string{"host.docker.internal"},
		}}
	}

	replicas := p.Replicas
	if replicas == 0 {
		replicas = 1
	}

	// Pull secrets for private registries (cluster-only; empty/nil in dev, which
	// keeps the dev PodSpec byte-identical).
	var imagePullSecrets []corev1.LocalObjectReference
	if p.ImagePullSecret != "" {
		imagePullSecrets = []corev1.LocalObjectReference{{Name: p.ImagePullSecret}}
	}

	// Requests in both profiles; a cluster also caps the cm with limits (it
	// co-hosts kine + ClickHouse + NATS + the EG control plane in one pod). Dev
	// stays requests-only to preserve the existing bootstrap object byte-for-byte.
	resources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
	}
	if !p.Dev {
		resources.Limits = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("2Gi"),
		}
	}

	deploy = &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      ControllerManagerName,
			Namespace: p.Namespace,
			Labels:    cmLabels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: cmLabels},
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RecreateDeploymentStrategyType,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: cmLabels},
				Spec: corev1.PodSpec{
					ServiceAccountName: ControllerManagerName,
					HostAliases:        hostAliases,
					Containers: []corev1.Container{{
						Name:            "controller-manager",
						Image:           p.ControllerImage,
						ImagePullPolicy: pull,
						Args:            args,
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
							{Name: otelemit.CMOTLPEndpointEnv, Value: "http://127.0.0.1:4318"},
						},
						Ports: []corev1.ContainerPort{
							{Name: "apiserver", ContainerPort: 8443, Protocol: corev1.ProtocolTCP},
							{Name: "grpc", ContainerPort: 9443, Protocol: corev1.ProtocolTCP},
							{Name: "extproc", ContainerPort: 9444, Protocol: corev1.ProtocolTCP},
							{Name: "health", ContainerPort: 8082, Protocol: corev1.ProtocolTCP},
							{Name: "admin", ContainerPort: 8085, Protocol: corev1.ProtocolTCP},
							{Name: "otlp", ContainerPort: 4318, Protocol: corev1.ProtocolTCP},
							{Name: "nats", ContainerPort: ports.NATSClientPort, Protocol: corev1.ProtocolTCP},
						},
						VolumeMounts: volumeMounts,
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
						Resources: resources,
					}},
					ImagePullSecrets: imagePullSecrets,
					Volumes:          volumes,
				},
			},
		},
	}

	svc := &corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      ControllerManagerName,
			Namespace: p.Namespace,
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
				{Name: "admin", Port: 8085, TargetPort: intstr.FromInt(8085), Protocol: corev1.ProtocolTCP},
				{Name: "otlp", Port: 4318, TargetPort: intstr.FromInt(4318), Protocol: corev1.ProtocolTCP},
				{Name: "nats", Port: ports.NATSClientPort, TargetPort: intstr.FromInt(int(ports.NATSClientPort)), Protocol: corev1.ProtocolTCP},
			},
		},
	}

	chPVC := newPVC(clickhouseDataPVC, p.Namespace, "5Gi", p.StorageClass)
	natsPVC := newPVC(natsDataPVC, p.Namespace, "1Gi", p.StorageClass)
	kinePVC := newPVC(kineDataPVC, p.Namespace, "1Gi", p.StorageClass)

	// EG's data-plane bootstrap dials `envoy-gateway.<ns>.svc:18000` for xDS.
	// The cm hosts that xDS listener internally; this second ClusterIP Service
	// satisfies the well-known name.
	egSvc := &corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      EnvoyGatewayServiceName,
			Namespace: p.Namespace,
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
		ObjectMeta: metav1.ObjectMeta{Name: APIServiceName},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:                "clrk.apoxy.dev",
			Version:              "v1alpha1",
			GroupPriorityMinimum: 1000,
			VersionPriority:      15,
			Service: &apiregistrationv1.ServiceReference{
				Name:      ControllerManagerName,
				Namespace: p.Namespace,
				Port:      ptr.To[int32](8443),
			},
		},
	}
	// TLS posture: dev/insecure skips verification; the cert-backed modes set
	// caBundle (self-signed) or rely on the cert-manager CA injector (M4
	// populates CABundle / the inject-ca-from annotation on the cluster path).
	switch p.TLS {
	case TLSInsecureSkipVerify:
		apiSvc.Spec.InsecureSkipTLSVerify = true
	case TLSSelfSigned:
		apiSvc.Spec.CABundle = p.CABundle
	case TLSCertManager:
		if p.CertManagerCertRef != "" {
			apiSvc.ObjectMeta.Annotations = map[string]string{
				"cert-manager.io/inject-ca-from": p.CertManagerCertRef,
			}
		}
	}

	// Order: SA, then its (Cluster)Role(Binding), then the Services/APIService/
	// PVCs, then the NetworkPolicy. A Dev profile yields the same set the dev
	// bootstrap applied before this refactor (cluster-admin binding, no policy).
	preDeploy = []client.Object{sa}
	preDeploy = append(preDeploy, flattenRBAC(controllerManagerRBAC(p))...)
	preDeploy = append(preDeploy, svc, egSvc, apiSvc, chPVC, natsPVC, kinePVC)
	if np := controllerManagerNetworkPolicy(p); np != nil {
		preDeploy = append(preDeploy, np)
	}
	return preDeploy, deploy
}

// newPVC builds a single-replica RWO PVC. storageClass "" leaves the field
// unset (cluster default / k3d local-path).
func newPVC(name, namespace, size, storageClass string) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(size),
				},
			},
		},
	}
	if storageClass != "" {
		pvc.Spec.StorageClassName = ptr.To(storageClass)
	}
	return pvc
}

// workerPoolObjects builds the worker SA + ClusterRoleBinding (preWP) and the
// `default` WorkerPool CR (wp, ApplyAndVerify'd). The CR carries only the
// curated overlay (image/pull policy/SA/pull secrets); the controller's pod
// builder (internal/workerpod) expands it onto the fixed gVisor/runsc base
// (privileged + seccomp-unconfined + AppArmor-unconfined + the reserved
// volumes/port/env), so those invariants can't be stripped by an edit and the
// installer no longer has to carry them verbatim.
func workerPoolObjects(p Profile) (preWP []client.Object, wp *clrkv1alpha1.WorkerPool) {
	pull := p.pullPolicy()

	workerSA := &corev1.ServiceAccount{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
		ObjectMeta: metav1.ObjectMeta{Name: WorkerAccountName, Namespace: p.WorkerNamespace},
	}

	var imagePullSecrets []corev1.LocalObjectReference
	if p.ImagePullSecret != "" {
		imagePullSecrets = []corev1.LocalObjectReference{{Name: p.ImagePullSecret}}
	}

	wp = &clrkv1alpha1.WorkerPool{
		TypeMeta: metav1.TypeMeta{APIVersion: "clrk.apoxy.dev/v1alpha1", Kind: "WorkerPool"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: p.WorkerNamespace,
		},
		Spec: clrkv1alpha1.WorkerPoolSpec{
			Replicas: ptr.To(p.Workers),
			Template: clrkv1alpha1.WorkerPodTemplate{
				Image:              p.WorkerImage,
				ImagePullPolicy:    pull,
				ServiceAccountName: WorkerAccountName,
				ImagePullSecrets:   imagePullSecrets,
			},
		},
	}

	preWP = []client.Object{workerSA}
	preWP = append(preWP, flattenRBAC(workerRBAC(p))...)
	return preWP, wp
}
