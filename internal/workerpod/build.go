// Package workerpod assembles the worker pod template that hosts agent
// sandboxes. It is the single source of truth for the fixed gVisor/runsc
// invariants -- privileged + seccomp Unconfined + the AppArmor annotation, the
// run/state/varlog scratch volumes, the dispatch port, and the downward-API
// env -- that the runsc gofer fork and the worker dispatcher depend on.
//
// A WorkerPool exposes only a curated overlay (clrkv1alpha1.WorkerPodTemplate);
// the deployment reconciler calls BuildWorkerPodTemplate to expand that overlay
// onto this base. A user can tune image/resources/scheduling but cannot express
// -- and therefore cannot break -- the invariants.
//
// It is a leaf package (corev1 + the clrk API types + the leaf ports/otelemit/
// invevent constant packages) so the controller imports it without taking on
// the installer's dependency surface.
package workerpod

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/invevent"
	"github.com/apoxy-dev/clrk/internal/otelemit"
	"github.com/apoxy-dev/clrk/internal/ports"
)

// poolNameLabelKey is the pod label the worker reads (via the downward API) to
// learn which WorkerPool it belongs to. It must match the selector label the
// deployment reconciler stamps (clrk.apoxy.dev/workerpool, no hyphen), NOT
// clrkv1alpha1.LabelWorkerPool (clrk.apoxy.dev/worker-pool), which is the
// AgentSandboxRevision watch label.
const poolNameLabelKey = "clrk.apoxy.dev/workerpool"

// healthProbePort is the worker's HTTP readiness/liveness port.
const healthProbePort = 8083

// Injections are the controller-supplied values stamped onto the worker pod.
// The installer writes only the WorkerPool overlay and never calls the builder,
// so it never sets these.
type Injections struct {
	// CMOTLPEndpoint, when set, becomes CLRK_CM_OTLP_ENDPOINT so the worker's
	// OTLP emitter ships signals to the controller-manager's receiver.
	CMOTLPEndpoint string
	// CMNATSAddr, when set, becomes CLRK_CM_NATS_ADDR so the dispatcher's
	// invocation publisher can dial the cm's embedded JetStream.
	CMNATSAddr string
	// SelectorLabels are merged onto the pod template last so the WorkerPool
	// Service selector matches; they win over any user label of the same key.
	SelectorLabels map[string]string
}

// BuildWorkerPodTemplate expands the curated overlay onto the fixed
// gVisor/runsc base and returns the full pod template the worker Deployment
// runs. Every invariant is owned here; overlay values add to but never override
// a load-bearing field.
func BuildWorkerPodTemplate(t clrkv1alpha1.WorkerPodTemplate, inj Injections) corev1.PodTemplateSpec {
	sa := t.ServiceAccountName
	if sa == "" {
		sa = clrkv1alpha1.WorkerServiceAccountName
	}
	pull := t.ImagePullPolicy
	if pull == "" {
		pull = corev1.PullIfNotPresent
	}

	// Annotations: the AppArmor-unconfined annotation always wins; user
	// annotations (e.g. clrk.apoxy.dev/restartedAt, which triggers rollouts)
	// fill the rest.
	annotations := map[string]string{
		clrkv1alpha1.WorkerAppArmorAnnotation: "unconfined",
	}
	for k, v := range t.Metadata.Annotations {
		if k == clrkv1alpha1.WorkerAppArmorAnnotation {
			continue
		}
		annotations[k] = v
	}

	// Labels: user labels first, then the controller selector labels win.
	labels := make(map[string]string, len(t.Metadata.Labels)+len(inj.SelectorLabels))
	for k, v := range t.Metadata.Labels {
		labels[k] = v
	}
	for k, v := range inj.SelectorLabels {
		labels[k] = v
	}

	// Reserved scratch volumes + their container mounts (runsc state, agent
	// state, stdio tee). EmptyDir, per-pod, owned by the runtime.
	volumes := make([]corev1.Volume, 0, len(clrkv1alpha1.WorkerReservedVolumes)+len(t.ExtraVolumes))
	mounts := make([]corev1.VolumeMount, 0, len(clrkv1alpha1.WorkerReservedVolumes)+len(t.ExtraVolumeMounts))
	for _, rv := range clrkv1alpha1.WorkerReservedVolumes {
		volumes = append(volumes, corev1.Volume{
			Name:         rv.Name,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: rv.Name, MountPath: rv.MountPath})
	}
	volumes = append(volumes, t.ExtraVolumes...)
	mounts = append(mounts, t.ExtraVolumeMounts...)

	// Env: fixed downward-API vars, the pool-name ref, and the cm wiring
	// first; then user additions whose names do not collide with that reserved
	// set (the reserved var wins).
	env := []corev1.EnvVar{
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
		}},
		{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
		}},
		{Name: "CLRK_POOL_NAME", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: "metadata.labels['" + poolNameLabelKey + "']",
			},
		}},
	}
	// POD_NAME/POD_NAMESPACE/CLRK_POOL_NAME are always controller-owned
	// (downward API); a user value for those is dropped. The CLRK_CM_* wiring,
	// by contrast, is a controller-supplied default the operator may override:
	// inject it only when the overlay did not already set the var, so an
	// operator-set endpoint wins -- matching the pre-overlay reconciler, which
	// injected behind a !hasEnv guard.
	reserved := map[string]bool{
		"POD_NAME":       true,
		"POD_NAMESPACE":  true,
		"CLRK_POOL_NAME": true,
	}
	if inj.CMOTLPEndpoint != "" && !hasEnv(t.Env, otelemit.CMOTLPEndpointEnv) {
		env = append(env, corev1.EnvVar{Name: otelemit.CMOTLPEndpointEnv, Value: inj.CMOTLPEndpoint})
	}
	if inj.CMNATSAddr != "" && !hasEnv(t.Env, invevent.CMNATSAddrEnv) {
		env = append(env, corev1.EnvVar{Name: invevent.CMNATSAddrEnv, Value: inj.CMNATSAddr})
	}
	for _, e := range t.Env {
		if reserved[e.Name] {
			continue
		}
		env = append(env, e)
	}

	worker := corev1.Container{
		Name:            clrkv1alpha1.WorkerContainerName,
		Image:           t.Image,
		ImagePullPolicy: pull,
		Resources:       t.Resources,
		SecurityContext: &corev1.SecurityContext{
			Privileged: ptr.To(true),
			SeccompProfile: &corev1.SeccompProfile{
				Type: corev1.SeccompProfileTypeUnconfined,
			},
		},
		Env: env,
		Ports: []corev1.ContainerPort{
			{Name: clrkv1alpha1.WorkerDispatchPortName, ContainerPort: ports.DispatchPort, Protocol: corev1.ProtocolTCP},
		},
		VolumeMounts: mounts,
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromInt(healthProbePort)},
			},
			InitialDelaySeconds: 2,
			PeriodSeconds:       2,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt(healthProbePort)},
			},
			InitialDelaySeconds: 10,
			PeriodSeconds:       10,
		},
	}

	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.PodSpec{
			ServiceAccountName:        sa,
			Containers:                []corev1.Container{worker},
			Volumes:                   volumes,
			ImagePullSecrets:          t.ImagePullSecrets,
			NodeSelector:              t.NodeSelector,
			Tolerations:               t.Tolerations,
			Affinity:                  t.Affinity,
			PriorityClassName:         t.PriorityClassName,
			TopologySpreadConstraints: t.TopologySpreadConstraints,
		},
	}
}

// hasEnv reports whether env already contains a variable named name.
func hasEnv(env []corev1.EnvVar, name string) bool {
	for _, e := range env {
		if e.Name == name {
			return true
		}
	}
	return false
}
