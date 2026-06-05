/*
Copyright 2026 Apoxy, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WorkerPool defines a pool of worker pods that host agent executions.
// Teams own their pools with independent sizing, node placement, and runtime
// configuration. Agents reference WorkerPool by name in the same namespace.
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=wp
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Active",type=integer,JSONPath=`.status.activeExecutions`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type WorkerPool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              WorkerPoolSpec   `json:"spec"`
	Status            WorkerPoolStatus `json:"status,omitempty"`
}

type WorkerPoolSpec struct {
	// Replicas is the desired number of worker pods.
	// +kubebuilder:default=1
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Template is the curated worker-pod overlay. It exposes only the pod-spec
	// knobs that are safe to tune; the gVisor/runsc-load-bearing fields
	// (privileged + seccomp Unconfined + the AppArmor annotation, the
	// run/state/varlog scratch volumes, the dispatch port, and the downward-API
	// env) are owned by the controller's pod builder and are not expressible
	// here, so a user edit cannot break the sandbox runtime.
	Template WorkerPodTemplate `json:"template"`

	// MaxExecutionsPerWorker caps how many agent executions a single worker pod
	// can host simultaneously.
	// +optional
	MaxExecutionsPerWorker *int32 `json:"maxExecutionsPerWorker,omitempty"`

	// WarmPool is the number of pre-spawned sandboxes to keep ready.
	// +optional
	WarmPool *int32 `json:"warmPool,omitempty"`

	// ImageCache configures OCI image caching on worker nodes.
	// +optional
	ImageCache *ImageCacheConfig `json:"imageCache,omitempty"`
}

// WorkerPodTemplate is the curated set of worker-pod knobs a pool owner may
// set. Every field is merged onto a fixed, code-owned base by the controller's
// pod builder (internal/workerpod). Fields that would compromise the runsc
// sandbox -- the security context, the worker container/ports, the reserved
// volumes, and the host namespaces -- are intentionally absent.
type WorkerPodTemplate struct {
	// Image is the worker container image. Required. It must be a clrk worker
	// image (runsc + the sentrystack plugin); an arbitrary image will not boot
	// the sandbox runtime.
	Image string `json:"image"`

	// ImagePullPolicy for the worker container. Defaults to IfNotPresent.
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSecrets are attached to the worker pod for pulling Image.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// Resources are the worker container's compute resource requests/limits.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Env is additional environment for the worker container, appended after
	// the fixed downward-API vars (POD_NAME, POD_NAMESPACE, CLRK_POOL_NAME). A
	// var named POD_NAME/POD_NAMESPACE/CLRK_POOL_NAME is always dropped (the
	// controller owns it). CLRK_CM_OTLP_ENDPOINT/CLRK_CM_NATS_ADDR are dropped
	// only when the controller injects them (cm mode); in single-binary mode
	// the controller injects nothing, so an operator-supplied value here is
	// kept.
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`

	// NodeSelector constrains worker pods to matching nodes.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations let worker pods schedule onto tainted nodes.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// Affinity is the worker pods' scheduling affinity.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// PriorityClassName sets the worker pods' scheduling priority.
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// TopologySpreadConstraints spread worker pods across failure domains.
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`

	// ServiceAccountName the worker pods run under. Defaults to clrk-worker.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// ExtraVolumes are additional pod volumes. Names colliding with the
	// reserved runtime volumes (state, run, varlog) are rejected at admission.
	// +optional
	ExtraVolumes []corev1.Volume `json:"extraVolumes,omitempty"`

	// ExtraVolumeMounts are additional mounts on the worker container. Names
	// colliding with a reserved volume, or mount paths overlapping a reserved
	// runtime path, are rejected at admission.
	// +optional
	ExtraVolumeMounts []corev1.VolumeMount `json:"extraVolumeMounts,omitempty"`

	// Metadata holds pod labels and annotations merged onto the base. The
	// controller-owned pool selector labels and the AppArmor annotation always
	// win on key collision, so user entries can add but never clobber a
	// load-bearing key. Setting the clrk.apoxy.dev/restartedAt annotation here
	// triggers a rolling restart, the same mechanism as `kubectl rollout
	// restart`.
	// +optional
	Metadata WorkerPodMeta `json:"metadata,omitempty"`
}

// WorkerPodMeta is the user-settable subset of the worker pod's ObjectMeta.
type WorkerPodMeta struct {
	// Labels are merged into the pod template; the pool selector labels win on
	// key collision.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations are merged into the pod template; the AppArmor annotation
	// wins on key collision.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// ImageCacheConfig configures OCI image caching on worker nodes.
type ImageCacheConfig struct {
	// Enabled controls whether image caching is active.
	// +optional
	Enabled bool `json:"enabled,omitempty"`
}

type WorkerPoolStatus struct {
	// ObservedGeneration is the most recent metadata.generation the controller
	// has reconciled. Clients compare it against metadata.generation to tell
	// whether the controller has acted on the latest spec; together with the
	// Available/Progressing conditions it gives a race-free rollout signal.
	// `clrk dev reload worker` waits on this rather than polling the
	// controller-owned Deployment, whose status lags this resource (polling the
	// Deployment can observe the pre-reconcile converged state and report
	// "rolled out" before the roll has even started).
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// ReadyReplicas is the number of worker pods that are ready.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`
	// Capacity is the aggregate capacity across all ready workers.
	// +optional
	Capacity WorkerPoolCapacity `json:"capacity,omitempty"`
	// ActiveExecutions is the total number of in-flight (non-terminal)
	// Invocations across all workers in the pool. See Invocation.
	// +optional
	ActiveExecutions int32 `json:"activeExecutions,omitempty"`
	// Conditions represent the latest available observations of the WorkerPool's state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// WorkerPoolCapacity describes the aggregate execution capacity of a WorkerPool.
type WorkerPoolCapacity struct {
	// MaxExecutions is the total execution slots across all ready workers.
	// +optional
	MaxExecutions int32 `json:"maxExecutions,omitempty"`
	// AvailableExecutions is the number of free execution slots.
	// +optional
	AvailableExecutions int32 `json:"availableExecutions,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// WorkerPoolList contains a list of WorkerPool resources.
type WorkerPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WorkerPool `json:"items"`
}
