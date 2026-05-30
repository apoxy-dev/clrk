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

	// PodTemplate defines the pod specification for worker pods.
	PodTemplate corev1.PodTemplateSpec `json:"podTemplate"`

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

// ImageCacheConfig configures OCI image caching on worker nodes.
type ImageCacheConfig struct {
	// Enabled controls whether image caching is active.
	// +optional
	Enabled bool `json:"enabled,omitempty"`
}

type WorkerPoolStatus struct {
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
