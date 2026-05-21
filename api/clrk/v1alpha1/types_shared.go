package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// AgentSandbox defines the OCI image and process configuration for an agent.
type AgentSandbox struct {
	// Image is the OCI image reference for the agent.
	Image string `json:"image"`
	// Command overrides the image entrypoint.
	// +optional
	Command []string `json:"command,omitempty"`
	// Args are arguments to the entrypoint.
	// +optional
	Args []string `json:"args,omitempty"`
	// Env is a list of environment variables to set in the sandbox.
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`
}

// ExecutionResources defines per-execution CPU/memory limits enforced by the
// worker runtime via cgroups/gVisor.
type ExecutionResources struct {
	// CPU limit for a single execution. Default "1".
	// +optional
	CPU resource.Quantity `json:"cpu,omitempty"`
	// Memory limit for a single execution. Default "512Mi".
	// +optional
	Memory resource.Quantity `json:"memory,omitempty"`
	// EphemeralStorage limit for a single execution.
	// +optional
	EphemeralStorage *resource.Quantity `json:"ephemeralStorage,omitempty"`
}

// AgentEgressRef references an EgressGateway object for outbound access.
type AgentEgressRef struct {
	// GatewayRef is the name of an EgressGateway in the same namespace.
	GatewayRef string `json:"gatewayRef"`
}

// AgentState configures persistent state across executions.
type AgentState struct {
	// Backend is the storage backend. Currently only "sqlite" is supported.
	// +optional
	Backend string `json:"backend,omitempty"`
	// SizeLimitMB caps the state storage size in megabytes. Default 100.
	// +optional
	SizeLimitMB int32 `json:"sizeLimitMB,omitempty"`
	// MountPath is where state is mounted in the sandbox. Default "/var/clrk/state".
	// Must be a clean absolute path other than "/" and must not overlap with
	// runtime-managed paths (e.g. /proc, /dev, /sys, /tmp, /etc, /usr, /bin,
	// system CA bundles, /etc/resolv.conf). Admission rejects conflicts.
	// +optional
	MountPath string `json:"mountPath,omitempty"`
}

// AgentStreaming configures stdout/stderr streaming behavior.
type AgentStreaming struct {
	// Enabled controls whether streaming is active.
	// +optional
	Enabled bool `json:"enabled,omitempty"`
}

// AgentIdentity configures user identity extraction from incoming requests.
type AgentIdentity struct {
	// Extractors defines one or more identity extraction rules.
	Extractors []IdentityExtractor `json:"extractors"`
}

// IdentityExtractorType defines the type of identity extractor.
// +kubebuilder:validation:Enum=Header;Body;JWT
type IdentityExtractorType string

const (
	IdentityExtractorTypeHeader IdentityExtractorType = "Header"
	IdentityExtractorTypeBody   IdentityExtractorType = "Body"
	IdentityExtractorTypeJWT    IdentityExtractorType = "JWT"
)

// IdentityExtractor defines a single identity extraction rule.
type IdentityExtractor struct {
	// Type selects the extraction method.
	Type IdentityExtractorType `json:"type"`
	// Header extracts identity from an HTTP header.
	// +optional
	Header *HeaderExtractor `json:"header,omitempty"`
	// Body extracts identity from the request body via JSONPath.
	// +optional
	Body *BodyExtractor `json:"body,omitempty"`
	// JWT extracts identity from a JWT claim.
	// +optional
	JWT *JWTExtractor `json:"jwt,omitempty"`
}

// HeaderExtractor extracts a value from an HTTP header.
type HeaderExtractor struct {
	// Name is the HTTP header name.
	Name string `json:"name"`
	// Field is the identity field to populate.
	Field string `json:"field"`
}

// BodyExtractor extracts a value from the request body using JSONPath.
type BodyExtractor struct {
	// JSONPath is the JSONPath expression into the request body.
	JSONPath string `json:"jsonPath"`
	// Field is the identity field to populate.
	Field string `json:"field"`
}

// JWTExtractor extracts a value from a JWT claim.
type JWTExtractor struct {
	// Claim is the JWT claim name.
	Claim string `json:"claim"`
	// Field is the identity field to populate.
	Field string `json:"field"`
}
