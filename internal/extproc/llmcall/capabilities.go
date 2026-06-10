package llmcall

// Capabilities declares what a provider's wire schema can express.
// Backend selection (Phase 2, APO-742) filters cross-schema candidates
// by comparing a decoded request's feature use against the target
// provider's Capabilities, so values must be honest: claim only what
// the codec can actually encode.
//
// +k8s:deepcopy-gen=true
type Capabilities struct {
	// Operations are the gen_ai.operation.name values the schema
	// serves ("chat", "text_completion", "embeddings", ...).
	Operations []string `json:"operations,omitempty" yaml:"operations,omitempty"`

	Tools             bool `json:"tools,omitempty" yaml:"tools,omitempty"`
	ParallelToolCalls bool `json:"parallelToolCalls,omitempty" yaml:"parallelToolCalls,omitempty"`
	ImageInput        bool `json:"imageInput,omitempty" yaml:"imageInput,omitempty"`
	AudioInput        bool `json:"audioInput,omitempty" yaml:"audioInput,omitempty"`
	FileInput         bool `json:"fileInput,omitempty" yaml:"fileInput,omitempty"`
	Reasoning         bool `json:"reasoning,omitempty" yaml:"reasoning,omitempty"`
	StructuredOutput  bool `json:"structuredOutput,omitempty" yaml:"structuredOutput,omitempty"`
	Streaming         bool `json:"streaming,omitempty" yaml:"streaming,omitempty"`
	// SystemRole reports whether the schema carries system content
	// natively (top-level field or system role) rather than requiring
	// it to be folded into the first user message.
	SystemRole bool `json:"systemRole,omitempty" yaml:"systemRole,omitempty"`
}
