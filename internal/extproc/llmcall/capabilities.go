package llmcall

// Capabilities declares what a provider's wire schema can express.
// Backend selection (Phase 2, APO-742) filters cross-schema candidates
// by comparing a decoded request's feature use against the target
// provider's Capabilities, so values must be honest: claim only what
// the codec can actually encode.
type Capabilities struct {
	// Operations are the gen_ai.operation.name values the schema
	// serves ("chat", "text_completion", "embeddings", ...).
	Operations []string

	Tools             bool
	ParallelToolCalls bool
	ImageInput        bool
	AudioInput        bool
	FileInput         bool
	Reasoning         bool
	StructuredOutput  bool
	Streaming         bool
	// SystemRole reports whether the schema carries system content
	// natively (top-level field or system role) rather than requiring
	// it to be folded into the first user message.
	SystemRole bool
}
