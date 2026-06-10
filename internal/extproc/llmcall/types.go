package llmcall

import "encoding/json"

// Role is a conversation participant. Values are the OTel GenAI
// semconv role spellings.
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// PartType discriminates the Part union. Values are the OTel GenAI
// semconv message-part type spellings where semconv defines one
// (text, tool_call, tool_call_response, reasoning); the remaining
// kinds extend that vocabulary with the same naming convention.
type PartType string

const (
	PartTypeText             PartType = "text"
	PartTypeImage            PartType = "image"
	PartTypeAudio            PartType = "audio"
	PartTypeFile             PartType = "file"
	PartTypeToolCall         PartType = "tool_call"
	PartTypeToolCallResponse PartType = "tool_call_response"
	PartTypeReasoning        PartType = "reasoning"
	PartTypeRefusal          PartType = "refusal"
	PartTypeCitation         PartType = "citation"
	// PartTypeUnknown marks a provider content block the IR doesn't
	// model. The verbatim block rides in Wire.Raw: re-emitted in
	// preserve mode, dropped and counted in strip mode.
	PartTypeUnknown PartType = ""
)

// FinishReason is the canonical reason generation stopped. Values are
// the OTel GenAI semconv finish-reason spellings. Codecs that map
// several provider literals onto one canonical value keep the original
// in Choice.FinishReasonRaw so preserve-mode encoding round-trips.
type FinishReason string

const (
	FinishReasonStop          FinishReason = "stop"
	FinishReasonLength        FinishReason = "length"
	FinishReasonToolCalls     FinishReason = "tool_calls"
	FinishReasonContentFilter FinishReason = "content_filter"
	FinishReasonError         FinishReason = "error"
)

// Part is one typed unit of message content — a tagged union in the
// same idiom as the CRD filter types: Type selects which variant
// pointer is set. PartTypeUnknown (the zero value) means no variant is
// set and Wire.Raw holds the unmodeled provider block verbatim.
//
// The json/yaml tags on IR types serve debug dumps and golden tests;
// the wire bytes are owned by the codecs, never by these tags.
//
// +k8s:deepcopy-gen=true
type Part struct {
	Type PartType `json:"type,omitempty" yaml:"type,omitempty"`

	Text             *TextPart             `json:"text,omitempty" yaml:"text,omitempty"`
	Image            *ImagePart            `json:"image,omitempty" yaml:"image,omitempty"`
	Audio            *AudioPart            `json:"audio,omitempty" yaml:"audio,omitempty"`
	File             *FilePart             `json:"file,omitempty" yaml:"file,omitempty"`
	ToolCall         *ToolCallPart         `json:"toolCall,omitempty" yaml:"toolCall,omitempty"`
	ToolCallResponse *ToolCallResponsePart `json:"toolCallResponse,omitempty" yaml:"toolCallResponse,omitempty"`
	Reasoning        *ReasoningPart        `json:"reasoning,omitempty" yaml:"reasoning,omitempty"`
	Refusal          *RefusalPart          `json:"refusal,omitempty" yaml:"refusal,omitempty"`
	Citation         *CitationPart         `json:"citation,omitempty" yaml:"citation,omitempty"`

	Wire Wire `json:"wire,omitempty" yaml:"wire,omitempty"`
}

// TextPart is plain text content.
//
// +k8s:deepcopy-gen=true
type TextPart struct {
	Text string `json:"text,omitempty" yaml:"text,omitempty"`
	Wire Wire   `json:"wire,omitempty" yaml:"wire,omitempty"`
}

// ImagePart is image content, carried inline (base64) or by reference.
// Exactly one of DataB64/URL is set; MIMEType qualifies inline data.
//
// +k8s:deepcopy-gen=true
type ImagePart struct {
	MIMEType string `json:"mimeType,omitempty" yaml:"mimeType,omitempty"`
	DataB64  string `json:"dataB64,omitempty" yaml:"dataB64,omitempty"`
	URL      string `json:"url,omitempty" yaml:"url,omitempty"`
	Wire     Wire   `json:"wire,omitempty" yaml:"wire,omitempty"`
}

// AudioPart is audio content; field semantics mirror ImagePart.
//
// +k8s:deepcopy-gen=true
type AudioPart struct {
	MIMEType string `json:"mimeType,omitempty" yaml:"mimeType,omitempty"`
	DataB64  string `json:"dataB64,omitempty" yaml:"dataB64,omitempty"`
	URL      string `json:"url,omitempty" yaml:"url,omitempty"`
	Wire     Wire   `json:"wire,omitempty" yaml:"wire,omitempty"`
}

// FilePart is document content (e.g. PDF); field semantics mirror
// ImagePart plus an optional display name.
//
// +k8s:deepcopy-gen=true
type FilePart struct {
	MIMEType string `json:"mimeType,omitempty" yaml:"mimeType,omitempty"`
	DataB64  string `json:"dataB64,omitempty" yaml:"dataB64,omitempty"`
	URL      string `json:"url,omitempty" yaml:"url,omitempty"`
	Name     string `json:"name,omitempty" yaml:"name,omitempty"`
	Wire     Wire   `json:"wire,omitempty" yaml:"wire,omitempty"`
}

// ToolCallPart is the model requesting a tool invocation. Arguments is
// the verbatim JSON arguments object (providers disagree on whether it
// rides as an object or a JSON-encoded string; codecs normalize to the
// object form and shadow the original).
//
// +k8s:deepcopy-gen=true
type ToolCallPart struct {
	ID        string          `json:"id,omitempty" yaml:"id,omitempty"`
	Name      string          `json:"name,omitempty" yaml:"name,omitempty"`
	Arguments json.RawMessage `json:"arguments,omitempty" yaml:"arguments,omitempty"`
	Wire      Wire            `json:"wire,omitempty" yaml:"wire,omitempty"`
}

// ToolCallResponsePart is a tool result returned to the model.
//
// +k8s:deepcopy-gen=true
type ToolCallResponsePart struct {
	ToolCallID string `json:"toolCallID,omitempty" yaml:"toolCallID,omitempty"`
	Parts      []Part `json:"parts,omitempty" yaml:"parts,omitempty"`
	IsError    bool   `json:"isError,omitempty" yaml:"isError,omitempty"`
	Wire       Wire   `json:"wire,omitempty" yaml:"wire,omitempty"`
}

// ReasoningPart is extended-thinking / reasoning content. Signature
// carries provider verification material (e.g. Anthropic's thinking
// signature) required to round-trip the block.
//
// +k8s:deepcopy-gen=true
type ReasoningPart struct {
	Text      string `json:"text,omitempty" yaml:"text,omitempty"`
	Signature string `json:"signature,omitempty" yaml:"signature,omitempty"`
	Wire      Wire   `json:"wire,omitempty" yaml:"wire,omitempty"`
}

// RefusalPart is an explicit model refusal (OpenAI refusal blocks).
//
// +k8s:deepcopy-gen=true
type RefusalPart struct {
	Text string `json:"text,omitempty" yaml:"text,omitempty"`
	Wire Wire   `json:"wire,omitempty" yaml:"wire,omitempty"`
}

// CitationPart is a source citation attached to generated content.
//
// +k8s:deepcopy-gen=true
type CitationPart struct {
	Title   string `json:"title,omitempty" yaml:"title,omitempty"`
	URL     string `json:"url,omitempty" yaml:"url,omitempty"`
	Snippet string `json:"snippet,omitempty" yaml:"snippet,omitempty"`
	Wire    Wire   `json:"wire,omitempty" yaml:"wire,omitempty"`
}

// Message is one conversation turn.
//
// +k8s:deepcopy-gen=true
type Message struct {
	Role  Role   `json:"role,omitempty" yaml:"role,omitempty"`
	Parts []Part `json:"parts,omitempty" yaml:"parts,omitempty"`
	Wire  Wire   `json:"wire,omitempty" yaml:"wire,omitempty"`
}

// ToolDefinition declares a tool the model may call. Parameters is the
// verbatim JSON Schema object — schema dialects ride through untouched.
//
// +k8s:deepcopy-gen=true
type ToolDefinition struct {
	Name        string          `json:"name,omitempty" yaml:"name,omitempty"`
	Description string          `json:"description,omitempty" yaml:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty" yaml:"parameters,omitempty"`
	Wire        Wire            `json:"wire,omitempty" yaml:"wire,omitempty"`
}

// ToolChoiceMode is how the model is steered toward tool use.
type ToolChoiceMode string

const (
	ToolChoiceAuto     ToolChoiceMode = "auto"
	ToolChoiceNone     ToolChoiceMode = "none"
	ToolChoiceRequired ToolChoiceMode = "required"
	// ToolChoiceNamed forces one specific tool; Name carries it.
	ToolChoiceNamed ToolChoiceMode = "named"
)

// ToolChoice steers tool selection for a request.
//
// +k8s:deepcopy-gen=true
type ToolChoice struct {
	Mode ToolChoiceMode `json:"mode,omitempty" yaml:"mode,omitempty"`
	Name string         `json:"name,omitempty" yaml:"name,omitempty"`
	Wire Wire           `json:"wire,omitempty" yaml:"wire,omitempty"`
}

// GenerationConfig holds the cross-provider sampling and length
// parameters. Numeric fields are json.Number so the original wire
// literal ("1.0" vs "1", exponent forms) survives a preserve-mode round
// trip; the empty string means the field was absent.
//
// +k8s:deepcopy-gen=true
type GenerationConfig struct {
	MaxTokens     json.Number `json:"maxTokens,omitempty" yaml:"maxTokens,omitempty"`
	Temperature   json.Number `json:"temperature,omitempty" yaml:"temperature,omitempty"`
	TopP          json.Number `json:"topP,omitempty" yaml:"topP,omitempty"`
	TopK          json.Number `json:"topK,omitempty" yaml:"topK,omitempty"`
	StopSequences []string    `json:"stopSequences,omitempty" yaml:"stopSequences,omitempty"`
	Wire          Wire        `json:"wire,omitempty" yaml:"wire,omitempty"`
}

// Request is the canonical decoded form of one LLM API request.
//
// +k8s:deepcopy-gen=true
type Request struct {
	// Provider is the canonical gen_ai.system name of the schema the
	// request was decoded from, stamped by DecodeRequest.
	Provider string `json:"provider,omitempty" yaml:"provider,omitempty"`

	// Operation is the gen_ai.operation.name value ("chat", ...).
	Operation string `json:"operation,omitempty" yaml:"operation,omitempty"`

	// Model is the requested model ID. For schemas that carry the model
	// in the URL rather than the body (Gemini), codecs keep Wire.Path
	// authoritative and synthesize the path on encode.
	Model string `json:"model,omitempty" yaml:"model,omitempty"`

	// System is the system/instructions content. Kept separate from
	// Messages because providers disagree on whether it is a message
	// role or a top-level field.
	System []Part `json:"system,omitempty" yaml:"system,omitempty"`

	Messages   []Message        `json:"messages,omitempty" yaml:"messages,omitempty"`
	Tools      []ToolDefinition `json:"tools,omitempty" yaml:"tools,omitempty"`
	ToolChoice *ToolChoice      `json:"toolChoice,omitempty" yaml:"toolChoice,omitempty"`
	Generation GenerationConfig `json:"generation,omitempty" yaml:"generation,omitempty"`
	Stream     bool             `json:"stream,omitempty" yaml:"stream,omitempty"`

	// Wire holds root-level bookkeeping: the full original body in Raw,
	// the original :path, and modeled request headers.
	Wire Wire `json:"wire,omitempty" yaml:"wire,omitempty"`
}

// Usage is provider-reported token accounting.
//
// +k8s:deepcopy-gen=true
type Usage struct {
	InputTokens  int64 `json:"inputTokens,omitempty" yaml:"inputTokens,omitempty"`
	OutputTokens int64 `json:"outputTokens,omitempty" yaml:"outputTokens,omitempty"`
	Wire         Wire  `json:"wire,omitempty" yaml:"wire,omitempty"`
}

// Choice is one generated completion.
//
// +k8s:deepcopy-gen=true
type Choice struct {
	Message      Message      `json:"message,omitempty" yaml:"message,omitempty"`
	FinishReason FinishReason `json:"finishReason,omitempty" yaml:"finishReason,omitempty"`
	// FinishReasonRaw is the provider's literal finish value when the
	// canonical mapping is lossy; empty when the mapping is bijective.
	FinishReasonRaw string `json:"finishReasonRaw,omitempty" yaml:"finishReasonRaw,omitempty"`
	Wire            Wire   `json:"wire,omitempty" yaml:"wire,omitempty"`
}

// Response is the canonical decoded form of one LLM API response.
//
// +k8s:deepcopy-gen=true
type Response struct {
	// Provider is the canonical gen_ai.system name of the schema the
	// response was decoded from.
	Provider string `json:"provider,omitempty" yaml:"provider,omitempty"`

	ID      string   `json:"id,omitempty" yaml:"id,omitempty"`
	Model   string   `json:"model,omitempty" yaml:"model,omitempty"`
	Choices []Choice `json:"choices,omitempty" yaml:"choices,omitempty"`
	Usage   Usage    `json:"usage,omitempty" yaml:"usage,omitempty"`

	Wire Wire `json:"wire,omitempty" yaml:"wire,omitempty"`
}
