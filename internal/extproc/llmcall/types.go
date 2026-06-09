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
type Part struct {
	Type PartType

	Text             *TextPart
	Image            *ImagePart
	Audio            *AudioPart
	File             *FilePart
	ToolCall         *ToolCallPart
	ToolCallResponse *ToolCallResponsePart
	Reasoning        *ReasoningPart
	Refusal          *RefusalPart
	Citation         *CitationPart

	Wire Wire
}

// TextPart is plain text content.
type TextPart struct {
	Text string
	Wire Wire
}

// ImagePart is image content, carried inline (base64) or by reference.
// Exactly one of DataB64/URL is set; MIMEType qualifies inline data.
type ImagePart struct {
	MIMEType string
	DataB64  string
	URL      string
	Wire     Wire
}

// AudioPart is audio content; field semantics mirror ImagePart.
type AudioPart struct {
	MIMEType string
	DataB64  string
	URL      string
	Wire     Wire
}

// FilePart is document content (e.g. PDF); field semantics mirror
// ImagePart plus an optional display name.
type FilePart struct {
	MIMEType string
	DataB64  string
	URL      string
	Name     string
	Wire     Wire
}

// ToolCallPart is the model requesting a tool invocation. Arguments is
// the verbatim JSON arguments object (providers disagree on whether it
// rides as an object or a JSON-encoded string; codecs normalize to the
// object form and shadow the original).
type ToolCallPart struct {
	ID        string
	Name      string
	Arguments json.RawMessage
	Wire      Wire
}

// ToolCallResponsePart is a tool result returned to the model.
type ToolCallResponsePart struct {
	ToolCallID string
	Parts      []Part
	IsError    bool
	Wire       Wire
}

// ReasoningPart is extended-thinking / reasoning content. Signature
// carries provider verification material (e.g. Anthropic's thinking
// signature) required to round-trip the block.
type ReasoningPart struct {
	Text      string
	Signature string
	Wire      Wire
}

// RefusalPart is an explicit model refusal (OpenAI refusal blocks).
type RefusalPart struct {
	Text string
	Wire Wire
}

// CitationPart is a source citation attached to generated content.
type CitationPart struct {
	Title   string
	URL     string
	Snippet string
	Wire    Wire
}

// Message is one conversation turn.
type Message struct {
	Role  Role
	Parts []Part
	Wire  Wire
}

// ToolDefinition declares a tool the model may call. Parameters is the
// verbatim JSON Schema object — schema dialects ride through untouched.
type ToolDefinition struct {
	Name        string
	Description string
	Parameters  json.RawMessage
	Wire        Wire
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
type ToolChoice struct {
	Mode ToolChoiceMode
	Name string
	Wire Wire
}

// GenerationConfig holds the cross-provider sampling and length
// parameters. Numeric fields are json.Number so the original wire
// literal ("1.0" vs "1", exponent forms) survives a preserve-mode round
// trip; the empty string means the field was absent.
type GenerationConfig struct {
	MaxTokens     json.Number
	Temperature   json.Number
	TopP          json.Number
	TopK          json.Number
	StopSequences []string
	Wire          Wire
}

// Request is the canonical decoded form of one LLM API request.
type Request struct {
	// Provider is the canonical gen_ai.system name of the schema the
	// request was decoded from, stamped by DecodeRequest.
	Provider string

	// Operation is the gen_ai.operation.name value ("chat", ...).
	Operation string

	// Model is the requested model ID. For schemas that carry the model
	// in the URL rather than the body (Gemini), codecs keep Wire.Path
	// authoritative and synthesize the path on encode.
	Model string

	// System is the system/instructions content. Kept separate from
	// Messages because providers disagree on whether it is a message
	// role or a top-level field.
	System []Part

	Messages   []Message
	Tools      []ToolDefinition
	ToolChoice *ToolChoice
	Generation GenerationConfig
	Stream     bool

	// Wire holds root-level bookkeeping: the full original body in Raw,
	// the original :path, and modeled request headers.
	Wire Wire
}

// Usage is provider-reported token accounting.
type Usage struct {
	InputTokens  int64
	OutputTokens int64
	Wire         Wire
}

// Choice is one generated completion.
type Choice struct {
	Message      Message
	FinishReason FinishReason
	// FinishReasonRaw is the provider's literal finish value when the
	// canonical mapping is lossy; empty when the mapping is bijective.
	FinishReasonRaw string
	Wire            Wire
}

// Response is the canonical decoded form of one LLM API response.
type Response struct {
	// Provider is the canonical gen_ai.system name of the schema the
	// response was decoded from.
	Provider string

	ID      string
	Model   string
	Choices []Choice
	Usage   Usage

	Wire Wire
}
