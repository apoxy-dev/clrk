package llmcall

// StreamEventType discriminates the StreamEvent union — the canonical
// taxonomy for streamed responses. Phase 1 defines the types only;
// streaming codecs are a later phase (APO-743).
type StreamEventType string

const (
	// StreamEventStart opens a streamed response (response ID, model).
	StreamEventStart StreamEventType = "start"
	// StreamEventContentStart opens one content block at an index.
	StreamEventContentStart StreamEventType = "content_start"
	// StreamEventTextDelta appends text to an open block.
	StreamEventTextDelta StreamEventType = "text_delta"
	// StreamEventToolCallDelta appends tool-call argument bytes.
	StreamEventToolCallDelta StreamEventType = "tool_call_delta"
	// StreamEventReasoningDelta appends reasoning text.
	StreamEventReasoningDelta StreamEventType = "reasoning_delta"
	// StreamEventContentEnd closes the block at an index.
	StreamEventContentEnd StreamEventType = "content_end"
	// StreamEventUsage reports (cumulative) token usage.
	StreamEventUsage StreamEventType = "usage"
	// StreamEventFinish reports the finish reason.
	StreamEventFinish StreamEventType = "finish"
	// StreamEventError reports a mid-stream provider error.
	StreamEventError StreamEventType = "error"
	// StreamEventDone terminates the stream (OpenAI's [DONE]).
	StreamEventDone StreamEventType = "done"
)

// StreamEvent is one canonical streamed-response event — a tagged
// union in the same idiom as Part. StreamEventDone has no payload.
//
// +k8s:deepcopy-gen=true
type StreamEvent struct {
	Type StreamEventType `json:"type,omitempty" yaml:"type,omitempty"`

	Start          *StreamStart          `json:"start,omitempty" yaml:"start,omitempty"`
	ContentStart   *StreamContentStart   `json:"contentStart,omitempty" yaml:"contentStart,omitempty"`
	TextDelta      *StreamTextDelta      `json:"textDelta,omitempty" yaml:"textDelta,omitempty"`
	ToolCallDelta  *StreamToolCallDelta  `json:"toolCallDelta,omitempty" yaml:"toolCallDelta,omitempty"`
	ReasoningDelta *StreamReasoningDelta `json:"reasoningDelta,omitempty" yaml:"reasoningDelta,omitempty"`
	ContentEnd     *StreamContentEnd     `json:"contentEnd,omitempty" yaml:"contentEnd,omitempty"`
	Usage          *Usage                `json:"usage,omitempty" yaml:"usage,omitempty"`
	Finish         *StreamFinish         `json:"finish,omitempty" yaml:"finish,omitempty"`
	Error          *StreamError          `json:"error,omitempty" yaml:"error,omitempty"`

	Wire Wire `json:"wire,omitempty" yaml:"wire,omitempty"`
}

// StreamStart carries the stream-opening metadata.
//
// +k8s:deepcopy-gen=true
type StreamStart struct {
	ID    string `json:"id,omitempty" yaml:"id,omitempty"`
	Model string `json:"model,omitempty" yaml:"model,omitempty"`
}

// StreamContentStart opens the content block at Index; Part carries
// the block's initial typed shape (empty text, tool-call ID + name).
//
// +k8s:deepcopy-gen=true
type StreamContentStart struct {
	Index int  `json:"index,omitempty" yaml:"index,omitempty"`
	Part  Part `json:"part,omitempty" yaml:"part,omitempty"`
}

// StreamTextDelta appends text to the block at Index.
//
// +k8s:deepcopy-gen=true
type StreamTextDelta struct {
	Index int    `json:"index,omitempty" yaml:"index,omitempty"`
	Text  string `json:"text,omitempty" yaml:"text,omitempty"`
}

// StreamToolCallDelta appends argument bytes to a tool call. ID and
// Name are set when the provider repeats them per chunk (OpenAI);
// empty otherwise.
//
// +k8s:deepcopy-gen=true
type StreamToolCallDelta struct {
	Index          int    `json:"index,omitempty" yaml:"index,omitempty"`
	ID             string `json:"id,omitempty" yaml:"id,omitempty"`
	Name           string `json:"name,omitempty" yaml:"name,omitempty"`
	ArgumentsDelta string `json:"argumentsDelta,omitempty" yaml:"argumentsDelta,omitempty"`
}

// StreamReasoningDelta appends reasoning text to the block at Index.
//
// +k8s:deepcopy-gen=true
type StreamReasoningDelta struct {
	Index int    `json:"index,omitempty" yaml:"index,omitempty"`
	Text  string `json:"text,omitempty" yaml:"text,omitempty"`
}

// StreamContentEnd closes the content block at Index.
//
// +k8s:deepcopy-gen=true
type StreamContentEnd struct {
	Index int `json:"index,omitempty" yaml:"index,omitempty"`
}

// StreamFinish reports why generation stopped; FinishReasonRaw mirrors
// Choice.FinishReasonRaw.
//
// +k8s:deepcopy-gen=true
type StreamFinish struct {
	FinishReason    FinishReason `json:"finishReason,omitempty" yaml:"finishReason,omitempty"`
	FinishReasonRaw string       `json:"finishReasonRaw,omitempty" yaml:"finishReasonRaw,omitempty"`
}

// StreamError is a mid-stream provider error event.
//
// +k8s:deepcopy-gen=true
type StreamError struct {
	Code    string `json:"code,omitempty" yaml:"code,omitempty"`
	Message string `json:"message,omitempty" yaml:"message,omitempty"`
}
