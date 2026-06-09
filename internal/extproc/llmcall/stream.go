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
type StreamEvent struct {
	Type StreamEventType

	Start          *StreamStart
	ContentStart   *StreamContentStart
	TextDelta      *StreamTextDelta
	ToolCallDelta  *StreamToolCallDelta
	ReasoningDelta *StreamReasoningDelta
	ContentEnd     *StreamContentEnd
	Usage          *Usage
	Finish         *StreamFinish
	Error          *StreamError

	Wire Wire
}

// StreamStart carries the stream-opening metadata.
type StreamStart struct {
	ID    string
	Model string
}

// StreamContentStart opens the content block at Index; Part carries
// the block's initial typed shape (empty text, tool-call ID + name).
type StreamContentStart struct {
	Index int
	Part  Part
}

// StreamTextDelta appends text to the block at Index.
type StreamTextDelta struct {
	Index int
	Text  string
}

// StreamToolCallDelta appends argument bytes to a tool call. ID and
// Name are set when the provider repeats them per chunk (OpenAI);
// empty otherwise.
type StreamToolCallDelta struct {
	Index          int
	ID             string
	Name           string
	ArgumentsDelta string
}

// StreamReasoningDelta appends reasoning text to the block at Index.
type StreamReasoningDelta struct {
	Index int
	Text  string
}

// StreamContentEnd closes the content block at Index.
type StreamContentEnd struct {
	Index int
}

// StreamFinish reports why generation stopped; FinishReasonRaw mirrors
// Choice.FinishReasonRaw.
type StreamFinish struct {
	FinishReason    FinishReason
	FinishReasonRaw string
}

// StreamError is a mid-stream provider error event.
type StreamError struct {
	Code    string
	Message string
}
