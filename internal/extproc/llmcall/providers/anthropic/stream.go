package anthropic

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/apoxy-dev/clrk/internal/extproc/jsonx"
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
)

// streamCodec implements llmcall.StreamCodec for the Messages API
// event stream (message_start / content_block_* / message_delta /
// message_stop).
type streamCodec struct{}

func (streamCodec) NewStreamDecoder(req *llmcall.Request) llmcall.StreamDecoder {
	return &streamDecoder{open: map[int]llmcall.PartType{}}
}

func (streamCodec) NewStreamEncoder(req *llmcall.Request) llmcall.StreamEncoder {
	model := ""
	if req != nil {
		model = req.Model
	}
	return &streamEncoder{model: model, open: map[int]bool{}}
}

// streamDecoder turns the Messages event stream into canonical events.
// Anthropic's content_block indexes are already dense and 0-based, so
// they map straight onto IR block indexes. Dispatch is on the payload
// "type" member — event-name lines are informative duplicates (and
// some emitters omit them), matching the telemetry parser's approach.
type streamDecoder struct {
	scan        llmcall.SSEScanner
	inputTokens int64
	open        map[int]llmcall.PartType
	done        bool
}

type antStreamPayload struct {
	Type    string `json:"type"`
	Message struct {
		ID    string         `json:"id"`
		Model string         `json:"model"`
		Usage anthropicUsage `json:"usage"`
	} `json:"message"`
	Index        int             `json:"index"`
	ContentBlock json.RawMessage `json:"content_block"`
	Delta        json.RawMessage `json:"delta"`
	Usage        anthropicUsage  `json:"usage"`
	Error        struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

func (d *streamDecoder) Decode(chunk []byte, eos bool) ([]llmcall.StreamEvent, error) {
	var out []llmcall.StreamEvent
	for _, ev := range d.scan.Feed(chunk) {
		es, err := d.payload(ev.Data)
		if err != nil {
			return out, err
		}
		out = append(out, es...)
	}
	if eos {
		final, leftover := d.scan.Flush()
		if final != nil {
			es, err := d.payload(final.Data)
			if err != nil {
				return out, err
			}
			out = append(out, es...)
		}
		if len(bytes.TrimSpace(leftover)) > 0 {
			return out, &llmcall.MalformedError{Provider: "anthropic", Detail: "truncated SSE frame"}
		}
		out = append(out, d.finishStream()...)
	}
	return out, nil
}

func (d *streamDecoder) payload(data []byte) ([]llmcall.StreamEvent, error) {
	if d.done {
		return nil, nil
	}
	var p antStreamPayload
	if err := jsonx.Unmarshal(data, &p); err != nil {
		return nil, &llmcall.MalformedError{Provider: "anthropic", Detail: "stream event", Err: err}
	}
	switch p.Type {
	case "message_start":
		d.inputTokens = p.Message.Usage.InputTokens
		out := []llmcall.StreamEvent{
			{Type: llmcall.StreamEventStart, Start: &llmcall.StreamStart{ID: p.Message.ID, Model: p.Message.Model}},
		}
		if p.Message.Usage.InputTokens > 0 || p.Message.Usage.OutputTokens > 0 {
			out = append(out, llmcall.StreamEvent{Type: llmcall.StreamEventUsage, Usage: &llmcall.Usage{
				InputTokens:  p.Message.Usage.InputTokens,
				OutputTokens: p.Message.Usage.OutputTokens,
			}})
		}
		return out, nil
	case "content_block_start":
		return d.blockStart(p.Index, p.ContentBlock)
	case "content_block_delta":
		return d.blockDelta(p.Index, p.Delta)
	case "content_block_stop":
		if _, ok := d.open[p.Index]; !ok {
			return nil, nil
		}
		delete(d.open, p.Index)
		return []llmcall.StreamEvent{{
			Type:       llmcall.StreamEventContentEnd,
			ContentEnd: &llmcall.StreamContentEnd{Index: p.Index},
		}}, nil
	case "message_delta":
		var delta struct {
			StopReason string `json:"stop_reason"`
		}
		_ = jsonx.Unmarshal(p.Delta, &delta)
		// Newer API versions echo cumulative input_tokens here; prefer
		// it over the message_start seed (cache reads can grow it).
		if p.Usage.InputTokens > 0 {
			d.inputTokens = p.Usage.InputTokens
		}
		var out []llmcall.StreamEvent
		if delta.StopReason != "" {
			out = append(out, d.closeOpen()...)
			out = append(out, llmcall.StreamEvent{
				Type: llmcall.StreamEventFinish,
				Finish: &llmcall.StreamFinish{
					FinishReason:    canonicalFinish(delta.StopReason),
					FinishReasonRaw: delta.StopReason,
				},
			})
		}
		if p.Usage.OutputTokens > 0 || d.inputTokens > 0 {
			out = append(out, llmcall.StreamEvent{
				Type: llmcall.StreamEventUsage,
				Usage: &llmcall.Usage{
					InputTokens:  d.inputTokens,
					OutputTokens: p.Usage.OutputTokens,
				},
			})
		}
		return out, nil
	case "message_stop":
		return d.finishStream(), nil
	case "error":
		return []llmcall.StreamEvent{{
			Type:  llmcall.StreamEventError,
			Error: &llmcall.StreamError{Code: p.Error.Type, Message: p.Error.Message},
		}}, nil
	}
	// ping and unknown event types: ignored.
	return nil, nil
}

func (d *streamDecoder) blockStart(index int, raw json.RawMessage) ([]llmcall.StreamEvent, error) {
	var block struct {
		Type string `json:"type"`
		ID   string `json:"id"`
		Name string `json:"name"`
		Text string `json:"text"`
	}
	_ = jsonx.Unmarshal(raw, &block)
	var part llmcall.Part
	switch block.Type {
	case "text":
		part = llmcall.Part{Type: llmcall.PartTypeText, Text: &llmcall.TextPart{Text: block.Text}}
	case "tool_use":
		part = llmcall.Part{Type: llmcall.PartTypeToolCall, ToolCall: &llmcall.ToolCallPart{ID: block.ID, Name: block.Name}}
	case "thinking":
		part = llmcall.Part{Type: llmcall.PartTypeReasoning, Reasoning: &llmcall.ReasoningPart{}}
	default:
		// Unmodeled block types (redacted_thinking, server-side tool
		// blocks): skipped entirely, deltas for the index included.
		return nil, nil
	}
	d.open[index] = part.Type
	return []llmcall.StreamEvent{{
		Type:         llmcall.StreamEventContentStart,
		ContentStart: &llmcall.StreamContentStart{Index: index, Part: part},
	}}, nil
}

func (d *streamDecoder) blockDelta(index int, raw json.RawMessage) ([]llmcall.StreamEvent, error) {
	var delta struct {
		Type        string `json:"type"`
		Text        string `json:"text"`
		PartialJSON string `json:"partial_json"`
		Thinking    string `json:"thinking"`
	}
	_ = jsonx.Unmarshal(raw, &delta)

	var out []llmcall.StreamEvent
	// Deltas for an unopened index synthesize the open frame — some
	// emitters skip content_block_start.
	if _, ok := d.open[index]; !ok {
		var part llmcall.Part
		switch delta.Type {
		case "text_delta":
			part = llmcall.Part{Type: llmcall.PartTypeText, Text: &llmcall.TextPart{}}
		case "input_json_delta":
			part = llmcall.Part{Type: llmcall.PartTypeToolCall, ToolCall: &llmcall.ToolCallPart{}}
		case "thinking_delta":
			part = llmcall.Part{Type: llmcall.PartTypeReasoning, Reasoning: &llmcall.ReasoningPart{}}
		default:
			return nil, nil
		}
		d.open[index] = part.Type
		out = append(out, llmcall.StreamEvent{
			Type:         llmcall.StreamEventContentStart,
			ContentStart: &llmcall.StreamContentStart{Index: index, Part: part},
		})
	}
	switch delta.Type {
	case "text_delta":
		if delta.Text != "" {
			out = append(out, llmcall.StreamEvent{
				Type:      llmcall.StreamEventTextDelta,
				TextDelta: &llmcall.StreamTextDelta{Index: index, Text: delta.Text},
			})
		}
	case "input_json_delta":
		if delta.PartialJSON != "" {
			out = append(out, llmcall.StreamEvent{
				Type:          llmcall.StreamEventToolCallDelta,
				ToolCallDelta: &llmcall.StreamToolCallDelta{Index: index, ArgumentsDelta: delta.PartialJSON},
			})
		}
	case "thinking_delta":
		if delta.Thinking != "" {
			out = append(out, llmcall.StreamEvent{
				Type:           llmcall.StreamEventReasoningDelta,
				ReasoningDelta: &llmcall.StreamReasoningDelta{Index: index, Text: delta.Thinking},
			})
		}
		// signature_delta: dropped. Signatures are anthropic-private
		// verification material no other schema can carry or verify.
	}
	return out, nil
}

func (d *streamDecoder) closeOpen() []llmcall.StreamEvent {
	if len(d.open) == 0 {
		return nil
	}
	// Map order is random; close in index order for determinism.
	idxs := slices.Sorted(maps.Keys(d.open))
	out := make([]llmcall.StreamEvent, 0, len(idxs))
	for _, i := range idxs {
		out = append(out, llmcall.StreamEvent{
			Type:       llmcall.StreamEventContentEnd,
			ContentEnd: &llmcall.StreamContentEnd{Index: i},
		})
	}
	d.open = map[int]llmcall.PartType{}
	return out
}

func (d *streamDecoder) finishStream() []llmcall.StreamEvent {
	if d.done {
		return nil
	}
	d.done = true
	return append(d.closeOpen(), llmcall.StreamEvent{Type: llmcall.StreamEventDone})
}

// streamEncoder renders canonical events as a Messages API event
// stream: a complete message_start envelope, content_block frames,
// one message_delta carrying stop reason + usage, then message_stop.
type streamEncoder struct {
	id      string
	model   string
	started bool
	erred   bool
	done    bool
	open    map[int]bool
	nTools  int
	finish  *llmcall.StreamFinish
	usage   *llmcall.Usage
}

func (*streamEncoder) ContentType() string { return "text/event-stream" }

func (e *streamEncoder) Encode(events []llmcall.StreamEvent) ([]byte, error) {
	var out []byte
	for i := range events {
		b, err := e.event(&events[i])
		if err != nil {
			return nil, err
		}
		out = append(out, b...)
	}
	return out, nil
}

func (e *streamEncoder) event(ev *llmcall.StreamEvent) ([]byte, error) {
	if e.done || e.erred {
		// Real anthropic streams end after an error event; emit
		// nothing further.
		return nil, nil
	}
	switch ev.Type {
	case llmcall.StreamEventStart:
		if ev.Start.ID != "" {
			e.id = ev.Start.ID
		}
		if ev.Start.Model != "" {
			e.model = ev.Start.Model
		}
		return e.start()
	case llmcall.StreamEventContentStart:
		pre, err := e.start()
		if err != nil {
			return nil, err
		}
		b, err := e.blockStart(ev.ContentStart.Index, &ev.ContentStart.Part)
		return append(pre, b...), err
	case llmcall.StreamEventTextDelta:
		if ev.TextDelta.Text == "" {
			return nil, nil
		}
		return e.delta(ev.TextDelta.Index, map[string]any{"type": "text_delta", "text": ev.TextDelta.Text})
	case llmcall.StreamEventReasoningDelta:
		if ev.ReasoningDelta.Text == "" {
			return nil, nil
		}
		return e.delta(ev.ReasoningDelta.Index, map[string]any{"type": "thinking_delta", "thinking": ev.ReasoningDelta.Text})
	case llmcall.StreamEventToolCallDelta:
		if ev.ToolCallDelta.ArgumentsDelta == "" {
			return nil, nil
		}
		return e.delta(ev.ToolCallDelta.Index, map[string]any{"type": "input_json_delta", "partial_json": ev.ToolCallDelta.ArgumentsDelta})
	case llmcall.StreamEventContentEnd:
		if !e.open[ev.ContentEnd.Index] {
			return nil, nil
		}
		delete(e.open, ev.ContentEnd.Index)
		return e.frame("content_block_stop", map[string]any{
			"type":  "content_block_stop",
			"index": ev.ContentEnd.Index,
		})
	case llmcall.StreamEventUsage:
		u := *ev.Usage
		e.usage = &u
		return nil, nil
	case llmcall.StreamEventFinish:
		f := *ev.Finish
		e.finish = &f
		return nil, nil
	case llmcall.StreamEventError:
		pre, err := e.start()
		if err != nil {
			return nil, err
		}
		e.erredLatch()
		typ := ev.Error.Code
		if typ == "" {
			typ = "api_error"
		}
		b, err := e.frameAfterErr("error", map[string]any{
			"type": "error",
			"error": map[string]any{
				"type":    typ,
				"message": ev.Error.Message,
			},
		})
		return append(pre, b...), err
	case llmcall.StreamEventDone:
		return e.terminal()
	}
	return nil, nil
}

// erredLatch marks the stream failed AFTER the error frame renders.
func (e *streamEncoder) erredLatch() { e.erred = true }

// frameAfterErr renders one frame ignoring the erred latch — only the
// error frame itself uses it.
func (e *streamEncoder) frameAfterErr(name string, payload map[string]any) ([]byte, error) {
	b, err := llmcall.MarshalCompact(payload)
	if err != nil {
		return nil, err
	}
	return llmcall.AppendSSE(nil, name, b), nil
}

// start emits message_start once, with the complete message object the
// SDKs require (content array, null stop fields, zeroed usage — real
// numbers arrive on the terminal message_delta).
func (e *streamEncoder) start() ([]byte, error) {
	if e.started {
		return nil, nil
	}
	e.started = true
	if e.id == "" {
		e.id = "msg_clrk"
	}
	return e.frame("message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id":            e.id,
			"type":          "message",
			"role":          "assistant",
			"model":         e.model,
			"content":       []any{},
			"stop_reason":   nil,
			"stop_sequence": nil,
			"usage":         map[string]any{"input_tokens": 0, "output_tokens": 0},
		},
	})
}

func (e *streamEncoder) blockStart(index int, part *llmcall.Part) ([]byte, error) {
	var block map[string]any
	switch part.Type {
	case llmcall.PartTypeText:
		block = map[string]any{"type": "text", "text": ""}
	case llmcall.PartTypeToolCall:
		id, name := "", ""
		if part.ToolCall != nil {
			id, name = part.ToolCall.ID, part.ToolCall.Name
		}
		if id == "" {
			// ID-less sources (Gemini): synthesize, mirroring
			// EnsureToolCallIDs on the non-streaming path.
			e.nTools++
			id = fmt.Sprintf("call_%d", e.nTools)
		}
		block = map[string]any{"type": "tool_use", "id": id, "name": name, "input": map[string]any{}}
	case llmcall.PartTypeReasoning:
		block = map[string]any{"type": "thinking", "thinking": ""}
	default:
		return nil, nil
	}
	e.open[index] = true
	return e.frame("content_block_start", map[string]any{
		"type":          "content_block_start",
		"index":         index,
		"content_block": block,
	})
}

// delta renders one content_block_delta, synthesizing the open frame
// for an unopened index.
func (e *streamEncoder) delta(index int, d map[string]any) ([]byte, error) {
	pre, err := e.start()
	if err != nil {
		return nil, err
	}
	if !e.open[index] {
		var part llmcall.Part
		switch d["type"] {
		case "text_delta":
			part = llmcall.Part{Type: llmcall.PartTypeText, Text: &llmcall.TextPart{}}
		case "input_json_delta":
			part = llmcall.Part{Type: llmcall.PartTypeToolCall, ToolCall: &llmcall.ToolCallPart{}}
		case "thinking_delta":
			part = llmcall.Part{Type: llmcall.PartTypeReasoning, Reasoning: &llmcall.ReasoningPart{}}
		}
		b, err := e.blockStart(index, &part)
		if err != nil {
			return nil, err
		}
		pre = append(pre, b...)
	}
	b, err := e.frame("content_block_delta", map[string]any{
		"type":  "content_block_delta",
		"index": index,
		"delta": d,
	})
	return append(pre, b...), err
}

// terminal closes open blocks and emits message_delta + message_stop.
func (e *streamEncoder) terminal() ([]byte, error) {
	out, err := e.start()
	if err != nil {
		return nil, err
	}
	// Close any blocks the source never ended, in index order.
	for _, i := range slices.Sorted(maps.Keys(e.open)) {
		b, err := e.frame("content_block_stop", map[string]any{"type": "content_block_stop", "index": i})
		if err != nil {
			return nil, err
		}
		out = append(out, b...)
	}
	e.open = map[int]bool{}
	stop := "end_turn"
	if e.finish != nil {
		stop = anthropicFinishCanonical(e.finish.FinishReason)
	}
	usage := map[string]any{"output_tokens": int64(0)}
	if e.usage != nil {
		usage["output_tokens"] = e.usage.OutputTokens
		if e.usage.InputTokens > 0 {
			usage["input_tokens"] = e.usage.InputTokens
		}
	}
	b, err := e.frame("message_delta", map[string]any{
		"type":  "message_delta",
		"delta": map[string]any{"stop_reason": stop, "stop_sequence": nil},
		"usage": usage,
	})
	if err != nil {
		return nil, err
	}
	out = append(out, b...)
	b, err = e.frame("message_stop", map[string]any{"type": "message_stop"})
	if err != nil {
		return nil, err
	}
	e.done = true
	return append(out, b...), nil
}

func (e *streamEncoder) frame(name string, payload map[string]any) ([]byte, error) {
	b, err := llmcall.MarshalCompact(payload)
	if err != nil {
		return nil, err
	}
	return llmcall.AppendSSE(nil, name, b), nil
}
