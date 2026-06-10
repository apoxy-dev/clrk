package openai

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apoxy-dev/clrk/internal/extproc/jsonx"
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
)

// chatStreamCodec implements llmcall.StreamCodec for the Chat
// Completions SSE chunk stream. Like the request codec, provider is
// the name stamped into errors — "openai" natively, the wrapping
// plugin's name when shared via NewChatStreamCodec.
type chatStreamCodec struct {
	provider string
}

// NewChatStreamCodec returns the Chat Completions stream codec — for
// plugins that speak OpenAI's stream framing on a different surface
// (azure_openai).
func NewChatStreamCodec(provider string) llmcall.StreamCodec {
	return chatStreamCodec{provider: provider}
}

func (c chatStreamCodec) NewStreamDecoder(req *llmcall.Request) llmcall.StreamDecoder {
	return &chatStreamDecoder{provider: c.provider}
}

func (c chatStreamCodec) NewStreamEncoder(req *llmcall.Request) llmcall.StreamEncoder {
	model := ""
	if req != nil {
		model = req.Model
	}
	return &chatStreamEncoder{model: model, slots: map[int]int{}}
}

// oaiToolDelta is one tool_calls fragment in a chunk delta. The first
// fragment for a wire index carries id + function.name; later
// fragments carry only argument bytes.
type oaiToolDelta struct {
	Index    int    `json:"index"`
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type oaiChunk struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Choices []struct {
		Index int `json:"index"`
		Delta struct {
			Role             string         `json:"role"`
			Content          *string        `json:"content"`
			ReasoningContent *string        `json:"reasoning_content"`
			Refusal          *string        `json:"refusal"`
			ToolCalls        []oaiToolDelta `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *openaiUsage    `json:"usage"`
	Error json.RawMessage `json:"error"`
}

// chatStreamDecoder turns the Chat Completions SSE chunk stream into
// canonical events. Block-slot assignment: content and
// reasoning_content each get one lazily-opened block; every wire
// tool_calls index gets its own.
type chatStreamDecoder struct {
	provider string
	scan     llmcall.SSEScanner
	started  bool
	done     bool
	next     int
	textIdx  int // IR index of the open text block; -1 when none
	reasIdx  int
	tools    map[int]int // wire tool_calls index -> IR block index
	open     []int       // open IR indexes, open order
}

func (d *chatStreamDecoder) Decode(chunk []byte, eos bool) ([]llmcall.StreamEvent, error) {
	if d.tools == nil {
		d.tools = map[int]int{}
		d.textIdx, d.reasIdx = -1, -1
	}
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
			return out, &llmcall.MalformedError{Provider: d.provider, Detail: "truncated SSE frame"}
		}
		out = append(out, d.finishStream()...)
	}
	return out, nil
}

func (d *chatStreamDecoder) payload(data []byte) ([]llmcall.StreamEvent, error) {
	if d.done {
		return nil, nil
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("[DONE]")) {
		return d.finishStream(), nil
	}
	var ch oaiChunk
	if err := jsonx.Unmarshal(data, &ch); err != nil {
		return nil, &llmcall.MalformedError{Provider: d.provider, Detail: "stream chunk", Err: err}
	}
	if len(ch.Error) > 0 && !bytes.Equal(ch.Error, []byte("null")) {
		var e struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		}
		_ = jsonx.Unmarshal(ch.Error, &e)
		code := e.Code
		if code == "" {
			code = e.Type
		}
		return []llmcall.StreamEvent{{
			Type:  llmcall.StreamEventError,
			Error: &llmcall.StreamError{Code: code, Message: e.Message},
		}}, nil
	}

	var out []llmcall.StreamEvent
	if !d.started {
		d.started = true
		out = append(out, llmcall.StreamEvent{
			Type:  llmcall.StreamEventStart,
			Start: &llmcall.StreamStart{ID: ch.ID, Model: ch.Model},
		})
	}
	for i := range ch.Choices {
		cho := &ch.Choices[i]
		if cho.Index != 0 {
			// n>1 cannot arise from translated requests ("n" is an
			// unmodeled extra, stripped before encode); ignore.
			continue
		}
		if cho.Delta.Content != nil && *cho.Delta.Content != "" {
			out = d.appendTextDelta(out, *cho.Delta.Content)
		}
		if cho.Delta.Refusal != nil && *cho.Delta.Refusal != "" {
			// Refusal text rides the text block: no canonical refusal
			// delta exists and dropping a model's refusal explanation
			// would hide it from the agent.
			out = d.appendTextDelta(out, *cho.Delta.Refusal)
		}
		if cho.Delta.ReasoningContent != nil && *cho.Delta.ReasoningContent != "" {
			if d.reasIdx < 0 {
				d.reasIdx = d.next
				d.next++
				d.open = append(d.open, d.reasIdx)
				out = append(out, llmcall.StreamEvent{
					Type: llmcall.StreamEventContentStart,
					ContentStart: &llmcall.StreamContentStart{
						Index: d.reasIdx,
						Part: llmcall.Part{
							Type:      llmcall.PartTypeReasoning,
							Reasoning: &llmcall.ReasoningPart{},
						},
					},
				})
			}
			out = append(out, llmcall.StreamEvent{
				Type:           llmcall.StreamEventReasoningDelta,
				ReasoningDelta: &llmcall.StreamReasoningDelta{Index: d.reasIdx, Text: *cho.Delta.ReasoningContent},
			})
		}
		for _, tc := range cho.Delta.ToolCalls {
			idx, ok := d.tools[tc.Index]
			if !ok {
				idx = d.next
				d.next++
				d.tools[tc.Index] = idx
				d.open = append(d.open, idx)
				out = append(out, llmcall.StreamEvent{
					Type: llmcall.StreamEventContentStart,
					ContentStart: &llmcall.StreamContentStart{
						Index: idx,
						Part: llmcall.Part{
							Type:     llmcall.PartTypeToolCall,
							ToolCall: &llmcall.ToolCallPart{ID: tc.ID, Name: tc.Function.Name},
						},
					},
				})
			}
			if tc.Function.Arguments != "" {
				out = append(out, llmcall.StreamEvent{
					Type: llmcall.StreamEventToolCallDelta,
					ToolCallDelta: &llmcall.StreamToolCallDelta{
						Index:          idx,
						ID:             tc.ID,
						Name:           tc.Function.Name,
						ArgumentsDelta: tc.Function.Arguments,
					},
				})
			}
		}
		if cho.FinishReason != nil && *cho.FinishReason != "" {
			out = append(out, d.closeOpen()...)
			out = append(out, llmcall.StreamEvent{
				Type: llmcall.StreamEventFinish,
				Finish: &llmcall.StreamFinish{
					FinishReason:    canonicalFinish(*cho.FinishReason),
					FinishReasonRaw: *cho.FinishReason,
				},
			})
		}
	}
	if ch.Usage != nil && (ch.Usage.PromptTokens > 0 || ch.Usage.CompletionTokens > 0) {
		out = append(out, llmcall.StreamEvent{
			Type: llmcall.StreamEventUsage,
			Usage: &llmcall.Usage{
				InputTokens:  ch.Usage.PromptTokens,
				OutputTokens: ch.Usage.CompletionTokens,
			},
		})
	}
	return out, nil
}

// appendTextDelta opens the text block lazily, then appends the delta.
func (d *chatStreamDecoder) appendTextDelta(out []llmcall.StreamEvent, text string) []llmcall.StreamEvent {
	if d.textIdx < 0 {
		d.textIdx = d.next
		d.next++
		d.open = append(d.open, d.textIdx)
		out = append(out, llmcall.StreamEvent{
			Type: llmcall.StreamEventContentStart,
			ContentStart: &llmcall.StreamContentStart{
				Index: d.textIdx,
				Part:  llmcall.Part{Type: llmcall.PartTypeText, Text: &llmcall.TextPart{}},
			},
		})
	}
	return append(out, llmcall.StreamEvent{
		Type:      llmcall.StreamEventTextDelta,
		TextDelta: &llmcall.StreamTextDelta{Index: d.textIdx, Text: text},
	})
}

func (d *chatStreamDecoder) closeOpen() []llmcall.StreamEvent {
	out := make([]llmcall.StreamEvent, 0, len(d.open))
	for _, idx := range d.open {
		out = append(out, llmcall.StreamEvent{
			Type:       llmcall.StreamEventContentEnd,
			ContentEnd: &llmcall.StreamContentEnd{Index: idx},
		})
	}
	d.open = d.open[:0]
	d.textIdx, d.reasIdx = -1, -1
	d.tools = map[int]int{}
	return out
}

// finishStream closes open blocks and emits the terminal done event —
// from [DONE], or synthesized at EOS when the upstream truncated.
func (d *chatStreamDecoder) finishStream() []llmcall.StreamEvent {
	if d.done {
		return nil
	}
	d.done = true
	return append(d.closeOpen(), llmcall.StreamEvent{Type: llmcall.StreamEventDone})
}

// chatStreamEncoder renders canonical events as a Chat Completions SSE
// chunk stream.
type chatStreamEncoder struct {
	id       string
	model    string
	created  int64
	sentRole bool
	slots    map[int]int // IR block index -> wire tool_calls index
	nextSlot int
	usage    *llmcall.Usage
	finish   *llmcall.StreamFinish
	done     bool
}

func (*chatStreamEncoder) ContentType() string { return "text/event-stream" }

// Wire shapes for encoded chunks. Pointer fields gate emission: a
// role-bearing first delta, argument-only tool fragments, the terminal
// usage chunk with an empty choices array.
type oaiOutDelta struct {
	Role             string       `json:"role,omitempty"`
	Content          *string      `json:"content,omitempty"`
	ReasoningContent *string      `json:"reasoning_content,omitempty"`
	ToolCalls        []oaiOutTool `json:"tool_calls,omitempty"`
}

type oaiOutTool struct {
	Index    int             `json:"index"`
	ID       string          `json:"id,omitempty"`
	Type     string          `json:"type,omitempty"`
	Function *oaiOutFunction `json:"function,omitempty"`
}

type oaiOutFunction struct {
	Name      string  `json:"name,omitempty"`
	Arguments *string `json:"arguments,omitempty"`
}

type oaiOutChoice struct {
	Index        int          `json:"index"`
	Delta        *oaiOutDelta `json:"delta,omitempty"`
	FinishReason *string      `json:"finish_reason,omitempty"`
}

type oaiOutChunk struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Created int64          `json:"created"`
	Model   string         `json:"model"`
	Choices []oaiOutChoice `json:"choices"`
	Usage   *oaiOutUsage   `json:"usage,omitempty"`
}

type oaiOutUsage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
}

func (e *chatStreamEncoder) Encode(events []llmcall.StreamEvent) ([]byte, error) {
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

func (e *chatStreamEncoder) event(ev *llmcall.StreamEvent) ([]byte, error) {
	if e.done {
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
		return nil, nil
	case llmcall.StreamEventContentStart:
		if ev.ContentStart.Part.Type != llmcall.PartTypeToolCall {
			// Text/reasoning blocks have no open frame on this wire.
			return nil, nil
		}
		slot := e.nextSlot
		e.nextSlot++
		e.slots[ev.ContentStart.Index] = slot
		tc := ev.ContentStart.Part.ToolCall
		id := tc.ID
		if id == "" {
			// ID-less sources (Gemini): synthesize, mirroring
			// EnsureToolCallIDs on the non-streaming path.
			id = fmt.Sprintf("call_%d", slot+1)
		}
		empty := ""
		return e.chunk(&oaiOutDelta{ToolCalls: []oaiOutTool{{
			Index:    slot,
			ID:       id,
			Type:     "function",
			Function: &oaiOutFunction{Name: tc.Name, Arguments: &empty},
		}}}, nil), nil
	case llmcall.StreamEventTextDelta:
		if ev.TextDelta.Text == "" {
			return nil, nil
		}
		return e.chunk(&oaiOutDelta{Content: &ev.TextDelta.Text}, nil), nil
	case llmcall.StreamEventReasoningDelta:
		if ev.ReasoningDelta.Text == "" {
			return nil, nil
		}
		return e.chunk(&oaiOutDelta{ReasoningContent: &ev.ReasoningDelta.Text}, nil), nil
	case llmcall.StreamEventToolCallDelta:
		slot, ok := e.slots[ev.ToolCallDelta.Index]
		if !ok {
			// Delta for a block whose content_start never arrived;
			// open it now.
			open, err := e.event(&llmcall.StreamEvent{
				Type: llmcall.StreamEventContentStart,
				ContentStart: &llmcall.StreamContentStart{
					Index: ev.ToolCallDelta.Index,
					Part: llmcall.Part{
						Type:     llmcall.PartTypeToolCall,
						ToolCall: &llmcall.ToolCallPart{ID: ev.ToolCallDelta.ID, Name: ev.ToolCallDelta.Name},
					},
				},
			})
			if err != nil {
				return nil, err
			}
			frag, err := e.event(ev)
			return append(open, frag...), err
		}
		if ev.ToolCallDelta.ArgumentsDelta == "" {
			return nil, nil
		}
		return e.chunk(&oaiOutDelta{ToolCalls: []oaiOutTool{{
			Index:    slot,
			Function: &oaiOutFunction{Arguments: &ev.ToolCallDelta.ArgumentsDelta},
		}}}, nil), nil
	case llmcall.StreamEventContentEnd:
		return nil, nil
	case llmcall.StreamEventUsage:
		u := *ev.Usage
		e.usage = &u
		return nil, nil
	case llmcall.StreamEventFinish:
		f := *ev.Finish
		e.finish = &f
		fr := openaiFinishCanonical(f.FinishReason)
		return e.chunk(&oaiOutDelta{}, &fr), nil
	case llmcall.StreamEventError:
		msg := ev.Error.Message
		if msg == "" {
			msg = "upstream stream error"
		}
		typ := ev.Error.Code
		if typ == "" {
			typ = "server_error"
		}
		payload := []byte(`{"error":{"message":` + llmcall.JSONString(msg) + `,"type":` + llmcall.JSONString(typ) + `}}`)
		return llmcall.AppendSSE(nil, "", payload), nil
	case llmcall.StreamEventDone:
		var out []byte
		if e.usage != nil {
			b, err := llmcall.MarshalCompact(&oaiOutChunk{
				ID:      e.chunkID(),
				Object:  "chat.completion.chunk",
				Created: e.createdAt(),
				Model:   e.model,
				Choices: []oaiOutChoice{},
				Usage: &oaiOutUsage{
					PromptTokens:     e.usage.InputTokens,
					CompletionTokens: e.usage.OutputTokens,
					TotalTokens:      e.usage.InputTokens + e.usage.OutputTokens,
				},
			})
			if err != nil {
				return nil, err
			}
			out = llmcall.AppendSSE(out, "", b)
		}
		out = llmcall.AppendSSE(out, "", []byte("[DONE]"))
		e.done = true
		return out, nil
	}
	return nil, nil
}

// chunk renders one delta-bearing chunk. The first delta chunk carries
// role "assistant", as real upstreams do.
func (e *chatStreamEncoder) chunk(delta *oaiOutDelta, finish *string) []byte {
	if !e.sentRole {
		e.sentRole = true
		delta.Role = "assistant"
	}
	b, err := llmcall.MarshalCompact(&oaiOutChunk{
		ID:      e.chunkID(),
		Object:  "chat.completion.chunk",
		Created: e.createdAt(),
		Model:   e.model,
		Choices: []oaiOutChoice{{Index: 0, Delta: delta, FinishReason: finish}},
	})
	if err != nil {
		// Marshal of these literal structs cannot fail.
		return nil
	}
	return llmcall.AppendSSE(nil, "", b)
}

func (e *chatStreamEncoder) chunkID() string {
	if e.id == "" {
		e.id = "chatcmpl-clrk"
	}
	return e.id
}

func (e *chatStreamEncoder) createdAt() int64 {
	if e.created == 0 {
		e.created = time.Now().Unix()
	}
	return e.created
}
