package google

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/apoxy-dev/clrk/internal/extproc/jsonx"
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
)

// streamCodec implements llmcall.StreamCodec for Gemini's
// :streamGenerateContent SSE framing (alt=sse). Each SSE data payload
// is one GenerateContentResponse whose candidate parts are
// incremental — text arrives as deltas, functionCall parts arrive
// whole; only usageMetadata is cumulative. The non-SSE framing (a
// streamed JSON array) is not modeled: the request codec refuses
// :streamGenerateContent without alt=sse, so neither side of a
// translation ever carries it.
type streamCodec struct{}

func (streamCodec) NewStreamDecoder(req *llmcall.Request) llmcall.StreamDecoder {
	return &streamDecoder{}
}

func (streamCodec) NewStreamEncoder(req *llmcall.Request) llmcall.StreamEncoder {
	model := ""
	if req != nil {
		model = req.Model
	}
	return &streamEncoder{model: model, argBuf: map[int][]byte{}, names: map[int]string{}}
}

type gemPart struct {
	Text         *string         `json:"text"`
	Thought      bool            `json:"thought"`
	FunctionCall json.RawMessage `json:"functionCall"`
}

type gemChunk struct {
	ResponseID   string `json:"responseId"`
	ModelVersion string `json:"modelVersion"`
	Candidates   []struct {
		Content struct {
			Parts []gemPart `json:"parts"`
			Role  string    `json:"role"`
		} `json:"content"`
		FinishReason string `json:"finishReason"`
		Index        int    `json:"index"`
	} `json:"candidates"`
	UsageMetadata *struct {
		PromptTokenCount     int64 `json:"promptTokenCount"`
		CandidatesTokenCount int64 `json:"candidatesTokenCount"`
	} `json:"usageMetadata"`
	Error json.RawMessage `json:"error"`
}

// streamDecoder turns the Gemini SSE stream into canonical events.
// Text and thought parts get one lazily-opened block each; every
// functionCall part opens, fills, and closes its own block (Gemini
// emits them whole, never fragmented).
type streamDecoder struct {
	scan    llmcall.SSEScanner
	started bool
	done    bool
	next    int
	textIdx int // -1 when closed; zero value patched on first use
	reasIdx int
	init    bool
	open    []int
}

func (d *streamDecoder) Decode(chunk []byte, eos bool) ([]llmcall.StreamEvent, error) {
	if !d.init {
		d.init = true
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
			return out, &llmcall.MalformedError{Provider: "google_genai", Detail: "truncated SSE frame"}
		}
		out = append(out, d.finishStream()...)
	}
	return out, nil
}

func (d *streamDecoder) payload(data []byte) ([]llmcall.StreamEvent, error) {
	if d.done {
		return nil, nil
	}
	var ch gemChunk
	if err := jsonx.Unmarshal(data, &ch); err != nil {
		return nil, &llmcall.MalformedError{Provider: "google_genai", Detail: "stream chunk", Err: err}
	}
	if len(ch.Error) > 0 && !bytes.Equal(ch.Error, []byte("null")) {
		var e struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Status  string `json:"status"`
		}
		_ = jsonx.Unmarshal(ch.Error, &e)
		return []llmcall.StreamEvent{{
			Type:  llmcall.StreamEventError,
			Error: &llmcall.StreamError{Code: e.Status, Message: e.Message},
		}}, nil
	}

	var out []llmcall.StreamEvent
	if !d.started {
		d.started = true
		out = append(out, llmcall.StreamEvent{
			Type:  llmcall.StreamEventStart,
			Start: &llmcall.StreamStart{ID: ch.ResponseID, Model: ch.ModelVersion},
		})
	}
	for i := range ch.Candidates {
		cand := &ch.Candidates[i]
		if cand.Index != 0 {
			continue
		}
		for j := range cand.Content.Parts {
			out = d.part(out, &cand.Content.Parts[j])
		}
		if cand.FinishReason != "" {
			out = append(out, d.closeOpen()...)
			out = append(out, llmcall.StreamEvent{
				Type: llmcall.StreamEventFinish,
				Finish: &llmcall.StreamFinish{
					FinishReason:    canonicalFinish(cand.FinishReason),
					FinishReasonRaw: cand.FinishReason,
				},
			})
		}
	}
	if ch.UsageMetadata != nil {
		// Cumulative on this wire; emit every occurrence, last wins
		// downstream.
		out = append(out, llmcall.StreamEvent{
			Type: llmcall.StreamEventUsage,
			Usage: &llmcall.Usage{
				InputTokens:  ch.UsageMetadata.PromptTokenCount,
				OutputTokens: ch.UsageMetadata.CandidatesTokenCount,
			},
		})
	}
	return out, nil
}

func (d *streamDecoder) part(out []llmcall.StreamEvent, p *gemPart) []llmcall.StreamEvent {
	switch {
	case len(p.FunctionCall) > 0:
		var fc struct {
			Name string          `json:"name"`
			Args json.RawMessage `json:"args"`
		}
		if err := jsonx.Unmarshal(p.FunctionCall, &fc); err != nil {
			return out
		}
		idx := d.next
		d.next++
		args := "{}"
		if len(fc.Args) > 0 {
			if c, err := llmcall.CompactRaw(fc.Args); err == nil {
				args = string(c)
			}
		}
		// functionCall parts arrive whole: open, deliver, close.
		return append(out,
			llmcall.StreamEvent{
				Type: llmcall.StreamEventContentStart,
				ContentStart: &llmcall.StreamContentStart{
					Index: idx,
					Part: llmcall.Part{
						Type:     llmcall.PartTypeToolCall,
						ToolCall: &llmcall.ToolCallPart{Name: fc.Name},
					},
				},
			},
			llmcall.StreamEvent{
				Type:          llmcall.StreamEventToolCallDelta,
				ToolCallDelta: &llmcall.StreamToolCallDelta{Index: idx, Name: fc.Name, ArgumentsDelta: args},
			},
			llmcall.StreamEvent{
				Type:       llmcall.StreamEventContentEnd,
				ContentEnd: &llmcall.StreamContentEnd{Index: idx},
			},
		)
	case p.Text != nil && *p.Text != "" && p.Thought:
		if d.reasIdx < 0 {
			d.reasIdx = d.next
			d.next++
			d.open = append(d.open, d.reasIdx)
			out = append(out, llmcall.StreamEvent{
				Type: llmcall.StreamEventContentStart,
				ContentStart: &llmcall.StreamContentStart{
					Index: d.reasIdx,
					Part:  llmcall.Part{Type: llmcall.PartTypeReasoning, Reasoning: &llmcall.ReasoningPart{}},
				},
			})
		}
		return append(out, llmcall.StreamEvent{
			Type:           llmcall.StreamEventReasoningDelta,
			ReasoningDelta: &llmcall.StreamReasoningDelta{Index: d.reasIdx, Text: *p.Text},
		})
	case p.Text != nil && *p.Text != "":
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
			TextDelta: &llmcall.StreamTextDelta{Index: d.textIdx, Text: *p.Text},
		})
	}
	// Unmodeled streamed parts (inlineData, executableCode): skipped.
	return out
}

func (d *streamDecoder) closeOpen() []llmcall.StreamEvent {
	out := make([]llmcall.StreamEvent, 0, len(d.open))
	for _, idx := range d.open {
		out = append(out, llmcall.StreamEvent{
			Type:       llmcall.StreamEventContentEnd,
			ContentEnd: &llmcall.StreamContentEnd{Index: idx},
		})
	}
	d.open = d.open[:0]
	d.textIdx, d.reasIdx = -1, -1
	return out
}

// finishStream closes open blocks and emits done — Gemini has no
// stream terminator of its own, so EOS is the terminator.
func (d *streamDecoder) finishStream() []llmcall.StreamEvent {
	if d.done {
		return nil
	}
	d.done = true
	return append(d.closeOpen(), llmcall.StreamEvent{Type: llmcall.StreamEventDone})
}

// streamEncoder renders canonical events as a Gemini SSE stream:
// incremental delta frames, whole functionCall frames (arguments
// buffered until the block closes), and a terminal frame carrying
// finishReason + usageMetadata.
type streamEncoder struct {
	id     string
	model  string
	argBuf map[int][]byte
	names  map[int]string
	usage  *llmcall.Usage
	finish *llmcall.StreamFinish
	done   bool
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
		if ev.ContentStart.Part.Type == llmcall.PartTypeToolCall {
			idx := ev.ContentStart.Index
			e.argBuf[idx] = []byte{}
			if ev.ContentStart.Part.ToolCall != nil {
				e.names[idx] = ev.ContentStart.Part.ToolCall.Name
			}
		}
		return nil, nil
	case llmcall.StreamEventTextDelta:
		if ev.TextDelta.Text == "" {
			return nil, nil
		}
		return e.frame([]map[string]any{{"text": ev.TextDelta.Text}}, "", false)
	case llmcall.StreamEventReasoningDelta:
		if ev.ReasoningDelta.Text == "" {
			return nil, nil
		}
		return e.frame([]map[string]any{{"text": ev.ReasoningDelta.Text, "thought": true}}, "", false)
	case llmcall.StreamEventToolCallDelta:
		idx := ev.ToolCallDelta.Index
		if _, ok := e.argBuf[idx]; !ok {
			e.argBuf[idx] = []byte{}
		}
		e.argBuf[idx] = append(e.argBuf[idx], ev.ToolCallDelta.ArgumentsDelta...)
		if ev.ToolCallDelta.Name != "" {
			e.names[idx] = ev.ToolCallDelta.Name
		}
		return nil, nil
	case llmcall.StreamEventContentEnd:
		idx := ev.ContentEnd.Index
		buf, ok := e.argBuf[idx]
		if !ok {
			return nil, nil
		}
		delete(e.argBuf, idx)
		name := e.names[idx]
		delete(e.names, idx)
		args := json.RawMessage("{}")
		if len(bytes.TrimSpace(buf)) > 0 {
			c, err := llmcall.CompactRaw(json.RawMessage(buf))
			if err != nil {
				// The accumulated argument fragments never formed a
				// JSON object — nothing valid to hand the client.
				return nil, fmt.Errorf("google_genai: tool call %q arguments: %w", name, err)
			}
			args = c
		}
		return e.frame([]map[string]any{{
			"functionCall": map[string]any{"name": name, "args": args},
		}}, "", false)
	case llmcall.StreamEventUsage:
		u := *ev.Usage
		e.usage = &u
		return nil, nil
	case llmcall.StreamEventFinish:
		f := *ev.Finish
		e.finish = &f
		return nil, nil
	case llmcall.StreamEventError:
		code := 502
		status := "INTERNAL"
		payload := map[string]any{
			"error": map[string]any{"code": code, "message": ev.Error.Message, "status": status},
		}
		b, err := llmcall.MarshalCompact(payload)
		if err != nil {
			return nil, err
		}
		return llmcall.AppendSSE(nil, "", b), nil
	case llmcall.StreamEventDone:
		e.done = true
		fr := "STOP"
		if e.finish != nil {
			fr = googleFinishCanonical(e.finish.FinishReason)
		}
		return e.frame([]map[string]any{}, fr, true)
	}
	return nil, nil
}

// frame renders one GenerateContentResponse SSE frame. Every frame
// carries responseId/modelVersion when known (as real Gemini does);
// terminal frames add finishReason + usageMetadata.
func (e *streamEncoder) frame(parts []map[string]any, finishReason string, terminal bool) ([]byte, error) {
	cand := map[string]any{
		"content": map[string]any{"parts": parts, "role": "model"},
		"index":   0,
	}
	if finishReason != "" {
		cand["finishReason"] = finishReason
	}
	payload := map[string]any{"candidates": []any{cand}}
	if e.id != "" {
		payload["responseId"] = e.id
	}
	if e.model != "" {
		payload["modelVersion"] = e.model
	}
	if terminal {
		var in, outTok int64
		if e.usage != nil {
			in, outTok = e.usage.InputTokens, e.usage.OutputTokens
		}
		payload["usageMetadata"] = map[string]any{
			"promptTokenCount":     in,
			"candidatesTokenCount": outTok,
			"totalTokenCount":      in + outTok,
		}
	}
	b, err := llmcall.MarshalCompact(payload)
	if err != nil {
		return nil, err
	}
	return llmcall.AppendSSE(nil, "", b), nil
}
