package llmcall

import "strings"

// AssembledToolCall is one tool invocation reconstructed from a streamed
// response: the tool name and its fully-accumulated argument bytes
// (verbatim JSON, concatenated across the stream's argument deltas).
type AssembledToolCall struct {
	ID        string
	Name      string
	Arguments string
}

// AssembledResponse is the human-readable reconstruction of a streamed
// assistant message: the concatenated text and any tool calls, in the
// order their content blocks opened. Reasoning deltas are intentionally
// excluded — they are not the answer.
type AssembledResponse struct {
	Text      string
	ToolCalls []AssembledToolCall
}

// AssembleStreamedResponse reconstructs the assistant message from a
// captured streamed response body in provider p's wire framing. It
// drives p's StreamCodec, so every schema with a stream codec is
// covered by this one path rather than a per-provider reassembler.
//
// Best-effort by construction: the captured body may be truncated (the
// keep-last-N ring drops the head of a long stream) or cut mid-frame,
// so decode errors are swallowed and whatever decoded so far is
// returned. ok is false when p has no stream codec, body is empty, or
// nothing decodable was produced. The decoder is read-only here — it is
// built with a nil request because only the encoder consults the
// request (for the model fallback), which assembly does not use.
func AssembleStreamedResponse(p *Provider, body []byte) (AssembledResponse, bool) {
	if p == nil || p.StreamCodec == nil || len(body) == 0 {
		return AssembledResponse{}, false
	}
	events, _ := p.StreamCodec.NewStreamDecoder(nil).Decode(body, true)
	if len(events) == 0 {
		return AssembledResponse{}, false
	}

	var text strings.Builder
	// Tool calls accumulate per content-block index, kept in open order
	// so the rendered output matches the model's emission order.
	type acc struct {
		id, name string
		args     strings.Builder
	}
	byIndex := map[int]*acc{}
	var order []int
	get := func(i int) *acc {
		a := byIndex[i]
		if a == nil {
			a = &acc{}
			byIndex[i] = a
			order = append(order, i)
		}
		return a
	}

	for _, ev := range events {
		switch ev.Type {
		case StreamEventTextDelta:
			if ev.TextDelta != nil {
				text.WriteString(ev.TextDelta.Text)
			}
		case StreamEventContentStart:
			// Anthropic carries a tool call's id+name on the block-open
			// frame; OpenAI carries them on the first delta. Capture both.
			if cs := ev.ContentStart; cs != nil && cs.Part.ToolCall != nil {
				a := get(cs.Index)
				if cs.Part.ToolCall.ID != "" {
					a.id = cs.Part.ToolCall.ID
				}
				if cs.Part.ToolCall.Name != "" {
					a.name = cs.Part.ToolCall.Name
				}
				a.args.Write(cs.Part.ToolCall.Arguments)
			}
		case StreamEventToolCallDelta:
			if d := ev.ToolCallDelta; d != nil {
				a := get(d.Index)
				if d.ID != "" {
					a.id = d.ID
				}
				if d.Name != "" {
					a.name = d.Name
				}
				a.args.WriteString(d.ArgumentsDelta)
			}
		}
	}

	res := AssembledResponse{Text: text.String()}
	for _, i := range order {
		a := byIndex[i]
		if a.name == "" && a.args.Len() == 0 {
			continue
		}
		res.ToolCalls = append(res.ToolCalls, AssembledToolCall{
			ID:        a.id,
			Name:      a.name,
			Arguments: a.args.String(),
		})
	}
	if res.Text == "" && len(res.ToolCalls) == 0 {
		return AssembledResponse{}, false
	}
	return res, true
}

// Rendered flattens the assembled response into one readable string for
// telemetry display: the text, then each tool call as `name(arguments)`
// on its own line. ASCII-only, no invented markup beyond the call form.
func (a AssembledResponse) Rendered() string {
	var b strings.Builder
	b.WriteString(a.Text)
	for _, tc := range a.ToolCalls {
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(tc.Name)
		b.WriteByte('(')
		b.WriteString(tc.Arguments)
		b.WriteByte(')')
	}
	return b.String()
}
