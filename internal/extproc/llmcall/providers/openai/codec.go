package openai

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apoxy-dev/clrk/internal/extproc/jsonx"
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
)

// codec implements llmcall.Codec for the OpenAI Chat Completions API.
// Phase 1 scope: non-streaming POST /v1/chat/completions only; every
// other endpoint and response framing returns ErrUnsupported.
type codec struct{}

func malformed(detail string, err error) error {
	return &llmcall.MalformedError{Provider: "openai", Detail: detail, Err: err}
}

func (codec) DecodeRequest(in llmcall.RequestInput) (*llmcall.Request, error) {
	path := in.Path
	if i := strings.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}
	if path != "/v1/chat/completions" {
		return nil, fmt.Errorf("path %q: %w", in.Path, llmcall.ErrUnsupported)
	}

	req := &llmcall.Request{Provider: "openai", Operation: "chat"}
	req.Wire.Raw = json.RawMessage(in.Body)
	req.Wire.Path = in.Path

	w := &req.Wire
	err := llmcall.DecodeKnown(in.Body, w, map[string]func(json.RawMessage) error{
		"model": func(v json.RawMessage) error {
			s, err := llmcall.DecodeShadowString(v, w, "model")
			req.Model = s
			return err
		},
		"messages": func(v json.RawMessage) error {
			msgs, err := decodeMessages(v)
			req.Messages = msgs
			return err
		},
		"tools": func(v json.RawMessage) error {
			return decodeTools(v, req)
		},
		"tool_choice": func(v json.RawMessage) error {
			return decodeToolChoice(v, req)
		},
		"temperature": func(v json.RawMessage) error {
			n, err := llmcall.DecodeNumber(v)
			req.Generation.Temperature = n
			return err
		},
		"top_p": func(v json.RawMessage) error {
			n, err := llmcall.DecodeNumber(v)
			req.Generation.TopP = n
			return err
		},
		"max_tokens": func(v json.RawMessage) error {
			n, err := llmcall.DecodeNumber(v)
			req.Generation.MaxTokens = n
			return err
		},
		"max_completion_tokens": func(v json.RawMessage) error {
			n, err := llmcall.DecodeNumber(v)
			req.Generation.MaxTokens = n
			return err
		},
		"stop": func(v json.RawMessage) error {
			return decodeStop(v, req)
		},
		"stream": func(v json.RawMessage) error {
			return jsonx.Unmarshal(v, &req.Stream)
		},
	})
	if err != nil {
		return nil, malformed("request body", err)
	}
	return req, nil
}

func textPart(s string) llmcall.Part {
	return llmcall.Part{Type: llmcall.PartTypeText, Text: &llmcall.TextPart{Text: s}}
}

// decodeStop handles OpenAI's string-or-array stop parameter; the
// original shape is recorded as a hint, the verbatim value as a shadow.
func decodeStop(v json.RawMessage, req *llmcall.Request) error {
	w := &req.Wire
	if len(v) > 0 && v[0] == '"' {
		s, err := llmcall.DecodeShadowString(v, w, "stop")
		if err != nil {
			return err
		}
		w.SetHint("stop", "string")
		req.Generation.StopSequences = []string{s}
		return nil
	}
	ss, err := llmcall.DecodeShadowStringSlice(v, w, "stop")
	if err != nil {
		return err
	}
	req.Generation.StopSequences = ss
	return nil
}

// toolCallShadowVal is the comparable composite for whole-array
// tool_calls shadows. It is computed from the IR fields (with the
// compacted arguments object), so any post-decode mutation breaks the
// shadow and falls back to canonical encoding.
func toolCallShadowVal(parts []llmcall.Part) string {
	var b strings.Builder
	for i := range parts {
		if parts[i].Type != llmcall.PartTypeToolCall || parts[i].ToolCall == nil {
			continue
		}
		tc := parts[i].ToolCall
		b.WriteString(tc.ID)
		b.WriteByte(0)
		b.WriteString(tc.Name)
		b.WriteByte(0)
		b.Write(tc.Arguments)
		b.WriteByte(1)
	}
	return b.String()
}

// decodeToolCalls parses the wire tool_calls array (arguments ride as
// a JSON-encoded string) into ToolCallParts with object-form
// Arguments, shadowing the whole compacted array on mw.
func decodeToolCalls(v json.RawMessage, mw *llmcall.Wire) ([]llmcall.Part, error) {
	var elems []struct {
		ID       string `json:"id"`
		Type     string `json:"type"`
		Function struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"function"`
	}
	if err := jsonx.Unmarshal(v, &elems); err != nil {
		return nil, err
	}
	parts := make([]llmcall.Part, 0, len(elems))
	for _, el := range elems {
		tc := &llmcall.ToolCallPart{ID: el.ID, Name: el.Function.Name}
		if args := strings.TrimSpace(el.Function.Arguments); args != "" {
			if c, err := llmcall.CompactRaw(json.RawMessage(args)); err == nil {
				tc.Arguments = c
			}
			// Invalid argument JSON keeps Arguments nil; the array
			// shadow still re-emits the original bytes on preserve.
		}
		parts = append(parts, llmcall.Part{Type: llmcall.PartTypeToolCall, ToolCall: tc})
	}
	c, err := llmcall.CompactRaw(v)
	if err != nil {
		return nil, err
	}
	mw.RecordShadow("tool_calls", c, toolCallShadowVal(parts))
	return parts, nil
}

func decodeMessages(raw json.RawMessage) ([]llmcall.Message, error) {
	var elems []json.RawMessage
	if err := jsonx.Unmarshal(raw, &elems); err != nil {
		return nil, err
	}
	msgs := make([]llmcall.Message, 0, len(elems))
	for _, el := range elems {
		msg, err := decodeMessage(el)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func decodeMessage(el json.RawMessage) (llmcall.Message, error) {
	var msg llmcall.Message
	mw := &msg.Wire
	var contentParts []llmcall.Part
	var toolCallID string
	err := llmcall.DecodeKnown(el, mw, map[string]func(json.RawMessage) error{
		"role": func(v json.RawMessage) error {
			var s string
			if err := jsonx.Unmarshal(v, &s); err != nil {
				return err
			}
			// "developer" is the modern spelling of the system role;
			// the hint reproduces it on encode. OpenAI system messages
			// stay positional in Messages (RoleSystem) rather than
			// moving to Request.System — cross-schema encoders lift
			// them when the target carries system content top-level.
			if s == "developer" {
				mw.SetHint("role", "developer")
				msg.Role = llmcall.RoleSystem
				return nil
			}
			msg.Role = llmcall.Role(s)
			return nil
		},
		"content": func(v json.RawMessage) error {
			switch {
			case string(v) == "null":
				mw.SetHint("content", "null")
				return nil
			case len(v) > 0 && v[0] == '"':
				s, err := llmcall.DecodeShadowString(v, mw, "content")
				if err != nil {
					return err
				}
				mw.SetHint("content", "string")
				contentParts = []llmcall.Part{textPart(s)}
				return nil
			default:
				parts, err := decodeContentParts(v)
				contentParts = parts
				return err
			}
		},
		"tool_calls": func(v json.RawMessage) error {
			parts, err := decodeToolCalls(v, mw)
			if err != nil {
				return err
			}
			msg.Parts = append(msg.Parts, parts...)
			return nil
		},
		"tool_call_id": func(v json.RawMessage) error {
			s, err := llmcall.DecodeShadowString(v, mw, "tool_call_id")
			toolCallID = s
			return err
		},
		"refusal": func(v json.RawMessage) error {
			// Must be modeled, not an extra: the encode side owns the
			// key (it can carry a RefusalPart), and an emitter-claimed
			// key shadows any extra of the same name.
			if string(v) == "null" {
				mw.SetHint("refusal", "null")
				return nil
			}
			s, err := llmcall.DecodeShadowString(v, mw, "refusal")
			if err != nil {
				return err
			}
			msg.Parts = append(msg.Parts, llmcall.Part{
				Type:    llmcall.PartTypeRefusal,
				Refusal: &llmcall.RefusalPart{Text: s},
			})
			return nil
		},
	})
	if err != nil {
		return llmcall.Message{}, err
	}
	if msg.Role == llmcall.RoleTool {
		// A tool message IS a tool result; fold its content into one
		// ToolCallResponsePart so the IR shape matches providers that
		// carry results as content blocks.
		msg.Parts = append([]llmcall.Part{{
			Type: llmcall.PartTypeToolCallResponse,
			ToolCallResponse: &llmcall.ToolCallResponsePart{
				ToolCallID: toolCallID,
				Parts:      contentParts,
			},
		}}, msg.Parts...)
	} else {
		msg.Parts = append(contentParts, msg.Parts...)
	}
	return msg, nil
}

func decodeContentParts(raw json.RawMessage) ([]llmcall.Part, error) {
	var elems []json.RawMessage
	if err := jsonx.Unmarshal(raw, &elems); err != nil {
		return nil, err
	}
	parts := make([]llmcall.Part, 0, len(elems))
	for _, el := range elems {
		t, err := llmcall.DecodeTypedBlock(el, "type")
		if err != nil {
			return nil, err
		}
		switch t {
		case "text":
			p := llmcall.Part{Type: llmcall.PartTypeText, Text: &llmcall.TextPart{}}
			pw := &p.Text.Wire
			err := llmcall.DecodeKnown(el, pw, map[string]func(json.RawMessage) error{
				"type": func(json.RawMessage) error { return nil },
				"text": func(v json.RawMessage) error {
					s, err := llmcall.DecodeShadowString(v, pw, "text")
					p.Text.Text = s
					return err
				},
			})
			if err != nil {
				return nil, err
			}
			parts = append(parts, p)
		case "image_url":
			p := llmcall.Part{Type: llmcall.PartTypeImage, Image: &llmcall.ImagePart{}}
			pw := &p.Image.Wire
			err := llmcall.DecodeKnown(el, pw, map[string]func(json.RawMessage) error{
				"type": func(json.RawMessage) error { return nil },
				"image_url": func(v json.RawMessage) error {
					var iu struct {
						URL string `json:"url"`
					}
					if err := jsonx.Unmarshal(v, &iu); err != nil {
						return err
					}
					p.Image.URL = iu.URL
					c, err := llmcall.CompactRaw(v)
					if err != nil {
						return err
					}
					// Composite shadow over the nested envelope (it
					// can carry "detail"); mutated URLs re-encode
					// canonically as {"url": ...}.
					pw.RecordShadow("image_url", c, p.Image.URL)
					return nil
				},
			})
			if err != nil {
				return nil, err
			}
			parts = append(parts, p)
		default:
			p, err := llmcall.UnknownPart(el)
			if err != nil {
				return nil, err
			}
			parts = append(parts, p)
		}
	}
	return parts, nil
}

// toolDefShadowVal mirrors toolCallShadowVal for tool definitions.
func toolDefShadowVal(tools []llmcall.ToolDefinition) string {
	var b strings.Builder
	for i := range tools {
		b.WriteString(tools[i].Name)
		b.WriteByte(0)
		b.WriteString(tools[i].Description)
		b.WriteByte(0)
		b.Write(tools[i].Parameters)
		b.WriteByte(1)
	}
	return b.String()
}

func decodeTools(v json.RawMessage, req *llmcall.Request) error {
	var elems []struct {
		Type     string `json:"type"`
		Function struct {
			Name        string          `json:"name"`
			Description string          `json:"description"`
			Parameters  json.RawMessage `json:"parameters"`
		} `json:"function"`
	}
	if err := jsonx.Unmarshal(v, &elems); err != nil {
		return err
	}
	for _, el := range elems {
		td := llmcall.ToolDefinition{Name: el.Function.Name, Description: el.Function.Description}
		if len(el.Function.Parameters) > 0 {
			c, err := llmcall.CompactRaw(el.Function.Parameters)
			if err != nil {
				return err
			}
			td.Parameters = c
		}
		req.Tools = append(req.Tools, td)
	}
	c, err := llmcall.CompactRaw(v)
	if err != nil {
		return err
	}
	req.Wire.RecordShadow("tools", c, toolDefShadowVal(req.Tools))
	return nil
}

func toolChoiceShadowVal(tc *llmcall.ToolChoice) string {
	if tc == nil {
		return ""
	}
	return string(tc.Mode) + "\x00" + tc.Name
}

func decodeToolChoice(v json.RawMessage, req *llmcall.Request) error {
	tc := &llmcall.ToolChoice{}
	if len(v) > 0 && v[0] == '"' {
		var s string
		if err := jsonx.Unmarshal(v, &s); err != nil {
			return err
		}
		switch s {
		case "auto":
			tc.Mode = llmcall.ToolChoiceAuto
		case "none":
			tc.Mode = llmcall.ToolChoiceNone
		case "required":
			tc.Mode = llmcall.ToolChoiceRequired
		default:
			tc.Mode = llmcall.ToolChoiceMode(s)
		}
	} else {
		var obj struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		}
		if err := jsonx.Unmarshal(v, &obj); err != nil {
			return err
		}
		tc.Mode = llmcall.ToolChoiceNamed
		tc.Name = obj.Function.Name
	}
	c, err := llmcall.CompactRaw(v)
	if err != nil {
		return err
	}
	req.Wire.RecordShadow("tool_choice", c, toolChoiceShadowVal(tc))
	req.ToolChoice = tc
	return nil
}

func (c codec) EncodeRequest(req *llmcall.Request, opts llmcall.EncodeOptions) (*llmcall.EncodedRequest, error) {
	out := &llmcall.EncodedRequest{}
	w := &req.Wire
	mode := opts.Mode
	dropped := &out.DroppedExtras

	emit := map[string]llmcall.FieldEmitter{
		"model": func(e *llmcall.ObjectEncoder) {
			if req.Model == "" && !w.HasKey("model") {
				return
			}
			llmcall.EmitShadowString(e, w, "model", req.Model)
		},
		"messages": func(e *llmcall.ObjectEncoder) {
			raw, err := encodeMessages(req, mode, dropped)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("messages", raw)
		},
		"tools": func(e *llmcall.ObjectEncoder) {
			if len(req.Tools) == 0 {
				return
			}
			if raw, ok := w.ShadowRaw("tools", toolDefShadowVal(req.Tools)); ok && mode == llmcall.ModePreserve {
				e.Raw("tools", raw)
				return
			}
			raw, err := encodeTools(req.Tools)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("tools", raw)
		},
		"tool_choice": func(e *llmcall.ObjectEncoder) {
			if req.ToolChoice == nil {
				return
			}
			if raw, ok := w.ShadowRaw("tool_choice", toolChoiceShadowVal(req.ToolChoice)); ok && mode == llmcall.ModePreserve {
				e.Raw("tool_choice", raw)
				return
			}
			encodeToolChoice(e, req.ToolChoice)
		},
		"temperature": numberEmitter(w, "temperature", &req.Generation.Temperature),
		"top_p":       numberEmitter(w, "top_p", &req.Generation.TopP),
		"max_tokens": func(e *llmcall.ObjectEncoder) {
			if req.Generation.MaxTokens == "" || w.HasKey("max_completion_tokens") {
				return
			}
			e.Field("max_tokens", req.Generation.MaxTokens)
		},
		"max_completion_tokens": func(e *llmcall.ObjectEncoder) {
			if req.Generation.MaxTokens == "" || !w.HasKey("max_completion_tokens") {
				return
			}
			e.Field("max_completion_tokens", req.Generation.MaxTokens)
		},
		"stop": func(e *llmcall.ObjectEncoder) {
			if len(req.Generation.StopSequences) == 0 {
				return
			}
			if w.Hint("stop") == "string" && len(req.Generation.StopSequences) == 1 {
				llmcall.EmitShadowString(e, w, "stop", req.Generation.StopSequences[0])
				return
			}
			if raw, ok := w.ShadowRaw("stop", llmcall.StringSliceShadowVal(req.Generation.StopSequences)); ok {
				e.Raw("stop", raw)
				return
			}
			e.Field("stop", req.Generation.StopSequences)
		},
		"stream": func(e *llmcall.ObjectEncoder) {
			if !req.Stream && !w.HasKey("stream") {
				return
			}
			e.Field("stream", req.Stream)
		},
	}
	canonical := []string{"model", "messages", "tools", "tool_choice", "temperature", "top_p", "max_tokens", "max_completion_tokens", "stop", "stream"}

	fresh, err := llmcall.EncodeOrderedObject(w, mode, emit, canonical, dropped)
	if err != nil {
		return nil, err
	}
	if mode == llmcall.ModePreserve {
		out.Body, out.Exact = llmcall.EncodeRaw(fresh, w.Raw)
	} else {
		out.Body = fresh
	}
	out.Path = "/v1/chat/completions"
	if mode == llmcall.ModePreserve && w.Path != "" {
		out.Path = w.Path
	}
	out.SetHeaders = map[string]string{"content-type": "application/json"}
	return out, nil
}

func numberEmitter(w *llmcall.Wire, key string, n *json.Number) llmcall.FieldEmitter {
	return func(e *llmcall.ObjectEncoder) {
		if *n == "" {
			return
		}
		e.Field(key, *n)
	}
}

func splitMessageParts(msg *llmcall.Message) (content []llmcall.Part, toolCalls []llmcall.Part, toolResp *llmcall.ToolCallResponsePart) {
	for i := range msg.Parts {
		p := msg.Parts[i]
		switch p.Type {
		case llmcall.PartTypeToolCall:
			toolCalls = append(toolCalls, p)
		case llmcall.PartTypeToolCallResponse:
			if toolResp == nil {
				toolResp = p.ToolCallResponse
			}
		default:
			content = append(content, p)
		}
	}
	return content, toolCalls, toolResp
}

func encodeMessages(req *llmcall.Request, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	elems := make([]json.RawMessage, 0, len(req.Messages)+1)
	// System content decoded from a schema that carries it top-level
	// (anthropic system, Gemini systemInstruction) re-enters this wire
	// shape as a leading system message. OpenAI decodes keep system
	// turns positional in Messages and leave req.System empty, so
	// same-schema round trips emit nothing here.
	if len(req.System) > 0 {
		sys := llmcall.Message{Role: llmcall.RoleSystem, Parts: req.System}
		b, err := encodeMessage(&sys, mode, dropped)
		if err != nil {
			return nil, err
		}
		elems = append(elems, b)
	}
	for i := range req.Messages {
		b, err := encodeMessage(&req.Messages[i], mode, dropped)
		if err != nil {
			return nil, err
		}
		elems = append(elems, b)
	}
	return llmcall.EncodeArray(elems), nil
}

func encodeMessage(msg *llmcall.Message, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	mw := &msg.Wire
	content, toolCalls, toolResp := splitMessageParts(msg)
	if toolResp != nil && len(content) == 0 {
		content = toolResp.Parts
	}

	emit := map[string]llmcall.FieldEmitter{
		"role": func(e *llmcall.ObjectEncoder) {
			role := string(msg.Role)
			if mw.Hint("role") == "developer" {
				role = "developer"
			}
			// Tool results must ride a role:"tool" message on this wire
			// shape. Cross-schema sources carry them in user messages
			// (anthropic tool_result blocks); openai decodes already
			// have the tool role, so same-schema round trips are
			// unchanged.
			if toolResp != nil {
				role = string(llmcall.RoleTool)
			}
			e.Field("role", role)
		},
		"content": func(e *llmcall.ObjectEncoder) {
			switch mw.Hint("content") {
			case "null":
				e.Raw("content", json.RawMessage("null"))
				return
			case "string":
				llmcall.EmitShadowString(e, mw, "content", llmcall.CollectText(content))
				return
			}
			if len(content) == 0 && !mw.HasKey("content") {
				if len(toolCalls) > 0 {
					return
				}
				// A message must carry content on this wire shape;
				// fall through to an empty string.
				e.Field("content", "")
				return
			}
			if isAllText(content) && !mw.HasKey("content") {
				// Programmatic messages default to the string form —
				// the universally-accepted shape.
				e.Field("content", llmcall.CollectText(content))
				return
			}
			raw, err := encodeContentParts(content, mode, dropped)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("content", raw)
		},
		"tool_calls": func(e *llmcall.ObjectEncoder) {
			if len(toolCalls) == 0 {
				return
			}
			if raw, ok := mw.ShadowRaw("tool_calls", toolCallShadowVal(toolCalls)); ok && mode == llmcall.ModePreserve {
				e.Raw("tool_calls", raw)
				return
			}
			raw, err := encodeToolCalls(toolCalls)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("tool_calls", raw)
		},
		"tool_call_id": func(e *llmcall.ObjectEncoder) {
			if toolResp == nil {
				return
			}
			llmcall.EmitShadowString(e, mw, "tool_call_id", toolResp.ToolCallID)
		},
		"refusal": func(e *llmcall.ObjectEncoder) {
			if mw.Hint("refusal") == "null" {
				e.Raw("refusal", json.RawMessage("null"))
				return
			}
			for i := range msg.Parts {
				if msg.Parts[i].Type == llmcall.PartTypeRefusal && msg.Parts[i].Refusal != nil {
					llmcall.EmitShadowString(e, mw, "refusal", msg.Parts[i].Refusal.Text)
					return
				}
			}
		},
	}
	return llmcall.EncodeOrderedObject(mw, mode, emit, []string{"role", "content", "tool_call_id", "tool_calls"}, dropped)
}

func isAllText(parts []llmcall.Part) bool {
	for i := range parts {
		if parts[i].Type != llmcall.PartTypeText {
			return false
		}
	}
	return len(parts) > 0
}

func encodeContentParts(parts []llmcall.Part, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	elems := make([]json.RawMessage, 0, len(parts))
	for i := range parts {
		p := &parts[i]
		switch p.Type {
		case llmcall.PartTypeText:
			t := p.Text
			tw := &t.Wire
			b, err := llmcall.EncodeOrderedObject(tw, mode, map[string]llmcall.FieldEmitter{
				"type": func(e *llmcall.ObjectEncoder) { e.Field("type", "text") },
				"text": func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowString(e, tw, "text", t.Text) },
			}, []string{"type", "text"}, dropped)
			if err != nil {
				return nil, err
			}
			elems = append(elems, b)
		case llmcall.PartTypeImage:
			img := p.Image
			iw := &img.Wire
			b, err := llmcall.EncodeOrderedObject(iw, mode, map[string]llmcall.FieldEmitter{
				"type": func(e *llmcall.ObjectEncoder) { e.Field("type", "image_url") },
				"image_url": func(e *llmcall.ObjectEncoder) {
					if raw, ok := iw.ShadowRaw("image_url", img.URL); ok {
						e.Raw("image_url", raw)
						return
					}
					// Inline images from schemas that carry raw base64
					// (anthropic, Gemini) re-enter this wire shape as a
					// data URL — the form image_url accepts inline.
					url := img.URL
					if url == "" && img.DataB64 != "" {
						url = llmcall.DataURL(img.MIMEType, img.DataB64)
					}
					inner := llmcall.NewObjectEncoder()
					inner.Field("url", url)
					raw, err := inner.Bytes()
					if err != nil {
						e.Fail(err)
						return
					}
					e.Raw("image_url", raw)
				},
			}, []string{"type", "image_url"}, dropped)
			if err != nil {
				return nil, err
			}
			elems = append(elems, b)
		case llmcall.PartTypeUnknown:
			if mode == llmcall.ModePreserve && len(p.Wire.Raw) > 0 {
				elems = append(elems, p.Wire.Raw)
				continue
			}
			*dropped++
		default:
			*dropped++
		}
	}
	return llmcall.EncodeArray(elems), nil
}

func encodeToolCalls(parts []llmcall.Part) (json.RawMessage, error) {
	elems := make([]json.RawMessage, 0, len(parts))
	for i := range parts {
		tc := parts[i].ToolCall
		if tc == nil {
			continue
		}
		args := "{}"
		if len(tc.Arguments) > 0 {
			args = string(tc.Arguments)
		}
		fn := llmcall.NewObjectEncoder()
		fn.Field("name", tc.Name)
		fn.Field("arguments", args)
		fnRaw, err := fn.Bytes()
		if err != nil {
			return nil, err
		}
		el := llmcall.NewObjectEncoder()
		el.Field("id", tc.ID)
		el.Field("type", "function")
		el.Raw("function", fnRaw)
		b, err := el.Bytes()
		if err != nil {
			return nil, err
		}
		elems = append(elems, b)
	}
	return llmcall.EncodeArray(elems), nil
}

func encodeTools(tools []llmcall.ToolDefinition) (json.RawMessage, error) {
	elems := make([]json.RawMessage, 0, len(tools))
	for i := range tools {
		td := &tools[i]
		fn := llmcall.NewObjectEncoder()
		fn.Field("name", td.Name)
		if td.Description != "" {
			fn.Field("description", td.Description)
		}
		params := td.Parameters
		if len(params) == 0 {
			params = json.RawMessage(`{"type":"object"}`)
		}
		fn.Raw("parameters", params)
		fnRaw, err := fn.Bytes()
		if err != nil {
			return nil, err
		}
		el := llmcall.NewObjectEncoder()
		el.Field("type", "function")
		el.Raw("function", fnRaw)
		b, err := el.Bytes()
		if err != nil {
			return nil, err
		}
		elems = append(elems, b)
	}
	return llmcall.EncodeArray(elems), nil
}

func encodeToolChoice(e *llmcall.ObjectEncoder, tc *llmcall.ToolChoice) {
	switch tc.Mode {
	case llmcall.ToolChoiceAuto, llmcall.ToolChoiceNone, llmcall.ToolChoiceRequired:
		e.Field("tool_choice", string(tc.Mode))
	case llmcall.ToolChoiceNamed:
		fn := llmcall.NewObjectEncoder()
		fn.Field("name", tc.Name)
		fnRaw, err := fn.Bytes()
		if err != nil {
			e.Fail(err)
			return
		}
		obj := llmcall.NewObjectEncoder()
		obj.Field("type", "function")
		obj.Raw("function", fnRaw)
		raw, err := obj.Bytes()
		if err != nil {
			e.Fail(err)
			return
		}
		e.Raw("tool_choice", raw)
	default:
		e.Field("tool_choice", string(tc.Mode))
	}
}

func (codec) DecodeResponse(in llmcall.ResponseInput, req *llmcall.Request) (*llmcall.Response, error) {
	if in.Status != 0 && in.Status != 200 {
		return nil, fmt.Errorf("status %d: %w", in.Status, llmcall.ErrUnsupported)
	}
	if llmcall.HasContentTypePrefix(in.Headers, "text/event-stream") ||
		llmcall.HasContentTypePrefix(in.Headers, "application/x-ndjson") {
		return nil, fmt.Errorf("streaming response: %w", llmcall.ErrUnsupported)
	}

	resp := &llmcall.Response{Provider: "openai"}
	resp.Wire.Raw = json.RawMessage(in.Body)

	w := &resp.Wire
	err := llmcall.DecodeKnown(in.Body, w, map[string]func(json.RawMessage) error{
		"id": func(v json.RawMessage) error {
			s, err := llmcall.DecodeShadowString(v, w, "id")
			resp.ID = s
			return err
		},
		// Consumed so the emitter's constant owns the key in both
		// modes; this endpoint never emits another value (streamed
		// chunks are refused above).
		"object": func(json.RawMessage) error { return nil },
		"model": func(v json.RawMessage) error {
			s, err := llmcall.DecodeShadowString(v, w, "model")
			resp.Model = s
			return err
		},
		"choices": func(v json.RawMessage) error {
			choices, err := decodeChoices(v)
			resp.Choices = choices
			return err
		},
		"usage": func(v json.RawMessage) error {
			return decodeUsage(v, &resp.Usage)
		},
	})
	if err != nil {
		return nil, malformed("response body", err)
	}
	return resp, nil
}

func decodeChoices(raw json.RawMessage) ([]llmcall.Choice, error) {
	var elems []json.RawMessage
	if err := jsonx.Unmarshal(raw, &elems); err != nil {
		return nil, err
	}
	choices := make([]llmcall.Choice, 0, len(elems))
	for _, el := range elems {
		var ch llmcall.Choice
		cw := &ch.Wire
		err := llmcall.DecodeKnown(el, cw, map[string]func(json.RawMessage) error{
			// The index value is positional; the emitter re-derives it
			// from the slice position, so just claim the key.
			"index": func(json.RawMessage) error { return nil },
			"message": func(v json.RawMessage) error {
				msg, err := decodeMessage(v)
				ch.Message = msg
				return err
			},
			"finish_reason": func(v json.RawMessage) error {
				if string(v) == "null" {
					cw.SetHint("finish_reason", "null")
					return nil
				}
				var s string
				if err := jsonx.Unmarshal(v, &s); err != nil {
					return err
				}
				ch.FinishReasonRaw = s
				ch.FinishReason = canonicalFinish(s)
				return nil
			},
		})
		if err != nil {
			return nil, err
		}
		choices = append(choices, ch)
	}
	return choices, nil
}

func canonicalFinish(s string) llmcall.FinishReason {
	switch s {
	case "stop":
		return llmcall.FinishReasonStop
	case "length":
		return llmcall.FinishReasonLength
	case "tool_calls", "function_call":
		return llmcall.FinishReasonToolCalls
	case "content_filter":
		return llmcall.FinishReasonContentFilter
	default:
		return ""
	}
}

func openaiFinish(c llmcall.Choice) string {
	if c.FinishReasonRaw != "" {
		return c.FinishReasonRaw
	}
	switch c.FinishReason {
	case llmcall.FinishReasonLength:
		return "length"
	case llmcall.FinishReasonToolCalls:
		return "tool_calls"
	case llmcall.FinishReasonContentFilter:
		return "content_filter"
	default:
		return "stop"
	}
}

func decodeUsage(raw json.RawMessage, u *llmcall.Usage) error {
	uw := &u.Wire
	return llmcall.DecodeKnown(raw, uw, map[string]func(json.RawMessage) error{
		"prompt_tokens": func(v json.RawMessage) error {
			n, err := llmcall.DecodeShadowInt64(v, uw, "prompt_tokens")
			u.InputTokens = n
			return err
		},
		"completion_tokens": func(v json.RawMessage) error {
			n, err := llmcall.DecodeShadowInt64(v, uw, "completion_tokens")
			u.OutputTokens = n
			return err
		},
		"total_tokens": func(v json.RawMessage) error {
			// Modeled as a hint, not a field: preserve mode re-emits
			// the captured literal, strip mode synthesizes the sum.
			c, err := llmcall.CompactRaw(v)
			if err != nil {
				return err
			}
			uw.SetHint("total_tokens", string(c))
			return nil
		},
	})
}

func (codec) EncodeResponse(resp *llmcall.Response, opts llmcall.EncodeOptions) (*llmcall.EncodedResponse, error) {
	out := &llmcall.EncodedResponse{}
	mode := opts.Mode
	dropped := &out.DroppedExtras
	w := &resp.Wire

	emit := map[string]llmcall.FieldEmitter{
		"id":     func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowString(e, w, "id", resp.ID) },
		"object": func(e *llmcall.ObjectEncoder) { e.Field("object", "chat.completion") },
		"model":  func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowString(e, w, "model", resp.Model) },
		"choices": func(e *llmcall.ObjectEncoder) {
			raw, err := encodeChoices(resp.Choices, mode, dropped)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("choices", raw)
		},
		"usage": func(e *llmcall.ObjectEncoder) {
			raw, err := encodeUsage(&resp.Usage, mode, dropped)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("usage", raw)
		},
	}
	canonical := []string{"id", "object", "model", "choices", "usage"}

	fresh, err := llmcall.EncodeOrderedObject(w, mode, emit, canonical, dropped)
	if err != nil {
		return nil, err
	}
	if mode == llmcall.ModePreserve {
		out.Body, out.Exact = llmcall.EncodeRaw(fresh, w.Raw)
	} else {
		out.Body = fresh
	}
	out.SetHeaders = map[string]string{"content-type": "application/json"}
	return out, nil
}

func encodeChoices(choices []llmcall.Choice, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	elems := make([]json.RawMessage, 0, len(choices))
	for i := range choices {
		ch := &choices[i]
		cw := &ch.Wire
		idx := i
		emit := map[string]llmcall.FieldEmitter{
			"index": func(e *llmcall.ObjectEncoder) { e.Field("index", idx) },
			"message": func(e *llmcall.ObjectEncoder) {
				raw, err := encodeMessage(&ch.Message, mode, dropped)
				if err != nil {
					e.Fail(err)
					return
				}
				e.Raw("message", raw)
			},
			"finish_reason": func(e *llmcall.ObjectEncoder) {
				if cw.Hint("finish_reason") == "null" {
					e.Raw("finish_reason", json.RawMessage("null"))
					return
				}
				e.Field("finish_reason", openaiFinish(*ch))
			},
		}
		b, err := llmcall.EncodeOrderedObject(cw, mode, emit, []string{"index", "message", "finish_reason"}, dropped)
		if err != nil {
			return nil, err
		}
		elems = append(elems, b)
	}
	return llmcall.EncodeArray(elems), nil
}

func encodeUsage(u *llmcall.Usage, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	uw := &u.Wire
	emit := map[string]llmcall.FieldEmitter{
		"prompt_tokens":     func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowInt64(e, uw, "prompt_tokens", u.InputTokens) },
		"completion_tokens": func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowInt64(e, uw, "completion_tokens", u.OutputTokens) },
		"total_tokens": func(e *llmcall.ObjectEncoder) {
			if h := uw.Hint("total_tokens"); h != "" && mode == llmcall.ModePreserve {
				e.Raw("total_tokens", json.RawMessage(h))
				return
			}
			e.Field("total_tokens", u.InputTokens+u.OutputTokens)
		},
	}
	return llmcall.EncodeOrderedObject(uw, mode, emit, []string{"prompt_tokens", "completion_tokens", "total_tokens"}, dropped)
}
