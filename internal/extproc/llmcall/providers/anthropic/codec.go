package anthropic

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
)

// defaultVersion is the anthropic-version header value stamped on
// requests encoded without a captured original (cross-schema).
const defaultVersion = "2023-06-01"

// defaultMaxTokens is synthesized by strip-mode EncodeRequest when the
// IR carries no max_tokens (cross-schema sources may omit it; the
// Messages API requires it). 4096 is the long-standing community
// default (LiteLLM and friends) — high enough not to clip ordinary
// completions, low enough to be safe on every Claude model.
const defaultMaxTokens = "4096"

// codec implements llmcall.Codec for the Anthropic Messages API.
// Phase 1 scope: non-streaming POST /v1/messages chat only; every
// other endpoint and response framing returns ErrUnsupported.
type codec struct{}

func malformed(detail string, err error) error {
	return &llmcall.MalformedError{Provider: "anthropic", Detail: detail, Err: err}
}

func (codec) DecodeRequest(in llmcall.RequestInput) (*llmcall.Request, error) {
	path := in.Path
	if i := strings.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}
	if path != "/v1/messages" {
		return nil, fmt.Errorf("path %q: %w", in.Path, llmcall.ErrUnsupported)
	}

	req := &llmcall.Request{Provider: "anthropic", Operation: "chat"}
	req.Wire.Raw = json.RawMessage(in.Body)
	req.Wire.Path = in.Path
	if v := in.Headers["anthropic-version"]; v != "" {
		req.Wire.Headers = map[string]string{"anthropic-version": v}
	}

	w := &req.Wire
	err := llmcall.DecodeKnown(in.Body, w, map[string]func(json.RawMessage) error{
		"model": func(v json.RawMessage) error {
			s, err := llmcall.DecodeShadowString(v, w, "model")
			req.Model = s
			return err
		},
		"max_tokens": func(v json.RawMessage) error {
			n, err := llmcall.DecodeNumber(v)
			req.Generation.MaxTokens = n
			return err
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
		"top_k": func(v json.RawMessage) error {
			n, err := llmcall.DecodeNumber(v)
			req.Generation.TopK = n
			return err
		},
		"stop_sequences": func(v json.RawMessage) error {
			ss, err := llmcall.DecodeShadowStringSlice(v, w, "stop_sequences")
			req.Generation.StopSequences = ss
			return err
		},
		"stream": func(v json.RawMessage) error {
			return json.Unmarshal(v, &req.Stream)
		},
		"system": func(v json.RawMessage) error {
			// System is a top-level string or an array of content
			// blocks; the string form is recorded as a hint so encode
			// reproduces the original shape.
			if len(v) > 0 && v[0] == '"' {
				s, err := llmcall.DecodeShadowString(v, w, "system")
				if err != nil {
					return err
				}
				w.SetHint("system", "string")
				req.System = []llmcall.Part{textPart(s)}
				return nil
			}
			parts, err := decodeParts(v)
			req.System = parts
			return err
		},
		"messages": func(v json.RawMessage) error {
			msgs, err := decodeMessages(v)
			req.Messages = msgs
			return err
		},
		"tools": func(v json.RawMessage) error {
			tools, err := decodeTools(v)
			req.Tools = tools
			return err
		},
		"tool_choice": func(v json.RawMessage) error {
			tc, err := decodeToolChoice(v)
			req.ToolChoice = tc
			return err
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

func decodeMessages(raw json.RawMessage) ([]llmcall.Message, error) {
	var elems []json.RawMessage
	if err := json.Unmarshal(raw, &elems); err != nil {
		return nil, err
	}
	msgs := make([]llmcall.Message, 0, len(elems))
	for _, el := range elems {
		var msg llmcall.Message
		mw := &msg.Wire
		err := llmcall.DecodeKnown(el, mw, map[string]func(json.RawMessage) error{
			"role": func(v json.RawMessage) error {
				var s string
				if err := json.Unmarshal(v, &s); err != nil {
					return err
				}
				msg.Role = llmcall.Role(s)
				return nil
			},
			"content": func(v json.RawMessage) error {
				if len(v) > 0 && v[0] == '"' {
					s, err := llmcall.DecodeShadowString(v, mw, "content")
					if err != nil {
						return err
					}
					mw.SetHint("content", "string")
					msg.Parts = []llmcall.Part{textPart(s)}
					return nil
				}
				parts, err := decodeParts(v)
				msg.Parts = parts
				return err
			},
		})
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func decodeParts(raw json.RawMessage) ([]llmcall.Part, error) {
	var elems []json.RawMessage
	if err := json.Unmarshal(raw, &elems); err != nil {
		return nil, err
	}
	parts := make([]llmcall.Part, 0, len(elems))
	for _, el := range elems {
		p, err := decodeBlock(el)
		if err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	return parts, nil
}

// consumeType is the handler for a block's "type" discriminator: the
// value was already read by DecodeTypedBlock, so just claim the key in
// KeyOrder (the emitter re-emits the constant).
func consumeType(json.RawMessage) error { return nil }

func decodeBlock(raw json.RawMessage) (llmcall.Part, error) {
	t, err := llmcall.DecodeTypedBlock(raw, "type")
	if err != nil {
		return llmcall.Part{}, err
	}
	switch t {
	case "text":
		p := llmcall.Part{Type: llmcall.PartTypeText, Text: &llmcall.TextPart{}}
		pw := &p.Text.Wire
		err := llmcall.DecodeKnown(raw, pw, map[string]func(json.RawMessage) error{
			"type": consumeType,
			"text": func(v json.RawMessage) error {
				s, err := llmcall.DecodeShadowString(v, pw, "text")
				p.Text.Text = s
				return err
			},
		})
		return p, err
	case "tool_use":
		p := llmcall.Part{Type: llmcall.PartTypeToolCall, ToolCall: &llmcall.ToolCallPart{}}
		pw := &p.ToolCall.Wire
		err := llmcall.DecodeKnown(raw, pw, map[string]func(json.RawMessage) error{
			"type": consumeType,
			"id": func(v json.RawMessage) error {
				s, err := llmcall.DecodeShadowString(v, pw, "id")
				p.ToolCall.ID = s
				return err
			},
			"name": func(v json.RawMessage) error {
				s, err := llmcall.DecodeShadowString(v, pw, "name")
				p.ToolCall.Name = s
				return err
			},
			"input": func(v json.RawMessage) error {
				c, err := llmcall.CompactRaw(v)
				p.ToolCall.Arguments = c
				return err
			},
		})
		return p, err
	case "tool_result":
		p := llmcall.Part{Type: llmcall.PartTypeToolCallResponse, ToolCallResponse: &llmcall.ToolCallResponsePart{}}
		pw := &p.ToolCallResponse.Wire
		err := llmcall.DecodeKnown(raw, pw, map[string]func(json.RawMessage) error{
			"type": consumeType,
			"tool_use_id": func(v json.RawMessage) error {
				s, err := llmcall.DecodeShadowString(v, pw, "tool_use_id")
				p.ToolCallResponse.ToolCallID = s
				return err
			},
			"is_error": func(v json.RawMessage) error {
				return json.Unmarshal(v, &p.ToolCallResponse.IsError)
			},
			"content": func(v json.RawMessage) error {
				if len(v) > 0 && v[0] == '"' {
					s, err := llmcall.DecodeShadowString(v, pw, "content")
					if err != nil {
						return err
					}
					pw.SetHint("content", "string")
					p.ToolCallResponse.Parts = []llmcall.Part{textPart(s)}
					return nil
				}
				parts, err := decodeParts(v)
				p.ToolCallResponse.Parts = parts
				return err
			},
		})
		return p, err
	case "thinking":
		p := llmcall.Part{Type: llmcall.PartTypeReasoning, Reasoning: &llmcall.ReasoningPart{}}
		pw := &p.Reasoning.Wire
		err := llmcall.DecodeKnown(raw, pw, map[string]func(json.RawMessage) error{
			"type": consumeType,
			"thinking": func(v json.RawMessage) error {
				s, err := llmcall.DecodeShadowString(v, pw, "thinking")
				p.Reasoning.Text = s
				return err
			},
			"signature": func(v json.RawMessage) error {
				s, err := llmcall.DecodeShadowString(v, pw, "signature")
				p.Reasoning.Signature = s
				return err
			},
		})
		return p, err
	case "image":
		p := llmcall.Part{Type: llmcall.PartTypeImage, Image: &llmcall.ImagePart{}}
		pw := &p.Image.Wire
		err := llmcall.DecodeKnown(raw, pw, map[string]func(json.RawMessage) error{
			"type": consumeType,
			"source": func(v json.RawMessage) error {
				if err := decodeImageSource(v, p.Image); err != nil {
					return err
				}
				// The nested source object rides a composite shadow
				// rather than per-field bookkeeping: opaque base64
				// payloads are re-encoded verbatim while unmutated and
				// canonically once any field changes.
				c, err := llmcall.CompactRaw(v)
				if err != nil {
					return err
				}
				pw.RecordShadow("source", c, imageShadowVal(p.Image))
				return nil
			},
		})
		return p, err
	default:
		return llmcall.UnknownPart(raw)
	}
}

func decodeImageSource(raw json.RawMessage, img *llmcall.ImagePart) error {
	var src struct {
		Type      string `json:"type"`
		MediaType string `json:"media_type"`
		Data      string `json:"data"`
		URL       string `json:"url"`
	}
	if err := json.Unmarshal(raw, &src); err != nil {
		return err
	}
	img.MIMEType = src.MediaType
	img.DataB64 = src.Data
	img.URL = src.URL
	return nil
}

func imageShadowVal(img *llmcall.ImagePart) string {
	return img.MIMEType + "\x00" + img.DataB64 + "\x00" + img.URL
}

func decodeTools(raw json.RawMessage) ([]llmcall.ToolDefinition, error) {
	var elems []json.RawMessage
	if err := json.Unmarshal(raw, &elems); err != nil {
		return nil, err
	}
	tools := make([]llmcall.ToolDefinition, 0, len(elems))
	for _, el := range elems {
		var td llmcall.ToolDefinition
		tw := &td.Wire
		err := llmcall.DecodeKnown(el, tw, map[string]func(json.RawMessage) error{
			"name": func(v json.RawMessage) error {
				s, err := llmcall.DecodeShadowString(v, tw, "name")
				td.Name = s
				return err
			},
			"description": func(v json.RawMessage) error {
				s, err := llmcall.DecodeShadowString(v, tw, "description")
				td.Description = s
				return err
			},
			"input_schema": func(v json.RawMessage) error {
				c, err := llmcall.CompactRaw(v)
				td.Parameters = c
				return err
			},
		})
		if err != nil {
			return nil, err
		}
		tools = append(tools, td)
	}
	return tools, nil
}

func decodeToolChoice(raw json.RawMessage) (*llmcall.ToolChoice, error) {
	tc := &llmcall.ToolChoice{}
	tw := &tc.Wire
	err := llmcall.DecodeKnown(raw, tw, map[string]func(json.RawMessage) error{
		"type": func(v json.RawMessage) error {
			var s string
			if err := json.Unmarshal(v, &s); err != nil {
				return err
			}
			switch s {
			case "auto":
				tc.Mode = llmcall.ToolChoiceAuto
			case "any":
				tc.Mode = llmcall.ToolChoiceRequired
			case "tool":
				tc.Mode = llmcall.ToolChoiceNamed
			case "none":
				tc.Mode = llmcall.ToolChoiceNone
			default:
				tc.Mode = llmcall.ToolChoiceMode(s)
			}
			return nil
		},
		"name": func(v json.RawMessage) error {
			s, err := llmcall.DecodeShadowString(v, tw, "name")
			tc.Name = s
			return err
		},
	})
	if err != nil {
		return nil, err
	}
	return tc, nil
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
		"max_tokens": func(e *llmcall.ObjectEncoder) {
			if req.Generation.MaxTokens == "" {
				// The Messages API requires max_tokens; sources that
				// don't (OpenAI omits it freely) decode to an empty
				// value. Strip mode — the cross-schema translation
				// path — synthesizes the customary default instead of
				// emitting a request the upstream would 400. Preserve
				// mode keeps the absence: a same-schema round trip
				// must not invent bytes.
				if mode == llmcall.ModeStrip {
					e.Field("max_tokens", json.Number(defaultMaxTokens))
				}
				return
			}
			numberEmitter(w, "max_tokens", &req.Generation.MaxTokens)(e)
		},
		"temperature": numberEmitter(w, "temperature", &req.Generation.Temperature),
		"top_p":       numberEmitter(w, "top_p", &req.Generation.TopP),
		"top_k":       numberEmitter(w, "top_k", &req.Generation.TopK),
		"stop_sequences": func(e *llmcall.ObjectEncoder) {
			if len(req.Generation.StopSequences) == 0 && !w.HasKey("stop_sequences") {
				return
			}
			if raw, ok := w.ShadowRaw("stop_sequences", llmcall.StringSliceShadowVal(req.Generation.StopSequences)); ok {
				e.Raw("stop_sequences", raw)
				return
			}
			e.Field("stop_sequences", req.Generation.StopSequences)
		},
		"stream": func(e *llmcall.ObjectEncoder) {
			if !req.Stream && !w.HasKey("stream") {
				return
			}
			e.Field("stream", req.Stream)
		},
		"system": func(e *llmcall.ObjectEncoder) {
			if len(req.System) == 0 {
				return
			}
			if w.Hint("system") == "string" {
				llmcall.EmitShadowString(e, w, "system", llmcall.CollectText(req.System))
				return
			}
			raw, err := encodeParts(req.System, mode, dropped)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("system", raw)
		},
		"messages": func(e *llmcall.ObjectEncoder) {
			raw, err := encodeMessages(req.Messages, mode, dropped)
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
			raw, err := encodeTools(req.Tools, mode, dropped)
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
			raw, err := encodeToolChoice(req.ToolChoice, mode, dropped)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("tool_choice", raw)
		},
	}
	canonical := []string{"model", "max_tokens", "messages", "system", "tools", "tool_choice", "temperature", "top_p", "top_k", "stop_sequences", "stream"}

	fresh, err := llmcall.EncodeOrderedObject(w, mode, emit, canonical, dropped)
	if err != nil {
		return nil, err
	}
	if mode == llmcall.ModePreserve {
		out.Body, out.Exact = llmcall.EncodeRaw(fresh, w.Raw)
	} else {
		out.Body = fresh
	}

	out.Path = "/v1/messages"
	if mode == llmcall.ModePreserve && w.Path != "" {
		out.Path = w.Path
	}
	version := defaultVersion
	if v := w.Headers["anthropic-version"]; v != "" {
		version = v
	}
	out.SetHeaders = map[string]string{"anthropic-version": version}
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

func encodeMessages(msgs []llmcall.Message, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	elems := make([]json.RawMessage, 0, len(msgs))
	for i := range msgs {
		msg := &msgs[i]
		mw := &msg.Wire
		emit := map[string]llmcall.FieldEmitter{
			"role": func(e *llmcall.ObjectEncoder) {
				e.Field("role", string(msg.Role))
			},
			"content": func(e *llmcall.ObjectEncoder) {
				if mw.Hint("content") == "string" {
					llmcall.EmitShadowString(e, mw, "content", llmcall.CollectText(msg.Parts))
					return
				}
				raw, err := encodeParts(msg.Parts, mode, dropped)
				if err != nil {
					e.Fail(err)
					return
				}
				e.Raw("content", raw)
			},
		}
		b, err := llmcall.EncodeOrderedObject(mw, mode, emit, []string{"role", "content"}, dropped)
		if err != nil {
			return nil, err
		}
		elems = append(elems, b)
	}
	return llmcall.EncodeArray(elems), nil
}

func encodeParts(parts []llmcall.Part, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	elems := make([]json.RawMessage, 0, len(parts))
	for i := range parts {
		b, err := encodeBlock(&parts[i], mode, dropped)
		if err != nil {
			return nil, err
		}
		if b != nil {
			elems = append(elems, b)
		}
	}
	return llmcall.EncodeArray(elems), nil
}

func encodeBlock(p *llmcall.Part, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	switch p.Type {
	case llmcall.PartTypeText:
		t := p.Text
		tw := &t.Wire
		return llmcall.EncodeOrderedObject(tw, mode, map[string]llmcall.FieldEmitter{
			"type": func(e *llmcall.ObjectEncoder) { e.Field("type", "text") },
			"text": func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowString(e, tw, "text", t.Text) },
		}, []string{"type", "text"}, dropped)
	case llmcall.PartTypeToolCall:
		tc := p.ToolCall
		tw := &tc.Wire
		return llmcall.EncodeOrderedObject(tw, mode, map[string]llmcall.FieldEmitter{
			"type": func(e *llmcall.ObjectEncoder) { e.Field("type", "tool_use") },
			"id":   func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowString(e, tw, "id", tc.ID) },
			"name": func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowString(e, tw, "name", tc.Name) },
			"input": func(e *llmcall.ObjectEncoder) {
				if len(tc.Arguments) == 0 {
					e.Raw("input", json.RawMessage("{}"))
					return
				}
				e.Raw("input", tc.Arguments)
			},
		}, []string{"type", "id", "name", "input"}, dropped)
	case llmcall.PartTypeToolCallResponse:
		tr := p.ToolCallResponse
		tw := &tr.Wire
		return llmcall.EncodeOrderedObject(tw, mode, map[string]llmcall.FieldEmitter{
			"type":        func(e *llmcall.ObjectEncoder) { e.Field("type", "tool_result") },
			"tool_use_id": func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowString(e, tw, "tool_use_id", tr.ToolCallID) },
			"is_error": func(e *llmcall.ObjectEncoder) {
				if !tr.IsError && !tw.HasKey("is_error") {
					return
				}
				e.Field("is_error", tr.IsError)
			},
			"content": func(e *llmcall.ObjectEncoder) {
				if tw.Hint("content") == "string" {
					llmcall.EmitShadowString(e, tw, "content", llmcall.CollectText(tr.Parts))
					return
				}
				if len(tr.Parts) == 0 && !tw.HasKey("content") {
					return
				}
				raw, err := encodeParts(tr.Parts, mode, dropped)
				if err != nil {
					e.Fail(err)
					return
				}
				e.Raw("content", raw)
			},
		}, []string{"type", "tool_use_id", "content", "is_error"}, dropped)
	case llmcall.PartTypeReasoning:
		r := p.Reasoning
		rw := &r.Wire
		return llmcall.EncodeOrderedObject(rw, mode, map[string]llmcall.FieldEmitter{
			"type":     func(e *llmcall.ObjectEncoder) { e.Field("type", "thinking") },
			"thinking": func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowString(e, rw, "thinking", r.Text) },
			"signature": func(e *llmcall.ObjectEncoder) {
				if r.Signature == "" && !rw.HasKey("signature") {
					return
				}
				llmcall.EmitShadowString(e, rw, "signature", r.Signature)
			},
		}, []string{"type", "thinking", "signature"}, dropped)
	case llmcall.PartTypeImage:
		img := p.Image
		iw := &img.Wire
		return llmcall.EncodeOrderedObject(iw, mode, map[string]llmcall.FieldEmitter{
			"type": func(e *llmcall.ObjectEncoder) { e.Field("type", "image") },
			"source": func(e *llmcall.ObjectEncoder) {
				if raw, ok := iw.ShadowRaw("source", imageShadowVal(img)); ok {
					e.Raw("source", raw)
					return
				}
				raw, err := encodeImageSource(img)
				if err != nil {
					e.Fail(err)
					return
				}
				e.Raw("source", raw)
			},
		}, []string{"type", "source"}, dropped)
	case llmcall.PartTypeUnknown:
		if mode == llmcall.ModePreserve && len(p.Wire.Raw) > 0 {
			return p.Wire.Raw, nil
		}
		*dropped++
		return nil, nil
	default:
		// Refusal, citation, audio, file: the Messages API has no
		// block for them; drop with a count in either mode.
		*dropped++
		return nil, nil
	}
}

func encodeImageSource(img *llmcall.ImagePart) (json.RawMessage, error) {
	e := llmcall.NewObjectEncoder()
	if img.URL != "" {
		e.Field("type", "url")
		e.Field("url", img.URL)
		return e.Bytes()
	}
	e.Field("type", "base64")
	e.Field("media_type", img.MIMEType)
	e.Field("data", img.DataB64)
	return e.Bytes()
}

func encodeTools(tools []llmcall.ToolDefinition, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	elems := make([]json.RawMessage, 0, len(tools))
	for i := range tools {
		td := &tools[i]
		tw := &td.Wire
		emit := map[string]llmcall.FieldEmitter{
			"name": func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowString(e, tw, "name", td.Name) },
			"description": func(e *llmcall.ObjectEncoder) {
				if td.Description == "" && !tw.HasKey("description") {
					return
				}
				llmcall.EmitShadowString(e, tw, "description", td.Description)
			},
			"input_schema": func(e *llmcall.ObjectEncoder) {
				if len(td.Parameters) == 0 {
					e.Raw("input_schema", json.RawMessage(`{"type":"object"}`))
					return
				}
				e.Raw("input_schema", td.Parameters)
			},
		}
		b, err := llmcall.EncodeOrderedObject(tw, mode, emit, []string{"name", "description", "input_schema"}, dropped)
		if err != nil {
			return nil, err
		}
		elems = append(elems, b)
	}
	return llmcall.EncodeArray(elems), nil
}

func encodeToolChoice(tc *llmcall.ToolChoice, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	tw := &tc.Wire
	emit := map[string]llmcall.FieldEmitter{
		"type": func(e *llmcall.ObjectEncoder) {
			var t string
			switch tc.Mode {
			case llmcall.ToolChoiceAuto:
				t = "auto"
			case llmcall.ToolChoiceRequired:
				t = "any"
			case llmcall.ToolChoiceNamed:
				t = "tool"
			case llmcall.ToolChoiceNone:
				t = "none"
			default:
				t = string(tc.Mode)
			}
			e.Field("type", t)
		},
		"name": func(e *llmcall.ObjectEncoder) {
			if tc.Name == "" && !tw.HasKey("name") {
				return
			}
			llmcall.EmitShadowString(e, tw, "name", tc.Name)
		},
	}
	return llmcall.EncodeOrderedObject(tw, mode, emit, []string{"type", "name"}, dropped)
}

func (codec) DecodeResponse(in llmcall.ResponseInput, req *llmcall.Request) (*llmcall.Response, error) {
	if in.Status != 0 && in.Status != 200 {
		return nil, fmt.Errorf("status %d: %w", in.Status, llmcall.ErrUnsupported)
	}
	if llmcall.HasContentTypePrefix(in.Headers, "text/event-stream") ||
		llmcall.HasContentTypePrefix(in.Headers, "application/x-ndjson") {
		return nil, fmt.Errorf("streaming response: %w", llmcall.ErrUnsupported)
	}

	resp := &llmcall.Response{Provider: "anthropic"}
	resp.Wire.Raw = json.RawMessage(in.Body)
	choice := llmcall.Choice{Message: llmcall.Message{Role: llmcall.RoleAssistant}}

	w := &resp.Wire
	var typeErr error
	err := llmcall.DecodeKnown(in.Body, w, map[string]func(json.RawMessage) error{
		"id": func(v json.RawMessage) error {
			s, err := llmcall.DecodeShadowString(v, w, "id")
			resp.ID = s
			return err
		},
		"type": func(v json.RawMessage) error {
			var s string
			if err := json.Unmarshal(v, &s); err != nil {
				return err
			}
			if s != "message" {
				typeErr = fmt.Errorf("response type %q: %w", s, llmcall.ErrUnsupported)
			}
			return nil
		},
		"role": func(v json.RawMessage) error {
			var s string
			if err := json.Unmarshal(v, &s); err != nil {
				return err
			}
			choice.Message.Role = llmcall.Role(s)
			return nil
		},
		"model": func(v json.RawMessage) error {
			s, err := llmcall.DecodeShadowString(v, w, "model")
			resp.Model = s
			return err
		},
		"content": func(v json.RawMessage) error {
			parts, err := decodeParts(v)
			choice.Message.Parts = parts
			return err
		},
		"stop_reason": func(v json.RawMessage) error {
			if string(v) == "null" {
				w.SetHint("stop_reason", "null")
				return nil
			}
			var s string
			if err := json.Unmarshal(v, &s); err != nil {
				return err
			}
			choice.FinishReasonRaw = s
			choice.FinishReason = canonicalFinish(s)
			return nil
		},
		"usage": func(v json.RawMessage) error {
			return decodeUsage(v, &resp.Usage)
		},
	})
	if err != nil {
		return nil, malformed("response body", err)
	}
	if typeErr != nil {
		return nil, typeErr
	}
	resp.Choices = []llmcall.Choice{choice}
	return resp, nil
}

func canonicalFinish(s string) llmcall.FinishReason {
	switch s {
	case "end_turn", "stop_sequence":
		return llmcall.FinishReasonStop
	case "max_tokens":
		return llmcall.FinishReasonLength
	case "tool_use":
		return llmcall.FinishReasonToolCalls
	case "refusal":
		return llmcall.FinishReasonContentFilter
	default:
		return ""
	}
}

func anthropicFinish(c llmcall.Choice) string {
	if c.FinishReasonRaw != "" {
		return c.FinishReasonRaw
	}
	switch c.FinishReason {
	case llmcall.FinishReasonStop:
		return "end_turn"
	case llmcall.FinishReasonLength:
		return "max_tokens"
	case llmcall.FinishReasonToolCalls:
		return "tool_use"
	case llmcall.FinishReasonContentFilter:
		return "refusal"
	default:
		return "end_turn"
	}
}

func decodeUsage(raw json.RawMessage, u *llmcall.Usage) error {
	uw := &u.Wire
	return llmcall.DecodeKnown(raw, uw, map[string]func(json.RawMessage) error{
		"input_tokens": func(v json.RawMessage) error {
			n, err := llmcall.DecodeShadowInt64(v, uw, "input_tokens")
			u.InputTokens = n
			return err
		},
		"output_tokens": func(v json.RawMessage) error {
			n, err := llmcall.DecodeShadowInt64(v, uw, "output_tokens")
			u.OutputTokens = n
			return err
		},
	})
}

func (codec) EncodeResponse(resp *llmcall.Response, opts llmcall.EncodeOptions) (*llmcall.EncodedResponse, error) {
	out := &llmcall.EncodedResponse{}
	mode := opts.Mode
	dropped := &out.DroppedExtras
	w := &resp.Wire

	var choice llmcall.Choice
	if len(resp.Choices) > 0 {
		choice = resp.Choices[0]
	}

	emit := map[string]llmcall.FieldEmitter{
		"id":   func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowString(e, w, "id", resp.ID) },
		"type": func(e *llmcall.ObjectEncoder) { e.Field("type", "message") },
		"role": func(e *llmcall.ObjectEncoder) {
			role := choice.Message.Role
			if role == "" {
				role = llmcall.RoleAssistant
			}
			e.Field("role", string(role))
		},
		"model": func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowString(e, w, "model", resp.Model) },
		"content": func(e *llmcall.ObjectEncoder) {
			raw, err := encodeParts(choice.Message.Parts, mode, dropped)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("content", raw)
		},
		"stop_reason": func(e *llmcall.ObjectEncoder) {
			if w.Hint("stop_reason") == "null" {
				e.Raw("stop_reason", json.RawMessage("null"))
				return
			}
			e.Field("stop_reason", anthropicFinish(choice))
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
	canonical := []string{"id", "type", "role", "model", "content", "stop_reason", "usage"}

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

func encodeUsage(u *llmcall.Usage, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	uw := &u.Wire
	emit := map[string]llmcall.FieldEmitter{
		"input_tokens":  func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowInt64(e, uw, "input_tokens", u.InputTokens) },
		"output_tokens": func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowInt64(e, uw, "output_tokens", u.OutputTokens) },
	}
	return llmcall.EncodeOrderedObject(uw, mode, emit, []string{"input_tokens", "output_tokens"}, dropped)
}
