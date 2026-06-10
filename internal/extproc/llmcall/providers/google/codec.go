package google

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
)

// codec implements llmcall.Codec for the native Gemini generateContent
// surface. Phase 1 scope: non-streaming :generateContent only — the
// OAI-compat layer, :streamGenerateContent, embeddings, and streamed
// framings return ErrUnsupported. Unlike the body-borne schemas, the
// model rides in the URL; DecodeRequest lifts it from the path and
// EncodeRequest synthesizes the path back.
type codec struct{}

func malformed(detail string, err error) error {
	return &llmcall.MalformedError{Provider: "google_genai", Detail: detail, Err: err}
}

func (codec) DecodeRequest(in llmcall.RequestInput) (*llmcall.Request, error) {
	if isGoogleOpenAICompatPath(in.Path) {
		return nil, fmt.Errorf("openai-compat path %q: %w", in.Path, llmcall.ErrUnsupported)
	}
	model, method := googlePathModelMethod(in.Path)
	if method != "generateContent" {
		return nil, fmt.Errorf("path %q: %w", in.Path, llmcall.ErrUnsupported)
	}

	req := &llmcall.Request{Provider: "google_genai", Operation: "chat", Model: model}
	req.Wire.Raw = json.RawMessage(in.Body)
	req.Wire.Path = in.Path

	w := &req.Wire
	err := llmcall.DecodeKnown(in.Body, w, map[string]func(json.RawMessage) error{
		"contents": func(v json.RawMessage) error {
			msgs, err := decodeContents(v)
			req.Messages = msgs
			return err
		},
		"systemInstruction": func(v json.RawMessage) error {
			// The envelope ({role?, parts}) rides a composite shadow:
			// verbatim while the system text is unmutated, canonical
			// {"parts":[{"text": ...}]} once changed.
			var env struct {
				Parts []json.RawMessage `json:"parts"`
			}
			if err := json.Unmarshal(v, &env); err != nil {
				return err
			}
			for _, p := range env.Parts {
				part, err := decodePart(p)
				if err != nil {
					return err
				}
				req.System = append(req.System, part)
			}
			c, err := llmcall.CompactRaw(v)
			if err != nil {
				return err
			}
			w.RecordShadow("systemInstruction", c, llmcall.CollectText(req.System))
			return nil
		},
		"generationConfig": func(v json.RawMessage) error {
			return decodeGenerationConfig(v, &req.Generation)
		},
		"tools": func(v json.RawMessage) error {
			return decodeTools(v, req)
		},
		"toolConfig": func(v json.RawMessage) error {
			return decodeToolConfig(v, req)
		},
	})
	if err != nil {
		return nil, malformed("request body", err)
	}
	return req, nil
}

// googlePathModelMethod extracts (model, method) from a Gemini API
// path, query string excluded.
func googlePathModelMethod(path string) (string, string) {
	if i := strings.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}
	_, after, ok := strings.Cut(path, "/models/")
	if !ok {
		return "", ""
	}
	model, method, ok := strings.Cut(after, ":")
	if !ok {
		return model, ""
	}
	return model, method
}

func textPart(s string) llmcall.Part {
	return llmcall.Part{Type: llmcall.PartTypeText, Text: &llmcall.TextPart{Text: s}}
}

func decodeContents(raw json.RawMessage) ([]llmcall.Message, error) {
	var elems []json.RawMessage
	if err := json.Unmarshal(raw, &elems); err != nil {
		return nil, err
	}
	msgs := make([]llmcall.Message, 0, len(elems))
	for _, el := range elems {
		msg, err := decodeContent(el)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func decodeContent(el json.RawMessage) (llmcall.Message, error) {
	var msg llmcall.Message
	mw := &msg.Wire
	err := llmcall.DecodeKnown(el, mw, map[string]func(json.RawMessage) error{
		"role": func(v json.RawMessage) error {
			var s string
			if err := json.Unmarshal(v, &s); err != nil {
				return err
			}
			// Gemini's assistant role is "model"; the mapping is
			// bijective so no hint is needed.
			if s == "model" {
				msg.Role = llmcall.RoleAssistant
				return nil
			}
			msg.Role = llmcall.Role(s)
			return nil
		},
		"parts": func(v json.RawMessage) error {
			var parts []json.RawMessage
			if err := json.Unmarshal(v, &parts); err != nil {
				return err
			}
			for _, p := range parts {
				part, err := decodePart(p)
				if err != nil {
					return err
				}
				msg.Parts = append(msg.Parts, part)
			}
			return nil
		},
	})
	return msg, err
}

// decodePart discriminates a Gemini part by member presence (parts
// carry no "type" field): text (+thought), inlineData, fileData,
// functionCall, functionResponse; anything else is unknown.
func decodePart(raw json.RawMessage) (llmcall.Part, error) {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return llmcall.Part{}, err
	}
	switch {
	case probe["thought"] != nil && probe["text"] != nil:
		p := llmcall.Part{Type: llmcall.PartTypeReasoning, Reasoning: &llmcall.ReasoningPart{}}
		pw := &p.Reasoning.Wire
		err := llmcall.DecodeKnown(raw, pw, map[string]func(json.RawMessage) error{
			"text": func(v json.RawMessage) error {
				s, err := llmcall.DecodeShadowString(v, pw, "text")
				p.Reasoning.Text = s
				return err
			},
			"thought": func(json.RawMessage) error { return nil },
			"thoughtSignature": func(v json.RawMessage) error {
				s, err := llmcall.DecodeShadowString(v, pw, "thoughtSignature")
				p.Reasoning.Signature = s
				return err
			},
		})
		return p, err
	case probe["text"] != nil:
		p := llmcall.Part{Type: llmcall.PartTypeText, Text: &llmcall.TextPart{}}
		pw := &p.Text.Wire
		err := llmcall.DecodeKnown(raw, pw, map[string]func(json.RawMessage) error{
			"text": func(v json.RawMessage) error {
				s, err := llmcall.DecodeShadowString(v, pw, "text")
				p.Text.Text = s
				return err
			},
		})
		return p, err
	case probe["inlineData"] != nil:
		return decodeInlineData(raw)
	case probe["fileData"] != nil:
		return decodeFileData(raw)
	case probe["functionCall"] != nil:
		p := llmcall.Part{Type: llmcall.PartTypeToolCall, ToolCall: &llmcall.ToolCallPart{}}
		pw := &p.ToolCall.Wire
		err := llmcall.DecodeKnown(raw, pw, map[string]func(json.RawMessage) error{
			"functionCall": func(v json.RawMessage) error {
				var fc struct {
					Name string          `json:"name"`
					Args json.RawMessage `json:"args"`
				}
				if err := json.Unmarshal(v, &fc); err != nil {
					return err
				}
				p.ToolCall.Name = fc.Name
				if len(fc.Args) > 0 {
					c, err := llmcall.CompactRaw(fc.Args)
					if err != nil {
						return err
					}
					p.ToolCall.Arguments = c
				}
				c, err := llmcall.CompactRaw(v)
				if err != nil {
					return err
				}
				pw.RecordShadow("functionCall", c, functionCallShadowVal(p.ToolCall))
				return nil
			},
		})
		return p, err
	case probe["functionResponse"] != nil:
		p := llmcall.Part{Type: llmcall.PartTypeToolCallResponse, ToolCallResponse: &llmcall.ToolCallResponsePart{}}
		pw := &p.ToolCallResponse.Wire
		err := llmcall.DecodeKnown(raw, pw, map[string]func(json.RawMessage) error{
			"functionResponse": func(v json.RawMessage) error {
				var fr struct {
					Name     string          `json:"name"`
					Response json.RawMessage `json:"response"`
				}
				if err := json.Unmarshal(v, &fr); err != nil {
					return err
				}
				// Gemini correlates results by function name, not call
				// ID; the name doubles as the IR's correlation key.
				p.ToolCallResponse.ToolCallID = fr.Name
				if len(fr.Response) > 0 {
					c, err := llmcall.CompactRaw(fr.Response)
					if err != nil {
						return err
					}
					pw.SetHint("response", string(c))
				}
				c, err := llmcall.CompactRaw(v)
				if err != nil {
					return err
				}
				pw.RecordShadow("functionResponse", c, fr.Name+"\x00"+pw.Hint("response"))
				return nil
			},
		})
		return p, err
	default:
		return llmcall.UnknownPart(raw)
	}
}

func functionCallShadowVal(tc *llmcall.ToolCallPart) string {
	return tc.Name + "\x00" + string(tc.Arguments)
}

func inlineShadowVal(mime, data, url string) string {
	return mime + "\x00" + data + "\x00" + url
}

func decodeInlineData(raw json.RawMessage) (llmcall.Part, error) {
	var env struct {
		InlineData struct {
			MimeType string `json:"mimeType"`
			Data     string `json:"data"`
		} `json:"inlineData"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return llmcall.Part{}, err
	}
	p := mediaPart(env.InlineData.MimeType)
	pw := partMediaWire(&p)
	err := llmcall.DecodeKnown(raw, pw, map[string]func(json.RawMessage) error{
		"inlineData": func(v json.RawMessage) error {
			c, err := llmcall.CompactRaw(v)
			if err != nil {
				return err
			}
			pw.RecordShadow("inlineData", c, inlineShadowVal(env.InlineData.MimeType, env.InlineData.Data, ""))
			return nil
		},
	})
	setMediaFields(&p, env.InlineData.MimeType, env.InlineData.Data, "")
	return p, err
}

func decodeFileData(raw json.RawMessage) (llmcall.Part, error) {
	var env struct {
		FileData struct {
			MimeType string `json:"mimeType"`
			FileURI  string `json:"fileUri"`
		} `json:"fileData"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return llmcall.Part{}, err
	}
	p := mediaPart(env.FileData.MimeType)
	pw := partMediaWire(&p)
	err := llmcall.DecodeKnown(raw, pw, map[string]func(json.RawMessage) error{
		"fileData": func(v json.RawMessage) error {
			c, err := llmcall.CompactRaw(v)
			if err != nil {
				return err
			}
			pw.RecordShadow("fileData", c, inlineShadowVal(env.FileData.MimeType, "", env.FileData.FileURI))
			return nil
		},
	})
	setMediaFields(&p, env.FileData.MimeType, "", env.FileData.FileURI)
	return p, err
}

// mediaPart picks the IR part kind for a Gemini media payload by MIME
// class.
func mediaPart(mime string) llmcall.Part {
	switch {
	case strings.HasPrefix(mime, "image/"):
		return llmcall.Part{Type: llmcall.PartTypeImage, Image: &llmcall.ImagePart{}}
	case strings.HasPrefix(mime, "audio/"):
		return llmcall.Part{Type: llmcall.PartTypeAudio, Audio: &llmcall.AudioPart{}}
	default:
		return llmcall.Part{Type: llmcall.PartTypeFile, File: &llmcall.FilePart{}}
	}
}

func partMediaWire(p *llmcall.Part) *llmcall.Wire {
	switch p.Type {
	case llmcall.PartTypeImage:
		return &p.Image.Wire
	case llmcall.PartTypeAudio:
		return &p.Audio.Wire
	default:
		return &p.File.Wire
	}
}

func setMediaFields(p *llmcall.Part, mime, data, url string) {
	switch p.Type {
	case llmcall.PartTypeImage:
		p.Image.MIMEType, p.Image.DataB64, p.Image.URL = mime, data, url
	case llmcall.PartTypeAudio:
		p.Audio.MIMEType, p.Audio.DataB64, p.Audio.URL = mime, data, url
	default:
		p.File.MIMEType, p.File.DataB64, p.File.URL = mime, data, url
	}
}

func mediaFields(p *llmcall.Part) (mime, data, url string) {
	switch p.Type {
	case llmcall.PartTypeImage:
		return p.Image.MIMEType, p.Image.DataB64, p.Image.URL
	case llmcall.PartTypeAudio:
		return p.Audio.MIMEType, p.Audio.DataB64, p.Audio.URL
	case llmcall.PartTypeFile:
		return p.File.MIMEType, p.File.DataB64, p.File.URL
	default:
		return "", "", ""
	}
}

func decodeGenerationConfig(raw json.RawMessage, gen *llmcall.GenerationConfig) error {
	gw := &gen.Wire
	return llmcall.DecodeKnown(raw, gw, map[string]func(json.RawMessage) error{
		"temperature": func(v json.RawMessage) error {
			n, err := llmcall.DecodeNumber(v)
			gen.Temperature = n
			return err
		},
		"topP": func(v json.RawMessage) error {
			n, err := llmcall.DecodeNumber(v)
			gen.TopP = n
			return err
		},
		"topK": func(v json.RawMessage) error {
			n, err := llmcall.DecodeNumber(v)
			gen.TopK = n
			return err
		},
		"maxOutputTokens": func(v json.RawMessage) error {
			n, err := llmcall.DecodeNumber(v)
			gen.MaxTokens = n
			return err
		},
		"stopSequences": func(v json.RawMessage) error {
			ss, err := llmcall.DecodeShadowStringSlice(v, gw, "stopSequences")
			gen.StopSequences = ss
			return err
		},
	})
}

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

// decodeTools flattens tools[].functionDeclarations into the IR tool
// list. Non-function tool kinds (googleSearch, codeExecution) are not
// modeled: the whole-array shadow re-emits them verbatim while the
// tool list is unmutated; a mutated list re-encodes function
// declarations only.
func decodeTools(v json.RawMessage, req *llmcall.Request) error {
	var elems []struct {
		FunctionDeclarations []struct {
			Name        string          `json:"name"`
			Description string          `json:"description"`
			Parameters  json.RawMessage `json:"parameters"`
		} `json:"functionDeclarations"`
	}
	if err := json.Unmarshal(v, &elems); err != nil {
		return err
	}
	for _, el := range elems {
		for _, fd := range el.FunctionDeclarations {
			td := llmcall.ToolDefinition{Name: fd.Name, Description: fd.Description}
			if len(fd.Parameters) > 0 {
				c, err := llmcall.CompactRaw(fd.Parameters)
				if err != nil {
					return err
				}
				td.Parameters = c
			}
			req.Tools = append(req.Tools, td)
		}
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

func decodeToolConfig(v json.RawMessage, req *llmcall.Request) error {
	var env struct {
		FunctionCallingConfig struct {
			Mode                 string   `json:"mode"`
			AllowedFunctionNames []string `json:"allowedFunctionNames"`
		} `json:"functionCallingConfig"`
	}
	if err := json.Unmarshal(v, &env); err != nil {
		return err
	}
	tc := &llmcall.ToolChoice{}
	switch env.FunctionCallingConfig.Mode {
	case "AUTO", "":
		tc.Mode = llmcall.ToolChoiceAuto
	case "NONE":
		tc.Mode = llmcall.ToolChoiceNone
	case "ANY":
		tc.Mode = llmcall.ToolChoiceRequired
		if len(env.FunctionCallingConfig.AllowedFunctionNames) == 1 {
			tc.Mode = llmcall.ToolChoiceNamed
			tc.Name = env.FunctionCallingConfig.AllowedFunctionNames[0]
		}
	default:
		tc.Mode = llmcall.ToolChoiceMode(env.FunctionCallingConfig.Mode)
	}
	c, err := llmcall.CompactRaw(v)
	if err != nil {
		return err
	}
	req.Wire.RecordShadow("toolConfig", c, toolChoiceShadowVal(tc))
	req.ToolChoice = tc
	return nil
}

func (c codec) EncodeRequest(req *llmcall.Request, opts llmcall.EncodeOptions) (*llmcall.EncodedRequest, error) {
	out := &llmcall.EncodedRequest{}
	w := &req.Wire
	mode := opts.Mode
	dropped := &out.DroppedExtras

	emit := map[string]llmcall.FieldEmitter{
		"contents": func(e *llmcall.ObjectEncoder) {
			raw, err := encodeContents(req.Messages, mode, dropped)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("contents", raw)
		},
		"systemInstruction": func(e *llmcall.ObjectEncoder) {
			if len(req.System) == 0 {
				return
			}
			if raw, ok := w.ShadowRaw("systemInstruction", llmcall.CollectText(req.System)); ok {
				e.Raw("systemInstruction", raw)
				return
			}
			inner := llmcall.NewObjectEncoder()
			partsRaw, err := encodeParts(req.System, mode, dropped)
			if err != nil {
				e.Fail(err)
				return
			}
			inner.Raw("parts", partsRaw)
			raw, err := inner.Bytes()
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("systemInstruction", raw)
		},
		"generationConfig": func(e *llmcall.ObjectEncoder) {
			raw, present, err := encodeGenerationConfig(&req.Generation, mode, dropped)
			if err != nil {
				e.Fail(err)
				return
			}
			if !present && !w.HasKey("generationConfig") {
				return
			}
			e.Raw("generationConfig", raw)
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
		"toolConfig": func(e *llmcall.ObjectEncoder) {
			if req.ToolChoice == nil {
				return
			}
			if raw, ok := w.ShadowRaw("toolConfig", toolChoiceShadowVal(req.ToolChoice)); ok && mode == llmcall.ModePreserve {
				e.Raw("toolConfig", raw)
				return
			}
			raw, err := encodeToolConfig(req.ToolChoice)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("toolConfig", raw)
		},
	}
	canonical := []string{"contents", "systemInstruction", "tools", "toolConfig", "generationConfig"}

	fresh, err := llmcall.EncodeOrderedObject(w, mode, emit, canonical, dropped)
	if err != nil {
		return nil, err
	}
	if mode == llmcall.ModePreserve {
		out.Body, out.Exact = llmcall.EncodeRaw(fresh, w.Raw)
	} else {
		out.Body = fresh
	}
	out.Path = "/v1beta/models/" + req.Model + ":generateContent"
	if mode == llmcall.ModePreserve && w.Path != "" {
		out.Path = w.Path
	}
	out.SetHeaders = map[string]string{"content-type": "application/json"}
	return out, nil
}

func encodeContents(msgs []llmcall.Message, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	elems := make([]json.RawMessage, 0, len(msgs))
	for i := range msgs {
		b, err := encodeContent(&msgs[i], mode, dropped)
		if err != nil {
			return nil, err
		}
		elems = append(elems, b)
	}
	return llmcall.EncodeArray(elems), nil
}

func encodeContent(msg *llmcall.Message, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	mw := &msg.Wire
	emit := map[string]llmcall.FieldEmitter{
		"role": func(e *llmcall.ObjectEncoder) {
			role := string(msg.Role)
			if msg.Role == llmcall.RoleAssistant {
				role = "model"
			}
			e.Field("role", role)
		},
		"parts": func(e *llmcall.ObjectEncoder) {
			raw, err := encodeParts(msg.Parts, mode, dropped)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("parts", raw)
		},
	}
	return llmcall.EncodeOrderedObject(mw, mode, emit, []string{"role", "parts"}, dropped)
}

func encodeParts(parts []llmcall.Part, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	elems := make([]json.RawMessage, 0, len(parts))
	for i := range parts {
		b, err := encodePart(&parts[i], mode, dropped)
		if err != nil {
			return nil, err
		}
		if b != nil {
			elems = append(elems, b)
		}
	}
	return llmcall.EncodeArray(elems), nil
}

func encodePart(p *llmcall.Part, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	switch p.Type {
	case llmcall.PartTypeText:
		t := p.Text
		tw := &t.Wire
		return llmcall.EncodeOrderedObject(tw, mode, map[string]llmcall.FieldEmitter{
			"text": func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowString(e, tw, "text", t.Text) },
		}, []string{"text"}, dropped)
	case llmcall.PartTypeReasoning:
		r := p.Reasoning
		rw := &r.Wire
		return llmcall.EncodeOrderedObject(rw, mode, map[string]llmcall.FieldEmitter{
			"text":    func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowString(e, rw, "text", r.Text) },
			"thought": func(e *llmcall.ObjectEncoder) { e.Field("thought", true) },
			"thoughtSignature": func(e *llmcall.ObjectEncoder) {
				if r.Signature == "" && !rw.HasKey("thoughtSignature") {
					return
				}
				llmcall.EmitShadowString(e, rw, "thoughtSignature", r.Signature)
			},
		}, []string{"text", "thought", "thoughtSignature"}, dropped)
	case llmcall.PartTypeToolCall:
		tc := p.ToolCall
		tw := &tc.Wire
		return llmcall.EncodeOrderedObject(tw, mode, map[string]llmcall.FieldEmitter{
			"functionCall": func(e *llmcall.ObjectEncoder) {
				if raw, ok := tw.ShadowRaw("functionCall", functionCallShadowVal(tc)); ok {
					e.Raw("functionCall", raw)
					return
				}
				inner := llmcall.NewObjectEncoder()
				inner.Field("name", tc.Name)
				args := tc.Arguments
				if len(args) == 0 {
					args = json.RawMessage("{}")
				}
				inner.Raw("args", args)
				raw, err := inner.Bytes()
				if err != nil {
					e.Fail(err)
					return
				}
				e.Raw("functionCall", raw)
			},
		}, []string{"functionCall"}, dropped)
	case llmcall.PartTypeToolCallResponse:
		tr := p.ToolCallResponse
		tw := &tr.Wire
		return llmcall.EncodeOrderedObject(tw, mode, map[string]llmcall.FieldEmitter{
			"functionResponse": func(e *llmcall.ObjectEncoder) {
				if raw, ok := tw.ShadowRaw("functionResponse", tr.ToolCallID+"\x00"+tw.Hint("response")); ok {
					e.Raw("functionResponse", raw)
					return
				}
				inner := llmcall.NewObjectEncoder()
				inner.Field("name", tr.ToolCallID)
				response := tw.Hint("response")
				if response == "" {
					response = "{}"
				}
				inner.Raw("response", json.RawMessage(response))
				raw, err := inner.Bytes()
				if err != nil {
					e.Fail(err)
					return
				}
				e.Raw("functionResponse", raw)
			},
		}, []string{"functionResponse"}, dropped)
	case llmcall.PartTypeImage, llmcall.PartTypeAudio, llmcall.PartTypeFile:
		return encodeMediaPart(p, mode, dropped)
	case llmcall.PartTypeUnknown:
		if mode == llmcall.ModePreserve && len(p.Wire.Raw) > 0 {
			return p.Wire.Raw, nil
		}
		*dropped++
		return nil, nil
	default:
		*dropped++
		return nil, nil
	}
}

func encodeMediaPart(p *llmcall.Part, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	pw := partMediaWire(p)
	mime, data, url := mediaFields(p)
	// A data URL (OpenAI's inline form) unpacks into inlineData —
	// fileData.fileUri does not accept data: URIs.
	if url != "" && data == "" {
		if m, d, ok := llmcall.SplitDataURL(url); ok {
			mime, data, url = m, d, ""
		}
	}
	emit := map[string]llmcall.FieldEmitter{
		"inlineData": func(e *llmcall.ObjectEncoder) {
			if url != "" && data == "" {
				return
			}
			if raw, ok := pw.ShadowRaw("inlineData", inlineShadowVal(mime, data, "")); ok {
				e.Raw("inlineData", raw)
				return
			}
			inner := llmcall.NewObjectEncoder()
			inner.Field("mimeType", mime)
			inner.Field("data", data)
			raw, err := inner.Bytes()
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("inlineData", raw)
		},
		"fileData": func(e *llmcall.ObjectEncoder) {
			if url == "" {
				return
			}
			if raw, ok := pw.ShadowRaw("fileData", inlineShadowVal(mime, "", url)); ok {
				e.Raw("fileData", raw)
				return
			}
			inner := llmcall.NewObjectEncoder()
			inner.Field("mimeType", mime)
			inner.Field("fileUri", url)
			raw, err := inner.Bytes()
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("fileData", raw)
		},
	}
	return llmcall.EncodeOrderedObject(pw, mode, emit, []string{"inlineData", "fileData"}, dropped)
}

func encodeGenerationConfig(gen *llmcall.GenerationConfig, mode llmcall.Mode, dropped *int) ([]byte, bool, error) {
	gw := &gen.Wire
	present := gen.Temperature != "" || gen.TopP != "" || gen.TopK != "" || gen.MaxTokens != "" ||
		len(gen.StopSequences) > 0 || len(gw.KeyOrder) > 0
	emit := map[string]llmcall.FieldEmitter{
		"temperature":     numberEmitter("temperature", &gen.Temperature),
		"topP":            numberEmitter("topP", &gen.TopP),
		"topK":            numberEmitter("topK", &gen.TopK),
		"maxOutputTokens": numberEmitter("maxOutputTokens", &gen.MaxTokens),
		"stopSequences": func(e *llmcall.ObjectEncoder) {
			if len(gen.StopSequences) == 0 {
				return
			}
			if raw, ok := gw.ShadowRaw("stopSequences", llmcall.StringSliceShadowVal(gen.StopSequences)); ok {
				e.Raw("stopSequences", raw)
				return
			}
			e.Field("stopSequences", gen.StopSequences)
		},
	}
	raw, err := llmcall.EncodeOrderedObject(gw, mode, emit, []string{"temperature", "topP", "topK", "maxOutputTokens", "stopSequences"}, dropped)
	return raw, present, err
}

func numberEmitter(key string, n *json.Number) llmcall.FieldEmitter {
	return func(e *llmcall.ObjectEncoder) {
		if *n == "" {
			return
		}
		e.Field(key, *n)
	}
}

func encodeTools(tools []llmcall.ToolDefinition) (json.RawMessage, error) {
	decls := make([]json.RawMessage, 0, len(tools))
	for i := range tools {
		td := &tools[i]
		d := llmcall.NewObjectEncoder()
		d.Field("name", td.Name)
		if td.Description != "" {
			d.Field("description", td.Description)
		}
		if len(td.Parameters) > 0 {
			d.Raw("parameters", td.Parameters)
		}
		b, err := d.Bytes()
		if err != nil {
			return nil, err
		}
		decls = append(decls, b)
	}
	el := llmcall.NewObjectEncoder()
	el.Raw("functionDeclarations", llmcall.EncodeArray(decls))
	b, err := el.Bytes()
	if err != nil {
		return nil, err
	}
	return llmcall.EncodeArray([]json.RawMessage{b}), nil
}

func encodeToolConfig(tc *llmcall.ToolChoice) (json.RawMessage, error) {
	fcc := llmcall.NewObjectEncoder()
	switch tc.Mode {
	case llmcall.ToolChoiceAuto:
		fcc.Field("mode", "AUTO")
	case llmcall.ToolChoiceNone:
		fcc.Field("mode", "NONE")
	case llmcall.ToolChoiceRequired:
		fcc.Field("mode", "ANY")
	case llmcall.ToolChoiceNamed:
		fcc.Field("mode", "ANY")
		fcc.Field("allowedFunctionNames", []string{tc.Name})
	default:
		fcc.Field("mode", string(tc.Mode))
	}
	fccRaw, err := fcc.Bytes()
	if err != nil {
		return nil, err
	}
	env := llmcall.NewObjectEncoder()
	env.Raw("functionCallingConfig", fccRaw)
	return env.Bytes()
}

func (codec) DecodeResponse(in llmcall.ResponseInput, req *llmcall.Request) (*llmcall.Response, error) {
	if in.Status != 0 && in.Status != 200 {
		return nil, fmt.Errorf("status %d: %w", in.Status, llmcall.ErrUnsupported)
	}
	if llmcall.HasContentTypePrefix(in.Headers, "text/event-stream") ||
		llmcall.HasContentTypePrefix(in.Headers, "application/x-ndjson") {
		return nil, fmt.Errorf("streaming response: %w", llmcall.ErrUnsupported)
	}

	resp := &llmcall.Response{Provider: "google_genai"}
	resp.Wire.Raw = json.RawMessage(in.Body)

	w := &resp.Wire
	err := llmcall.DecodeKnown(in.Body, w, map[string]func(json.RawMessage) error{
		"candidates": func(v json.RawMessage) error {
			choices, err := decodeCandidates(v)
			resp.Choices = choices
			return err
		},
		"usageMetadata": func(v json.RawMessage) error {
			return decodeUsageMetadata(v, &resp.Usage)
		},
		"modelVersion": func(v json.RawMessage) error {
			s, err := llmcall.DecodeShadowString(v, w, "modelVersion")
			resp.Model = s
			return err
		},
		"responseId": func(v json.RawMessage) error {
			s, err := llmcall.DecodeShadowString(v, w, "responseId")
			resp.ID = s
			return err
		},
	})
	if err != nil {
		return nil, malformed("response body", err)
	}
	return resp, nil
}

func decodeCandidates(raw json.RawMessage) ([]llmcall.Choice, error) {
	var elems []json.RawMessage
	if err := json.Unmarshal(raw, &elems); err != nil {
		return nil, err
	}
	choices := make([]llmcall.Choice, 0, len(elems))
	for _, el := range elems {
		var ch llmcall.Choice
		cw := &ch.Wire
		err := llmcall.DecodeKnown(el, cw, map[string]func(json.RawMessage) error{
			"content": func(v json.RawMessage) error {
				msg, err := decodeContent(v)
				ch.Message = msg
				return err
			},
			"finishReason": func(v json.RawMessage) error {
				var s string
				if err := json.Unmarshal(v, &s); err != nil {
					return err
				}
				ch.FinishReasonRaw = s
				ch.FinishReason = canonicalFinish(s)
				return nil
			},
			// Positional; the emitter re-derives it.
			"index": func(json.RawMessage) error { return nil },
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
	case "STOP":
		return llmcall.FinishReasonStop
	case "MAX_TOKENS":
		return llmcall.FinishReasonLength
	case "SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII":
		return llmcall.FinishReasonContentFilter
	case "MALFORMED_FUNCTION_CALL":
		return llmcall.FinishReasonError
	default:
		return ""
	}
}

func googleFinish(c llmcall.Choice) string {
	if c.FinishReasonRaw != "" {
		return c.FinishReasonRaw
	}
	switch c.FinishReason {
	case llmcall.FinishReasonLength:
		return "MAX_TOKENS"
	case llmcall.FinishReasonContentFilter:
		return "SAFETY"
	case llmcall.FinishReasonError:
		return "MALFORMED_FUNCTION_CALL"
	default:
		// Gemini reports STOP for tool calls too.
		return "STOP"
	}
}

func decodeUsageMetadata(raw json.RawMessage, u *llmcall.Usage) error {
	uw := &u.Wire
	return llmcall.DecodeKnown(raw, uw, map[string]func(json.RawMessage) error{
		"promptTokenCount": func(v json.RawMessage) error {
			n, err := llmcall.DecodeShadowInt64(v, uw, "promptTokenCount")
			u.InputTokens = n
			return err
		},
		"candidatesTokenCount": func(v json.RawMessage) error {
			n, err := llmcall.DecodeShadowInt64(v, uw, "candidatesTokenCount")
			u.OutputTokens = n
			return err
		},
		"totalTokenCount": func(v json.RawMessage) error {
			c, err := llmcall.CompactRaw(v)
			if err != nil {
				return err
			}
			uw.SetHint("totalTokenCount", string(c))
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
		"candidates": func(e *llmcall.ObjectEncoder) {
			raw, err := encodeCandidates(resp.Choices, mode, dropped)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("candidates", raw)
		},
		"usageMetadata": func(e *llmcall.ObjectEncoder) {
			raw, err := encodeUsageMetadata(&resp.Usage, mode, dropped)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("usageMetadata", raw)
		},
		"modelVersion": func(e *llmcall.ObjectEncoder) {
			if resp.Model == "" && !w.HasKey("modelVersion") {
				return
			}
			llmcall.EmitShadowString(e, w, "modelVersion", resp.Model)
		},
		"responseId": func(e *llmcall.ObjectEncoder) {
			if resp.ID == "" && !w.HasKey("responseId") {
				return
			}
			llmcall.EmitShadowString(e, w, "responseId", resp.ID)
		},
	}
	canonical := []string{"candidates", "usageMetadata", "modelVersion", "responseId"}

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

func encodeCandidates(choices []llmcall.Choice, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	elems := make([]json.RawMessage, 0, len(choices))
	for i := range choices {
		ch := &choices[i]
		cw := &ch.Wire
		idx := i
		emit := map[string]llmcall.FieldEmitter{
			"content": func(e *llmcall.ObjectEncoder) {
				raw, err := encodeContent(&ch.Message, mode, dropped)
				if err != nil {
					e.Fail(err)
					return
				}
				e.Raw("content", raw)
			},
			"finishReason": func(e *llmcall.ObjectEncoder) {
				e.Field("finishReason", googleFinish(*ch))
			},
			"index": func(e *llmcall.ObjectEncoder) {
				if !cw.HasKey("index") && mode == llmcall.ModePreserve {
					return
				}
				e.Field("index", idx)
			},
		}
		b, err := llmcall.EncodeOrderedObject(cw, mode, emit, []string{"content", "finishReason", "index"}, dropped)
		if err != nil {
			return nil, err
		}
		elems = append(elems, b)
	}
	return llmcall.EncodeArray(elems), nil
}

func encodeUsageMetadata(u *llmcall.Usage, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	uw := &u.Wire
	emit := map[string]llmcall.FieldEmitter{
		"promptTokenCount": func(e *llmcall.ObjectEncoder) {
			llmcall.EmitShadowInt64(e, uw, "promptTokenCount", u.InputTokens)
		},
		"candidatesTokenCount": func(e *llmcall.ObjectEncoder) {
			llmcall.EmitShadowInt64(e, uw, "candidatesTokenCount", u.OutputTokens)
		},
		"totalTokenCount": func(e *llmcall.ObjectEncoder) {
			if h := uw.Hint("totalTokenCount"); h != "" && mode == llmcall.ModePreserve {
				e.Raw("totalTokenCount", json.RawMessage(h))
				return
			}
			e.Field("totalTokenCount", u.InputTokens+u.OutputTokens)
		},
	}
	return llmcall.EncodeOrderedObject(uw, mode, emit, []string{"promptTokenCount", "candidatesTokenCount", "totalTokenCount"}, dropped)
}
