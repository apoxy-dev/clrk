package bedrock

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/apoxy-dev/clrk/internal/extproc/jsonx"
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
)

// codec implements llmcall.Codec for the Bedrock Converse API:
// non-streaming POST /model/{modelId}/converse chat only. InvokeModel
// endpoints carry provider-native bodies and return ErrUnsupported, as
// does the converse-stream response framing (AWS binary event-stream).
//
// Converse content blocks are single-member unions ({"text": ...},
// {"toolUse": {...}}) rather than type-discriminated objects; the
// decoder peels the one member and treats anything else as an unknown
// part.
type codec struct{}

func malformed(detail string, err error) error {
	return &llmcall.MalformedError{Provider: "aws_bedrock", Detail: detail, Err: err}
}

// bedrockPath parses /model/{modelId}/{op}. modelId is one URL-encoded
// path parameter that may contain ':' raw or as %3A, and ARN-form IDs
// carry '/' (escaped %2F by SDKs, sometimes raw from hand-built
// clients) — so the op is split at the LAST '/', not the first.
func bedrockPath(path string) (modelID, op string, ok bool) {
	if i := strings.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}
	rest, found := strings.CutPrefix(path, "/model/")
	if !found {
		return "", "", false
	}
	i := strings.LastIndex(rest, "/")
	if i <= 0 || i == len(rest)-1 {
		return "", "", false
	}
	escaped, op := rest[:i], rest[i+1:]
	modelID, err := url.PathUnescape(escaped)
	if err != nil {
		modelID = escaped
	}
	return modelID, op, true
}

func (codec) DecodeRequest(in llmcall.RequestInput) (*llmcall.Request, error) {
	modelID, op, ok := bedrockPath(in.Path)
	if !ok || (op != "converse" && op != "converse-stream") {
		return nil, fmt.Errorf("path %q: %w", in.Path, llmcall.ErrUnsupported)
	}

	req := &llmcall.Request{
		Provider:  "aws_bedrock",
		Operation: "chat",
		Model:     modelID,
		Stream:    op == "converse-stream",
	}
	req.Wire.Raw = json.RawMessage(in.Body)
	req.Wire.Path = in.Path

	w := &req.Wire
	err := llmcall.DecodeKnown(in.Body, w, map[string]func(json.RawMessage) error{
		"messages": func(v json.RawMessage) error {
			msgs, err := decodeMessages(v)
			req.Messages = msgs
			return err
		},
		"system": func(v json.RawMessage) error {
			parts, err := decodeSystem(v)
			req.System = parts
			return err
		},
		"inferenceConfig": func(v json.RawMessage) error {
			return decodeInferenceConfig(v, &req.Generation)
		},
		"toolConfig": func(v json.RawMessage) error {
			return decodeToolConfig(v, w, req)
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
	if err := jsonx.Unmarshal(raw, &elems); err != nil {
		return nil, err
	}
	msgs := make([]llmcall.Message, 0, len(elems))
	for _, el := range elems {
		var msg llmcall.Message
		if err := decodeMessage(el, &msg); err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func decodeMessage(raw json.RawMessage, msg *llmcall.Message) error {
	mw := &msg.Wire
	return llmcall.DecodeKnown(raw, mw, map[string]func(json.RawMessage) error{
		"role": func(v json.RawMessage) error {
			var s string
			if err := jsonx.Unmarshal(v, &s); err != nil {
				return err
			}
			msg.Role = llmcall.Role(s)
			return nil
		},
		"content": func(v json.RawMessage) error {
			parts, err := decodeParts(v, false)
			msg.Parts = parts
			return err
		},
	})
}

// decodeSystem decodes the system array: SystemContentBlock is a
// single-member union of text/guardContent/cachePoint; only text is
// modeled, the rest ride as unknown parts.
func decodeSystem(raw json.RawMessage) ([]llmcall.Part, error) {
	var elems []json.RawMessage
	if err := jsonx.Unmarshal(raw, &elems); err != nil {
		return nil, err
	}
	parts := make([]llmcall.Part, 0, len(elems))
	for _, el := range elems {
		key, val, single, err := peelUnionMember(el)
		if err != nil {
			return nil, err
		}
		if !single || key != "text" {
			p, err := llmcall.UnknownPart(el)
			if err != nil {
				return nil, err
			}
			parts = append(parts, p)
			continue
		}
		p := textPart("")
		s, err := llmcall.DecodeShadowString(val, &p.Text.Wire, "text")
		if err != nil {
			return nil, err
		}
		p.Text.Text = s
		parts = append(parts, p)
	}
	return parts, nil
}

// peelUnionMember reads a single-member union block, returning its
// discriminating key and value. single is false when the object has
// zero or multiple members — callers fall back to unknown-part
// handling so unexpected shapes survive preserve mode verbatim.
func peelUnionMember(raw json.RawMessage) (key string, val json.RawMessage, single bool, err error) {
	n := 0
	err = llmcall.DecodeObject(raw, func(k string, v json.RawMessage) error {
		n++
		key, val = k, v
		return nil
	})
	return key, val, n == 1, err
}

func decodeParts(raw json.RawMessage, inToolResult bool) ([]llmcall.Part, error) {
	var elems []json.RawMessage
	if err := jsonx.Unmarshal(raw, &elems); err != nil {
		return nil, err
	}
	parts := make([]llmcall.Part, 0, len(elems))
	for _, el := range elems {
		p, err := decodeBlock(el, inToolResult)
		if err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	return parts, nil
}

func decodeBlock(raw json.RawMessage, inToolResult bool) (llmcall.Part, error) {
	key, val, single, err := peelUnionMember(raw)
	if err != nil {
		return llmcall.Part{}, err
	}
	if !single {
		return llmcall.UnknownPart(raw)
	}
	switch key {
	case "text":
		p := llmcall.Part{Type: llmcall.PartTypeText, Text: &llmcall.TextPart{}}
		s, err := llmcall.DecodeShadowString(val, &p.Text.Wire, "text")
		p.Text.Text = s
		return p, err
	case "json":
		// toolResult-only JSON content. Modeled as a text part holding
		// the compact JSON, with a hint so the bedrock encoder re-emits
		// the {"json": ...} form; foreign encoders see plain text.
		if !inToolResult {
			return llmcall.UnknownPart(raw)
		}
		c, err := llmcall.CompactRaw(val)
		if err != nil {
			return llmcall.Part{}, err
		}
		p := llmcall.Part{Type: llmcall.PartTypeText, Text: &llmcall.TextPart{Text: string(c)}}
		p.Text.Wire.SetHint("bedrock", "json")
		return p, nil
	case "toolUse":
		p := llmcall.Part{Type: llmcall.PartTypeToolCall, ToolCall: &llmcall.ToolCallPart{}}
		pw := &p.ToolCall.Wire
		err := llmcall.DecodeKnown(val, pw, map[string]func(json.RawMessage) error{
			"toolUseId": func(v json.RawMessage) error {
				s, err := llmcall.DecodeShadowString(v, pw, "toolUseId")
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
	case "toolResult":
		p := llmcall.Part{Type: llmcall.PartTypeToolCallResponse, ToolCallResponse: &llmcall.ToolCallResponsePart{}}
		pw := &p.ToolCallResponse.Wire
		err := llmcall.DecodeKnown(val, pw, map[string]func(json.RawMessage) error{
			"toolUseId": func(v json.RawMessage) error {
				s, err := llmcall.DecodeShadowString(v, pw, "toolUseId")
				p.ToolCallResponse.ToolCallID = s
				return err
			},
			"status": func(v json.RawMessage) error {
				var s string
				if err := jsonx.Unmarshal(v, &s); err != nil {
					return err
				}
				p.ToolCallResponse.IsError = s == "error"
				return nil
			},
			"content": func(v json.RawMessage) error {
				parts, err := decodeParts(v, true)
				p.ToolCallResponse.Parts = parts
				return err
			},
		})
		return p, err
	case "image":
		img := &llmcall.ImagePart{}
		var src blockSource
		var format string
		if err := decodeMediaBlock(val, &format, nil, &src); err != nil {
			return llmcall.Part{}, err
		}
		if src.bytes == "" {
			// s3Location sources carry no inline data the IR (or a
			// translation target) could use.
			return llmcall.UnknownPart(raw)
		}
		img.MIMEType = "image/" + format
		img.DataB64 = src.bytes
		c, err := llmcall.CompactRaw(val)
		if err != nil {
			return llmcall.Part{}, err
		}
		// Composite shadow over the whole block body: opaque base64
		// re-encodes verbatim while unmutated, canonically after any
		// field change.
		img.Wire.RecordShadow("image", c, imageShadowVal(img))
		return llmcall.Part{Type: llmcall.PartTypeImage, Image: img}, nil
	case "document":
		f := &llmcall.FilePart{}
		var src blockSource
		var format string
		if err := decodeMediaBlock(val, &format, &f.Name, &src); err != nil {
			return llmcall.Part{}, err
		}
		if src.bytes == "" {
			return llmcall.UnknownPart(raw)
		}
		f.MIMEType = documentMIME(format)
		f.DataB64 = src.bytes
		c, err := llmcall.CompactRaw(val)
		if err != nil {
			return llmcall.Part{}, err
		}
		f.Wire.RecordShadow("document", c, fileShadowVal(f))
		return llmcall.Part{Type: llmcall.PartTypeFile, File: f}, nil
	case "reasoningContent":
		rkey, rval, rsingle, err := peelUnionMember(val)
		if err != nil {
			return llmcall.Part{}, err
		}
		if !rsingle || rkey != "reasoningText" {
			// redactedContent (and future variants) carry nothing the
			// IR models.
			return llmcall.UnknownPart(raw)
		}
		r := &llmcall.ReasoningPart{}
		var rt struct {
			Text      string `json:"text"`
			Signature string `json:"signature"`
		}
		if err := jsonx.Unmarshal(rval, &rt); err != nil {
			return llmcall.Part{}, err
		}
		r.Text = rt.Text
		r.Signature = rt.Signature
		c, err := llmcall.CompactRaw(val)
		if err != nil {
			return llmcall.Part{}, err
		}
		r.Wire.RecordShadow("reasoningContent", c, r.Text+"\x00"+r.Signature)
		return llmcall.Part{Type: llmcall.PartTypeReasoning, Reasoning: r}, nil
	default:
		// video, guardContent, cachePoint, citationsContent, ...
		return llmcall.UnknownPart(raw)
	}
}

// blockSource is the inline-bytes arm of image/document source unions.
type blockSource struct {
	bytes string
}

// decodeMediaBlock reads the shared {format, name?, source{bytes}}
// shape of image and document blocks. Unmodeled source variants
// (s3Location) leave src.bytes empty.
func decodeMediaBlock(raw json.RawMessage, format, name *string, src *blockSource) error {
	var blk struct {
		Format string `json:"format"`
		Name   string `json:"name"`
		Source struct {
			Bytes string `json:"bytes"`
		} `json:"source"`
	}
	if err := jsonx.Unmarshal(raw, &blk); err != nil {
		return err
	}
	*format = blk.Format
	if name != nil {
		*name = blk.Name
	}
	src.bytes = blk.Source.Bytes
	return nil
}

func imageShadowVal(img *llmcall.ImagePart) string {
	return img.MIMEType + "\x00" + img.DataB64 + "\x00" + img.URL
}

func fileShadowVal(f *llmcall.FilePart) string {
	return f.MIMEType + "\x00" + f.Name + "\x00" + f.DataB64 + "\x00" + f.URL
}

// documentMIME maps Converse document formats to MIME types (and back,
// below). The map is lossy for exotic formats — the composite shadow
// keeps same-schema round trips faithful regardless.
var documentMIMEs = map[string]string{
	"pdf":  "application/pdf",
	"csv":  "text/csv",
	"doc":  "application/msword",
	"docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	"xls":  "application/vnd.ms-excel",
	"xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	"html": "text/html",
	"txt":  "text/plain",
	"md":   "text/markdown",
}

func documentMIME(format string) string {
	if m, ok := documentMIMEs[format]; ok {
		return m
	}
	return "application/octet-stream"
}

func documentFormat(mime string) string {
	for f, m := range documentMIMEs {
		if m == mime {
			return f
		}
	}
	return ""
}

func decodeInferenceConfig(raw json.RawMessage, gen *llmcall.GenerationConfig) error {
	gw := &gen.Wire
	return llmcall.DecodeKnown(raw, gw, map[string]func(json.RawMessage) error{
		"maxTokens": func(v json.RawMessage) error {
			n, err := llmcall.DecodeNumber(v)
			gen.MaxTokens = n
			return err
		},
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
		"stopSequences": func(v json.RawMessage) error {
			ss, err := llmcall.DecodeShadowStringSlice(v, gw, "stopSequences")
			gen.StopSequences = ss
			return err
		},
	})
}

// decodeToolConfig decodes the nested toolConfig node into the IR's
// flat Tools/ToolChoice and records a composite shadow of the whole
// subtree on the root Wire: the IR has no slot for the node's own
// bookkeeping, so an unmutated round trip re-emits the source bytes
// verbatim and any mutation falls back to canonical assembly. Unknown
// tool entries (cachePoint) and node extras survive only through the
// shadow; their counts ride hints so strip mode reports them dropped.
func decodeToolConfig(raw json.RawMessage, w *llmcall.Wire, req *llmcall.Request) error {
	unknownTools := 0
	var tmp llmcall.Wire
	err := llmcall.DecodeKnown(raw, &tmp, map[string]func(json.RawMessage) error{
		"tools": func(v json.RawMessage) error {
			var elems []json.RawMessage
			if err := jsonx.Unmarshal(v, &elems); err != nil {
				return err
			}
			for _, el := range elems {
				key, val, single, err := peelUnionMember(el)
				if err != nil {
					return err
				}
				if !single || key != "toolSpec" {
					unknownTools++
					continue
				}
				td, err := decodeToolSpec(val)
				if err != nil {
					return err
				}
				req.Tools = append(req.Tools, td)
			}
			return nil
		},
		"toolChoice": func(v json.RawMessage) error {
			tc, err := decodeToolChoice(v)
			req.ToolChoice = tc
			return err
		},
	})
	if err != nil {
		return err
	}
	c, err := llmcall.CompactRaw(raw)
	if err != nil {
		return err
	}
	w.RecordShadow("toolConfig", c, toolConfigShadowVal(req.Tools, req.ToolChoice))
	if n := unknownTools + len(tmp.Extras); n > 0 {
		w.SetHint("toolConfigUnmodeled", strconv.Itoa(n))
	}
	return nil
}

func decodeToolSpec(raw json.RawMessage) (llmcall.ToolDefinition, error) {
	var td llmcall.ToolDefinition
	tw := &td.Wire
	err := llmcall.DecodeKnown(raw, tw, map[string]func(json.RawMessage) error{
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
		"inputSchema": func(v json.RawMessage) error {
			// inputSchema is a {"json": <schema>} union; the schema
			// subtree becomes Parameters and the whole node rides a
			// shadow for byte fidelity.
			var probe map[string]json.RawMessage
			if err := jsonx.Unmarshal(v, &probe); err != nil {
				return err
			}
			if jv, ok := probe["json"]; ok {
				c, err := llmcall.CompactRaw(jv)
				if err != nil {
					return err
				}
				td.Parameters = c
			}
			c, err := llmcall.CompactRaw(v)
			if err != nil {
				return err
			}
			tw.RecordShadow("inputSchema", c, string(td.Parameters))
			return nil
		},
	})
	return td, err
}

// decodeToolChoice reads the {"auto":{}}/{"any":{}}/{"tool":{name}}
// union. Unknown variants keep the verbatim block in Wire.Raw with no
// mode, re-emitted in preserve and dropped (counted) in strip.
func decodeToolChoice(raw json.RawMessage) (*llmcall.ToolChoice, error) {
	key, val, single, err := peelUnionMember(raw)
	if err != nil {
		return nil, err
	}
	tc := &llmcall.ToolChoice{}
	if !single {
		c, err := llmcall.CompactRaw(raw)
		if err != nil {
			return nil, err
		}
		tc.Wire.Raw = c
		return tc, nil
	}
	switch key {
	case "auto":
		tc.Mode = llmcall.ToolChoiceAuto
	case "any":
		tc.Mode = llmcall.ToolChoiceRequired
	case "tool":
		tc.Mode = llmcall.ToolChoiceNamed
		tw := &tc.Wire
		if err := llmcall.DecodeKnown(val, tw, map[string]func(json.RawMessage) error{
			"name": func(v json.RawMessage) error {
				s, err := llmcall.DecodeShadowString(v, tw, "name")
				tc.Name = s
				return err
			},
		}); err != nil {
			return nil, err
		}
	default:
		c, err := llmcall.CompactRaw(raw)
		if err != nil {
			return nil, err
		}
		tc.Wire.Raw = c
	}
	return tc, nil
}

func toolConfigShadowVal(tools []llmcall.ToolDefinition, tc *llmcall.ToolChoice) string {
	var b strings.Builder
	for i := range tools {
		b.WriteString(tools[i].Name)
		b.WriteByte(0)
		b.WriteString(tools[i].Description)
		b.WriteByte(0)
		b.Write(tools[i].Parameters)
		b.WriteByte(1)
	}
	if tc != nil {
		b.WriteString(string(tc.Mode))
		b.WriteByte(0)
		b.WriteString(tc.Name)
	}
	return b.String()
}

func (c codec) EncodeRequest(req *llmcall.Request, opts llmcall.EncodeOptions) (*llmcall.EncodedRequest, error) {
	out := &llmcall.EncodedRequest{}
	w := &req.Wire
	mode := opts.Mode
	dropped := &out.DroppedExtras

	emit := map[string]llmcall.FieldEmitter{
		"messages": func(e *llmcall.ObjectEncoder) {
			raw, err := encodeMessages(req.Messages, mode, dropped)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("messages", raw)
		},
		"system": func(e *llmcall.ObjectEncoder) {
			if len(req.System) == 0 && !w.HasKey("system") {
				return
			}
			raw, err := encodeSystem(req.System, mode, dropped)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("system", raw)
		},
		"toolConfig": func(e *llmcall.ObjectEncoder) {
			// Preserve mode re-emits the whole source subtree while the
			// flattened IR is unmutated — the node's own member order,
			// unknown tool entries, and extras all ride the composite
			// shadow. Strip (and mutated preserve) assembles canonically
			// from the IR, losing the unmodeled pieces; strip counts
			// them via the decode-time hint.
			if mode == llmcall.ModePreserve {
				if raw, ok := w.ShadowRaw("toolConfig", toolConfigShadowVal(req.Tools, req.ToolChoice)); ok {
					e.Raw("toolConfig", raw)
					return
				}
			}
			if len(req.Tools) == 0 && req.ToolChoice == nil {
				return
			}
			raw, err := encodeToolConfig(req, mode, dropped)
			if err != nil {
				e.Fail(err)
				return
			}
			if mode == llmcall.ModeStrip {
				if n, err := strconv.Atoi(w.Hint("toolConfigUnmodeled")); err == nil {
					*dropped += n
				}
			}
			e.Raw("toolConfig", raw)
		},
		"inferenceConfig": func(e *llmcall.ObjectEncoder) {
			g := &req.Generation
			hasValues := g.MaxTokens != "" || g.Temperature != "" || g.TopP != "" || len(g.StopSequences) > 0
			if !hasValues && !w.HasKey("inferenceConfig") {
				return
			}
			raw, err := encodeInferenceConfig(g, mode, dropped)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("inferenceConfig", raw)
		},
	}
	canonical := []string{"messages", "system", "toolConfig", "inferenceConfig"}

	// topK has no inferenceConfig member (Converse routes it through
	// additionalModelRequestFields); a cross-schema source that set it
	// loses it, counted.
	if mode == llmcall.ModeStrip && req.Generation.TopK != "" {
		*dropped++
	}

	fresh, err := llmcall.EncodeOrderedObject(w, mode, emit, canonical, dropped)
	if err != nil {
		return nil, err
	}
	if mode == llmcall.ModePreserve {
		out.Body, out.Exact = llmcall.EncodeRaw(fresh, w.Raw)
	} else {
		out.Body = fresh
	}

	op := "converse"
	if req.Stream {
		op = "converse-stream"
	}
	out.Path = "/model/" + url.PathEscape(req.Model) + "/" + op
	if mode == llmcall.ModePreserve && w.Path != "" {
		out.Path = w.Path
	}
	out.SetHeaders = map[string]string{"content-type": "application/json"}
	return out, nil
}

func encodeMessages(msgs []llmcall.Message, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	elems := make([]json.RawMessage, 0, len(msgs))
	for i := range msgs {
		b, err := encodeMessage(&msgs[i], mode, dropped)
		if err != nil {
			return nil, err
		}
		elems = append(elems, b)
	}
	return llmcall.EncodeArray(elems), nil
}

func encodeMessage(msg *llmcall.Message, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	mw := &msg.Wire
	emit := map[string]llmcall.FieldEmitter{
		"role": func(e *llmcall.ObjectEncoder) {
			role := string(msg.Role)
			// Converse has only user/assistant roles; tool results ride
			// inside user messages (cross-schema sources produce the
			// tool role, bedrock decodes never do).
			if msg.Role == llmcall.RoleTool || msg.Role == "" {
				role = string(llmcall.RoleUser)
			}
			e.Field("role", role)
		},
		"content": func(e *llmcall.ObjectEncoder) {
			raw, err := encodeParts(msg.Parts, mode, dropped, false)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("content", raw)
		},
	}
	return llmcall.EncodeOrderedObject(mw, mode, emit, []string{"role", "content"}, dropped)
}

func encodeSystem(parts []llmcall.Part, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	elems := make([]json.RawMessage, 0, len(parts))
	for i := range parts {
		p := &parts[i]
		switch p.Type {
		case llmcall.PartTypeText:
			e := llmcall.NewObjectEncoder()
			llmcall.EmitShadowString(e, &p.Text.Wire, "text", p.Text.Text)
			b, err := e.Bytes()
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

func encodeParts(parts []llmcall.Part, mode llmcall.Mode, dropped *int, inToolResult bool) (json.RawMessage, error) {
	elems := make([]json.RawMessage, 0, len(parts))
	for i := range parts {
		b, err := encodeBlock(&parts[i], mode, dropped, inToolResult)
		if err != nil {
			return nil, err
		}
		if b != nil {
			elems = append(elems, b)
		}
	}
	return llmcall.EncodeArray(elems), nil
}

func encodeBlock(p *llmcall.Part, mode llmcall.Mode, dropped *int, inToolResult bool) (json.RawMessage, error) {
	switch p.Type {
	case llmcall.PartTypeText:
		t := p.Text
		e := llmcall.NewObjectEncoder()
		if inToolResult && t.Wire.Hint("bedrock") == "json" {
			// Round-trip the {"json": ...} toolResult form when the
			// text still holds valid JSON; mutated-to-non-JSON content
			// degrades to a text block.
			if json.Valid([]byte(t.Text)) {
				e.Raw("json", json.RawMessage(t.Text))
				return e.Bytes()
			}
		}
		llmcall.EmitShadowString(e, &t.Wire, "text", t.Text)
		return e.Bytes()
	case llmcall.PartTypeToolCall:
		tc := p.ToolCall
		tw := &tc.Wire
		inner, err := llmcall.EncodeOrderedObject(tw, mode, map[string]llmcall.FieldEmitter{
			"toolUseId": func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowString(e, tw, "toolUseId", tc.ID) },
			"name":      func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowString(e, tw, "name", tc.Name) },
			"input": func(e *llmcall.ObjectEncoder) {
				if len(tc.Arguments) == 0 {
					e.Raw("input", json.RawMessage("{}"))
					return
				}
				e.Raw("input", tc.Arguments)
			},
		}, []string{"toolUseId", "name", "input"}, dropped)
		if err != nil {
			return nil, err
		}
		return wrapUnion("toolUse", inner)
	case llmcall.PartTypeToolCallResponse:
		tr := p.ToolCallResponse
		tw := &tr.Wire
		inner, err := llmcall.EncodeOrderedObject(tw, mode, map[string]llmcall.FieldEmitter{
			"toolUseId": func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowString(e, tw, "toolUseId", tr.ToolCallID) },
			"status": func(e *llmcall.ObjectEncoder) {
				if !tr.IsError && !tw.HasKey("status") {
					return
				}
				s := "success"
				if tr.IsError {
					s = "error"
				}
				e.Field("status", s)
			},
			"content": func(e *llmcall.ObjectEncoder) {
				raw, err := encodeParts(tr.Parts, mode, dropped, true)
				if err != nil {
					e.Fail(err)
					return
				}
				e.Raw("content", raw)
			},
		}, []string{"toolUseId", "content", "status"}, dropped)
		if err != nil {
			return nil, err
		}
		return wrapUnion("toolResult", inner)
	case llmcall.PartTypeImage:
		img := p.Image
		if raw, ok := img.Wire.ShadowRaw("image", imageShadowVal(img)); ok {
			return wrapUnion("image", raw)
		}
		mime, data := img.MIMEType, img.DataB64
		if data == "" && img.URL != "" {
			// Inline data URLs (OpenAI's form) unpack; remote URLs have
			// no Converse representation.
			m, d, ok := llmcall.SplitDataURL(img.URL)
			if !ok {
				*dropped++
				return nil, nil
			}
			mime, data = m, d
		}
		if data == "" {
			*dropped++
			return nil, nil
		}
		inner, err := encodeMediaBlock(strings.TrimPrefix(mime, "image/"), "", data)
		if err != nil {
			return nil, err
		}
		return wrapUnion("image", inner)
	case llmcall.PartTypeFile:
		f := p.File
		if raw, ok := f.Wire.ShadowRaw("document", fileShadowVal(f)); ok {
			return wrapUnion("document", raw)
		}
		mime, data := f.MIMEType, f.DataB64
		if data == "" && f.URL != "" {
			m, d, ok := llmcall.SplitDataURL(f.URL)
			if !ok {
				*dropped++
				return nil, nil
			}
			mime, data = m, d
		}
		format := documentFormat(mime)
		if data == "" || format == "" {
			*dropped++
			return nil, nil
		}
		name := f.Name
		if name == "" {
			// DocumentBlock requires a name; sources without one get a
			// neutral placeholder.
			name = "document"
		}
		inner, err := encodeMediaBlock(format, name, data)
		if err != nil {
			return nil, err
		}
		return wrapUnion("document", inner)
	case llmcall.PartTypeReasoning:
		r := p.Reasoning
		if raw, ok := r.Wire.ShadowRaw("reasoningContent", r.Text+"\x00"+r.Signature); ok {
			return wrapUnion("reasoningContent", raw)
		}
		rt := llmcall.NewObjectEncoder()
		rt.Field("text", r.Text)
		if r.Signature != "" {
			rt.Field("signature", r.Signature)
		}
		rb, err := rt.Bytes()
		if err != nil {
			return nil, err
		}
		inner, err := wrapUnion("reasoningText", rb)
		if err != nil {
			return nil, err
		}
		return wrapUnion("reasoningContent", inner)
	case llmcall.PartTypeUnknown:
		if mode == llmcall.ModePreserve && len(p.Wire.Raw) > 0 {
			return p.Wire.Raw, nil
		}
		*dropped++
		return nil, nil
	default:
		// audio, refusal, citation: no Converse block; drop with count.
		*dropped++
		return nil, nil
	}
}

// wrapUnion wraps an encoded inner object as {key: inner} — the
// Converse single-member union shape.
func wrapUnion(key string, inner json.RawMessage) (json.RawMessage, error) {
	e := llmcall.NewObjectEncoder()
	e.Raw(key, inner)
	return e.Bytes()
}

func encodeMediaBlock(format, name, dataB64 string) (json.RawMessage, error) {
	e := llmcall.NewObjectEncoder()
	e.Field("format", format)
	if name != "" {
		e.Field("name", name)
	}
	src := llmcall.NewObjectEncoder()
	src.Field("bytes", dataB64)
	sb, err := src.Bytes()
	if err != nil {
		return nil, err
	}
	e.Raw("source", sb)
	return e.Bytes()
}

func encodeToolConfig(req *llmcall.Request, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	e := llmcall.NewObjectEncoder()
	if len(req.Tools) > 0 {
		elems := make([]json.RawMessage, 0, len(req.Tools))
		for i := range req.Tools {
			b, err := encodeToolSpec(&req.Tools[i], mode, dropped)
			if err != nil {
				return nil, err
			}
			if b != nil {
				elems = append(elems, b)
			}
		}
		e.Raw("tools", llmcall.EncodeArray(elems))
	}
	if tc := req.ToolChoice; tc != nil {
		raw, err := encodeToolChoice(tc, mode, dropped)
		if err != nil {
			return nil, err
		}
		if raw != nil {
			e.Raw("toolChoice", raw)
		}
	}
	return e.Bytes()
}

func encodeToolSpec(td *llmcall.ToolDefinition, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	tw := &td.Wire
	emit := map[string]llmcall.FieldEmitter{
		"name": func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowString(e, tw, "name", td.Name) },
		"description": func(e *llmcall.ObjectEncoder) {
			if td.Description == "" && !tw.HasKey("description") {
				return
			}
			llmcall.EmitShadowString(e, tw, "description", td.Description)
		},
		"inputSchema": func(e *llmcall.ObjectEncoder) {
			if raw, ok := tw.ShadowRaw("inputSchema", string(td.Parameters)); ok {
				e.Raw("inputSchema", raw)
				return
			}
			params := td.Parameters
			if len(params) == 0 {
				params = json.RawMessage(`{"type":"object"}`)
			}
			wrapped, err := wrapUnion("json", params)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("inputSchema", wrapped)
		},
	}
	inner, err := llmcall.EncodeOrderedObject(tw, mode, emit, []string{"name", "description", "inputSchema"}, dropped)
	if err != nil {
		return nil, err
	}
	return wrapUnion("toolSpec", inner)
}

func encodeToolChoice(tc *llmcall.ToolChoice, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	switch tc.Mode {
	case llmcall.ToolChoiceAuto:
		return json.RawMessage(`{"auto":{}}`), nil
	case llmcall.ToolChoiceRequired:
		return json.RawMessage(`{"any":{}}`), nil
	case llmcall.ToolChoiceNamed:
		tw := &tc.Wire
		inner, err := llmcall.EncodeOrderedObject(tw, mode, map[string]llmcall.FieldEmitter{
			"name": func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowString(e, tw, "name", tc.Name) },
		}, []string{"name"}, dropped)
		if err != nil {
			return nil, err
		}
		return wrapUnion("tool", inner)
	case llmcall.ToolChoiceNone:
		// Converse has no "none"; omitting toolChoice is the closest
		// semantic (the model decides, tools stay visible).
		*dropped++
		return nil, nil
	default:
		if len(tc.Wire.Raw) > 0 {
			if mode == llmcall.ModePreserve {
				return tc.Wire.Raw, nil
			}
			*dropped++
			return nil, nil
		}
		*dropped++
		return nil, nil
	}
}

func encodeInferenceConfig(gen *llmcall.GenerationConfig, mode llmcall.Mode, dropped *int) (json.RawMessage, error) {
	gw := &gen.Wire
	emit := map[string]llmcall.FieldEmitter{
		"maxTokens":   numberEmitter("maxTokens", &gen.MaxTokens),
		"temperature": numberEmitter("temperature", &gen.Temperature),
		"topP":        numberEmitter("topP", &gen.TopP),
		"stopSequences": func(e *llmcall.ObjectEncoder) {
			if len(gen.StopSequences) == 0 && !gw.HasKey("stopSequences") {
				return
			}
			if raw, ok := gw.ShadowRaw("stopSequences", llmcall.StringSliceShadowVal(gen.StopSequences)); ok {
				e.Raw("stopSequences", raw)
				return
			}
			e.Field("stopSequences", gen.StopSequences)
		},
	}
	return llmcall.EncodeOrderedObject(gw, mode, emit, []string{"maxTokens", "temperature", "topP", "stopSequences"}, dropped)
}

func numberEmitter(key string, n *json.Number) llmcall.FieldEmitter {
	return func(e *llmcall.ObjectEncoder) {
		if *n == "" {
			return
		}
		e.Field(key, *n)
	}
}

func (codec) DecodeResponse(in llmcall.ResponseInput, req *llmcall.Request) (*llmcall.Response, error) {
	if in.Status != 0 && in.Status != 200 {
		return nil, fmt.Errorf("status %d: %w", in.Status, llmcall.ErrUnsupported)
	}
	if llmcall.HasContentTypePrefix(in.Headers, "application/vnd.amazon.eventstream") {
		return nil, fmt.Errorf("event-stream response: %w", llmcall.ErrUnsupported)
	}

	resp := &llmcall.Response{Provider: "aws_bedrock"}
	resp.Wire.Raw = json.RawMessage(in.Body)
	if req != nil {
		// Converse responses don't echo the model; carry the request's.
		resp.Model = req.Model
	}
	choice := llmcall.Choice{Message: llmcall.Message{Role: llmcall.RoleAssistant}}

	w := &resp.Wire
	err := llmcall.DecodeKnown(in.Body, w, map[string]func(json.RawMessage) error{
		"output": func(v json.RawMessage) error {
			cw := &choice.Wire
			return llmcall.DecodeKnown(v, cw, map[string]func(json.RawMessage) error{
				"message": func(mv json.RawMessage) error {
					return decodeMessage(mv, &choice.Message)
				},
			})
		},
		"stopReason": func(v json.RawMessage) error {
			var s string
			if err := jsonx.Unmarshal(v, &s); err != nil {
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
	case "guardrail_intervened", "content_filtered":
		return llmcall.FinishReasonContentFilter
	default:
		return ""
	}
}

func bedrockFinish(c llmcall.Choice) string {
	if c.FinishReasonRaw != "" {
		return c.FinishReasonRaw
	}
	switch c.FinishReason {
	case llmcall.FinishReasonLength:
		return "max_tokens"
	case llmcall.FinishReasonToolCalls:
		return "tool_use"
	case llmcall.FinishReasonContentFilter:
		return "content_filtered"
	default:
		return "end_turn"
	}
}

func decodeUsage(raw json.RawMessage, u *llmcall.Usage) error {
	uw := &u.Wire
	return llmcall.DecodeKnown(raw, uw, map[string]func(json.RawMessage) error{
		"inputTokens": func(v json.RawMessage) error {
			n, err := llmcall.DecodeShadowInt64(v, uw, "inputTokens")
			u.InputTokens = n
			return err
		},
		"outputTokens": func(v json.RawMessage) error {
			n, err := llmcall.DecodeShadowInt64(v, uw, "outputTokens")
			u.OutputTokens = n
			return err
		},
		"totalTokens": func(v json.RawMessage) error {
			// Modeled only as a verbatim shadow: AWS computes the total
			// (cache accounting included), so re-emitting the source
			// bytes is the one faithful option. The "" sentinel makes
			// the shadow unconditional — there is no IR field whose
			// mutation should invalidate it.
			c, err := llmcall.CompactRaw(v)
			if err != nil {
				return err
			}
			uw.RecordShadow("totalTokens", c, "")
			return nil
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
		"output": func(e *llmcall.ObjectEncoder) {
			cw := &choice.Wire
			raw, err := llmcall.EncodeOrderedObject(cw, mode, map[string]llmcall.FieldEmitter{
				"message": func(me *llmcall.ObjectEncoder) {
					mb, err := encodeMessage(&choice.Message, mode, dropped)
					if err != nil {
						me.Fail(err)
						return
					}
					me.Raw("message", mb)
				},
			}, []string{"message"}, dropped)
			if err != nil {
				e.Fail(err)
				return
			}
			e.Raw("output", raw)
		},
		"stopReason": func(e *llmcall.ObjectEncoder) {
			e.Field("stopReason", bedrockFinish(choice))
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
	canonical := []string{"output", "stopReason", "usage"}

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
		"inputTokens":  func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowInt64(e, uw, "inputTokens", u.InputTokens) },
		"outputTokens": func(e *llmcall.ObjectEncoder) { llmcall.EmitShadowInt64(e, uw, "outputTokens", u.OutputTokens) },
		"totalTokens": func(e *llmcall.ObjectEncoder) {
			if raw, ok := uw.ShadowRaw("totalTokens", ""); ok {
				e.Raw("totalTokens", raw)
				return
			}
			e.Field("totalTokens", u.InputTokens+u.OutputTokens)
		},
	}
	return llmcall.EncodeOrderedObject(uw, mode, emit, []string{"inputTokens", "outputTokens", "totalTokens"}, dropped)
}
