package llmcall

import (
	"fmt"
	"slices"
	"strings"
)

// Translation blocker reasons returned by TranslationBlockers. Values
// are stable identifiers surfaced on telemetry (skipped-candidate
// accounting), not user-facing prose.
const (
	BlockerNoCodec     = "no_codec"
	BlockerStreaming   = "streaming"
	BlockerOperation   = "operation"
	BlockerTools       = "tools"
	BlockerToolResults = "tool_results"
	BlockerImageInput  = "image_input"
	BlockerAudioInput  = "audio_input"
	BlockerFileInput   = "file_input"
	BlockerReasoning   = "reasoning"
	BlockerSystem      = "system"
)

// TranslationBlockers reports why a decoded request cannot be
// translated from src's wire schema to tgt's. An empty result means
// the pair is translatable for this request. Backend selection calls
// this per candidate to filter cross-schema backends; the blocker
// strings feed the skipped-candidate telemetry.
//
// The checks compare the features the request actually uses against
// the target's Capabilities (which are honest to the codec, not the
// provider's API surface), plus two pair-specific gates: streaming
// (stream codecs are APO-743) and tool results through the Gemini
// schema, whose codec keys results by function name and stashes
// payloads in codec-private hints — lossy in either direction until
// Phase 3 models them fully.
func TranslationBlockers(req *Request, src, tgt *Provider) []string {
	if req == nil || src == nil || src.Codec == nil || tgt == nil || tgt.Codec == nil {
		return []string{BlockerNoCodec}
	}

	var blockers []string
	add := func(b string) {
		if !slices.Contains(blockers, b) {
			blockers = append(blockers, b)
		}
	}

	if req.Stream {
		add(BlockerStreaming)
	}
	if req.Operation != "" && !slices.Contains(tgt.Capabilities.Operations, req.Operation) {
		add(BlockerOperation)
	}
	if (len(req.Tools) > 0 || req.ToolChoice != nil) && !tgt.Capabilities.Tools {
		add(BlockerTools)
	}

	googleSide := src.Name == "google_genai" || tgt.Name == "google_genai"
	var walk func(parts []Part)
	walk = func(parts []Part) {
		for i := range parts {
			p := &parts[i]
			switch p.Type {
			case PartTypeImage:
				if !tgt.Capabilities.ImageInput {
					add(BlockerImageInput)
				}
			case PartTypeAudio:
				if !tgt.Capabilities.AudioInput {
					add(BlockerAudioInput)
				}
			case PartTypeFile:
				if !tgt.Capabilities.FileInput {
					add(BlockerFileInput)
				}
			case PartTypeReasoning:
				if !tgt.Capabilities.Reasoning {
					add(BlockerReasoning)
				}
			case PartTypeToolCall:
				if !tgt.Capabilities.Tools {
					add(BlockerTools)
				}
			case PartTypeToolCallResponse:
				if !tgt.Capabilities.Tools {
					add(BlockerTools)
				}
				if googleSide {
					add(BlockerToolResults)
				}
				if p.ToolCallResponse != nil {
					walk(p.ToolCallResponse.Parts)
				}
			}
		}
	}
	hasSystem := len(req.System) > 0
	walk(req.System)
	for i := range req.Messages {
		if req.Messages[i].Role == RoleSystem {
			hasSystem = true
		}
		walk(req.Messages[i].Parts)
	}
	if hasSystem && !tgt.Capabilities.SystemRole {
		add(BlockerSystem)
	}
	return blockers
}

// LiftSystemMessages moves RoleSystem turns out of Messages into
// req.System, appending their parts in conversation order. OpenAI
// carries system content as positional messages while anthropic and
// Gemini carry a top-level field and reject a system role inside the
// message list, so translation lifts before encoding to either.
// OpenAI-decoded same-schema round trips never call this; the openai
// encoder re-emits a populated req.System as a leading system message.
//
// Mutates req in place — translation runs it on a DeepCopy.
func LiftSystemMessages(req *Request) {
	if req == nil {
		return
	}
	moved := false
	kept := make([]Message, 0, len(req.Messages))
	for i := range req.Messages {
		if req.Messages[i].Role == RoleSystem {
			req.System = append(req.System, req.Messages[i].Parts...)
			moved = true
			continue
		}
		kept = append(kept, req.Messages[i])
	}
	if moved {
		req.Messages = kept
	}
}

// EnsureToolCallIDs assigns deterministic IDs (call_1, call_2, ... in
// walk order) to tool calls decoded from schemas that don't carry one
// — Gemini's functionCall is name-keyed. anthropic/openai targets
// require an id on every assistant tool call. Tool results need no
// matching pass: name-keyed results only arise from Gemini decodes,
// and TranslationBlockers refuses tool results on any Gemini-side
// pair until the codec models them fully (Phase 3).
//
// Mutates req in place — translation runs it on a DeepCopy.
func EnsureToolCallIDs(req *Request) {
	if req == nil {
		return
	}
	n := 0
	for mi := range req.Messages {
		for pi := range req.Messages[mi].Parts {
			p := &req.Messages[mi].Parts[pi]
			if p.Type == PartTypeToolCall && p.ToolCall != nil && p.ToolCall.ID == "" {
				n++
				p.ToolCall.ID = fmt.Sprintf("call_%d", n)
			}
		}
	}
}

// StripRequestForTranslation removes the source schema's wire
// bookkeeping from a decoded request so a different provider's codec
// can encode it cleanly, and drops content the target cannot receive.
// Returns the number of source artifacts discarded (unmodeled extras
// across every node plus unmodeled provider blocks), which feeds the
// dropped-extras telemetry.
//
// Stripping is what makes cross-codec encoding sound: Wire carries
// codec-PRIVATE conventions — hint keys collide across codecs
// ("content", "system"), KeyOrder/Extras hold source-schema members,
// and shadows hold source-schema bytes. A target encoder walking them
// would interleave another schema's fields into its output. Zeroed
// wires put every encoder on its canonical programmatic path.
//
// Mutates req in place — translation runs it on a DeepCopy, after
// reading anything it needs from the source wire (header names, the
// original path).
func StripRequestForTranslation(req *Request) int {
	if req == nil {
		return 0
	}
	dropped := stripWire(&req.Wire)
	dropped += stripWire(&req.Generation.Wire)
	if req.ToolChoice != nil {
		dropped += stripWire(&req.ToolChoice.Wire)
	}
	for i := range req.Tools {
		dropped += stripWire(&req.Tools[i].Wire)
	}
	req.System, dropped = stripParts(req.System, dropped)
	for i := range req.Messages {
		dropped += stripWire(&req.Messages[i].Wire)
		req.Messages[i].Parts, dropped = stripParts(req.Messages[i].Parts, dropped)
	}
	return dropped
}

// StripResponseForTranslation is StripRequestForTranslation for the
// response direction (backend-schema decode re-encoded in the client's
// schema).
func StripResponseForTranslation(resp *Response) int {
	if resp == nil {
		return 0
	}
	dropped := stripWire(&resp.Wire)
	dropped += stripWire(&resp.Usage.Wire)
	for i := range resp.Choices {
		ch := &resp.Choices[i]
		// FinishReasonRaw is SOURCE-schema vocabulary (it exists to
		// round-trip lossy canonical mappings); every encoder prefers
		// it over the canonical value, so leaving it set would leak
		// "end_turn" into an openai finish_reason. Cleared, the target
		// encoder's canonical->wire map takes over.
		ch.FinishReasonRaw = ""
		dropped += stripWire(&ch.Wire)
		dropped += stripWire(&ch.Message.Wire)
		ch.Message.Parts, dropped = stripParts(ch.Message.Parts, dropped)
	}
	return dropped
}

// DataURL composes an RFC 2397 data URL from a MIME type and base64
// payload — the inline-image form OpenAI's image_url accepts.
func DataURL(mime, dataB64 string) string {
	return "data:" + mime + ";base64," + dataB64
}

// SplitDataURL parses a base64 data URL back into its MIME type and
// payload. ok is false for any other URL shape (including non-base64
// data URLs), in which case the caller should treat the value as a
// fetchable reference.
func SplitDataURL(url string) (mime, dataB64 string, ok bool) {
	rest, found := strings.CutPrefix(url, "data:")
	if !found {
		return "", "", false
	}
	meta, payload, found := strings.Cut(rest, ",")
	if !found {
		return "", "", false
	}
	meta, found = strings.CutSuffix(meta, ";base64")
	if !found {
		return "", "", false
	}
	return meta, payload, true
}

// stripWire zeroes one node's bookkeeping, returning how many
// unmodeled extras it carried.
func stripWire(w *Wire) int {
	n := len(w.Extras)
	*w = Wire{}
	return n
}

// stripParts drops unmodeled provider blocks (PartTypeUnknown — their
// only content is the source-schema Wire.Raw) and zeroes the wires of
// every typed part, recursing into tool-result content.
func stripParts(parts []Part, dropped int) ([]Part, int) {
	kept := make([]Part, 0, len(parts))
	for i := range parts {
		p := parts[i]
		if p.Type == PartTypeUnknown {
			dropped++
			continue
		}
		dropped += stripWire(&p.Wire)
		switch {
		case p.Text != nil:
			dropped += stripWire(&p.Text.Wire)
		case p.Image != nil:
			dropped += stripWire(&p.Image.Wire)
		case p.Audio != nil:
			dropped += stripWire(&p.Audio.Wire)
		case p.File != nil:
			dropped += stripWire(&p.File.Wire)
		case p.ToolCall != nil:
			dropped += stripWire(&p.ToolCall.Wire)
		case p.ToolCallResponse != nil:
			dropped += stripWire(&p.ToolCallResponse.Wire)
			p.ToolCallResponse.Parts, dropped = stripParts(p.ToolCallResponse.Parts, dropped)
		case p.Reasoning != nil:
			dropped += stripWire(&p.Reasoning.Wire)
		case p.Refusal != nil:
			dropped += stripWire(&p.Refusal.Wire)
		case p.Citation != nil:
			dropped += stripWire(&p.Citation.Wire)
		}
		kept = append(kept, p)
	}
	return kept, dropped
}
