package llmcall

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

// This file holds the shared machinery provider codecs build their
// Decode/Encode walks from. The invariant all helpers enforce: every
// byte stored in Wire bookkeeping (extras, shadows, unknown-part raws)
// is COMPACTED at capture time. Fresh encodes are compact throughout,
// so the root passthrough gate — fresh == CompactJSON(Wire.Raw) — can
// hold even when the original body was pretty-printed; only root
// Wire.Raw keeps original formatting, because it is the passthrough's
// output.

// HasKey reports whether key was present in the node's original member
// sequence. Emitters use it for presence semantics: a modeled key in
// KeyOrder re-emits even at its zero value (e.g. "stream": false),
// while a key absent from it emits only when meaningfully set.
func (w *Wire) HasKey(key string) bool {
	for _, k := range w.KeyOrder {
		if k == key {
			return true
		}
	}
	return false
}

// DecodeKnown walks raw's object members in source order, recording
// KeyOrder on w, dispatching modeled keys to their handler, and
// capturing everything else as a compacted extra.
func DecodeKnown(raw json.RawMessage, w *Wire, known map[string]func(value json.RawMessage) error) error {
	return DecodeObject(raw, func(key string, value json.RawMessage) error {
		w.KeyOrder = append(w.KeyOrder, key)
		if fn, ok := known[key]; ok {
			return fn(value)
		}
		compacted, err := CompactJSON(value)
		if err != nil {
			return err
		}
		w.AddExtra(key, compacted)
		return nil
	})
}

// DecodeShadowString unmarshals a JSON string value and records a
// shadow on w when the canonical re-marshal differs from the source
// bytes (\uXXXX escapes and friends).
func DecodeShadowString(value json.RawMessage, w *Wire, key string) (string, error) {
	var s string
	if err := json.Unmarshal(value, &s); err != nil {
		return "", err
	}
	if fresh, err := MarshalCompact(s); err == nil && !bytes.Equal(fresh, value) {
		w.RecordShadow(key, value, s)
	}
	return s, nil
}

// DecodeShadowInt64 unmarshals an integer value, shadowing exotic
// literals ("12.0", "1e2") that wouldn't re-marshal identically.
func DecodeShadowInt64(value json.RawMessage, w *Wire, key string) (int64, error) {
	var n json.Number
	if err := json.Unmarshal(value, &n); err != nil {
		return 0, err
	}
	i, err := n.Int64()
	if err != nil {
		// Not integral (e.g. "12.7") — keep the truncated parse but
		// shadow the literal so preserve mode stays faithful.
		f, ferr := n.Float64()
		if ferr != nil {
			return 0, ferr
		}
		i = int64(f)
	}
	if string(n) != strconv.FormatInt(i, 10) {
		w.RecordShadow(key, json.RawMessage(n), i)
	}
	return i, nil
}

// DecodeNumber unmarshals any JSON number as its literal. json.Number
// re-marshals byte-identically, so numbers stored this way never need
// shadows.
func DecodeNumber(value json.RawMessage) (json.Number, error) {
	var n json.Number
	err := json.Unmarshal(value, &n)
	return n, err
}

// DecodeShadowStringSlice unmarshals a JSON string array, recording a
// whole-value shadow keyed on the joined contents.
func DecodeShadowStringSlice(value json.RawMessage, w *Wire, key string) ([]string, error) {
	var ss []string
	if err := json.Unmarshal(value, &ss); err != nil {
		return nil, err
	}
	compacted, err := CompactJSON(value)
	if err != nil {
		return nil, err
	}
	w.RecordShadow(key, compacted, StringSliceShadowVal(ss))
	return ss, nil
}

// StringSliceShadowVal is the comparable composite for string-slice
// shadows; encoders recompute it to detect mutation.
func StringSliceShadowVal(ss []string) string {
	return strings.Join(ss, "\x00")
}

// CompactRaw compacts a captured subtree for Wire storage, per the
// file invariant.
func CompactRaw(value json.RawMessage) (json.RawMessage, error) {
	c, err := CompactJSON(value)
	return json.RawMessage(c), err
}

// FieldEmitter writes one modeled key's member(s) to e. Emitters check
// presence themselves and may emit nothing.
type FieldEmitter func(e *ObjectEncoder)

// EncodeOrderedObject assembles a node: in preserve mode it walks
// Wire.KeyOrder, emitting modeled keys via their emitter and extras
// verbatim, then appends modeled keys missing from KeyOrder (added
// after decode) in canonical order; in strip mode it walks canonical
// order only and counts the dropped extras into *dropped.
func EncodeOrderedObject(w *Wire, mode Mode, emit map[string]FieldEmitter, canonical []string, dropped *int) ([]byte, error) {
	e := NewObjectEncoder()
	emitted := make(map[string]bool, len(canonical))
	if mode == ModePreserve && w != nil {
		for _, key := range w.KeyOrder {
			if emitted[key] {
				continue
			}
			if fn, ok := emit[key]; ok {
				fn(e)
				emitted[key] = true
				continue
			}
			if v, ok := w.Extra(key); ok {
				e.Raw(key, v)
				emitted[key] = true
			}
		}
	}
	for _, key := range canonical {
		if emitted[key] {
			continue
		}
		if fn, ok := emit[key]; ok {
			fn(e)
			emitted[key] = true
		}
	}
	if mode == ModeStrip && w != nil && dropped != nil {
		*dropped += len(w.Extras)
	}
	return e.Bytes()
}

// EmitShadowString writes key with its shadowed source bytes when val
// is unmutated, canonical marshal otherwise.
func EmitShadowString(e *ObjectEncoder, w *Wire, key, val string) {
	if raw, ok := w.ShadowRaw(key, val); ok {
		e.Raw(key, raw)
		return
	}
	e.Field(key, val)
}

// EmitShadowInt64 is EmitShadowString for shadowed integers.
func EmitShadowInt64(e *ObjectEncoder, w *Wire, key string, val int64) {
	if raw, ok := w.ShadowRaw(key, val); ok {
		e.Raw(key, raw)
		return
	}
	e.Field(key, val)
}

// Fail records err on the encoder (sticky, surfaced by Bytes). Lets
// FieldEmitter closures propagate nested encode failures.
func (e *ObjectEncoder) Fail(err error) {
	if e.err == nil {
		e.err = err
	}
}

// CollectText concatenates the text of all text parts. Codecs use it
// to re-flatten parts into the string-form content some wire shapes
// carry (and to verify string shadows against the current IR value).
func CollectText(parts []Part) string {
	var b strings.Builder
	for i := range parts {
		if parts[i].Type == PartTypeText && parts[i].Text != nil {
			b.WriteString(parts[i].Text.Text)
		}
	}
	return b.String()
}

// EncodeArray joins pre-encoded elements into a JSON array.
func EncodeArray(elems []json.RawMessage) []byte {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, el := range elems {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(el)
	}
	buf.WriteByte(']')
	return buf.Bytes()
}

// DecodeTypedBlock peeks the discriminator field of a content-block
// object (e.g. "type" for Anthropic/OpenAI part shapes) without
// consuming the rest.
func DecodeTypedBlock(raw json.RawMessage, field string) (string, error) {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return "", err
	}
	tv, ok := probe[field]
	if !ok {
		return "", nil
	}
	var t string
	if err := json.Unmarshal(tv, &t); err != nil {
		return "", err
	}
	return t, nil
}

// UnknownPart captures an unmodeled content block verbatim (compacted)
// for preserve-mode re-emit / strip-mode drop+count.
func UnknownPart(raw json.RawMessage) (Part, error) {
	compacted, err := CompactRaw(raw)
	if err != nil {
		return Part{}, err
	}
	return Part{Type: PartTypeUnknown, Wire: Wire{Raw: compacted}}, nil
}
