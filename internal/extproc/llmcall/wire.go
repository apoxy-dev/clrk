package llmcall

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// ExtraField is one unmodeled JSON object member captured at decode
// time. Value is the verbatim source subtree — nested key order, number
// literals, and string escapes inside it are preserved for free because
// it is never re-parsed. Extras are an ordered slice, never a map:
// preserve-mode encoding must re-emit members in source order.
//
// +k8s:deepcopy-gen=true
type ExtraField struct {
	Key   string          `json:"key,omitempty" yaml:"key,omitempty"`
	Value json.RawMessage `json:"value,omitempty" yaml:"value,omitempty"`
}

// Shadow pairs the verbatim source bytes of a modeled scalar with the
// value decoded from them. Codecs record one when re-marshaling the
// decoded value would not reproduce the source bytes (\uXXXX escapes,
// exotic number literals, value-bearing formatting). At encode time the
// shadow's Raw is emitted only while Val still equals the field —
// a mutated field falls back to canonical marshaling.
type Shadow struct {
	Raw json.RawMessage `json:"raw,omitempty" yaml:"raw,omitempty"`
	Val any             `json:"val,omitempty" yaml:"val,omitempty"`
}

// DeepCopyInto is hand-written because deepcopy-gen cannot generate
// for the interface-typed Val. Val holds only comparable scalars
// (string, bool, json.Number, int64 — see RecordShadow), so plain
// assignment is a correct deep copy.
func (in *Shadow) DeepCopyInto(out *Shadow) {
	*out = *in
	if in.Raw != nil {
		out.Raw = make(json.RawMessage, len(in.Raw))
		copy(out.Raw, in.Raw)
	}
}

// DeepCopy returns a deep copy of the Shadow.
func (in *Shadow) DeepCopy() *Shadow {
	if in == nil {
		return nil
	}
	out := new(Shadow)
	in.DeepCopyInto(out)
	return out
}

// Wire is per-node bookkeeping that makes preserve-mode encoding
// byte-faithful. The zero value means "programmatically built": no
// extras, canonical field order, canonical formatting.
//
// +k8s:deepcopy-gen=true
type Wire struct {
	// KeyOrder is the node's full original member sequence — modeled
	// and extra keys interleaved. The encoder walks it to reproduce
	// source order; modeled keys absent from it (added after decode)
	// are appended in the codec's canonical order.
	KeyOrder []string `json:"keyOrder,omitempty" yaml:"keyOrder,omitempty"`

	// Extras are the unmodeled members in encounter order.
	Extras []ExtraField `json:"extras,omitempty" yaml:"extras,omitempty"`

	// Shadow maps a modeled key to its source-byte fallback. See Shadow.
	Shadow map[string]Shadow `json:"shadow,omitempty" yaml:"shadow,omitempty"`

	// Hints record wire-shape variants the typed fields can't express,
	// e.g. {"content": "string"} when an OpenAI message carried string
	// content rather than a parts array. Keys and values are
	// codec-private conventions.
	Hints map[string]string `json:"hints,omitempty" yaml:"hints,omitempty"`

	// Raw is the verbatim source subtree. On root nodes (Request,
	// Response) it is the full original body, used by the
	// equality-gated passthrough (EncodeRaw). On unknown Parts it is
	// the whole unmodeled block.
	Raw json.RawMessage `json:"raw,omitempty" yaml:"raw,omitempty"`

	// Path is the original request :path. Root Request only.
	Path string `json:"path,omitempty" yaml:"path,omitempty"`

	// Headers are modeled request headers captured at decode time
	// (e.g. anthropic-version), lowercased keys. Root Request only.
	Headers map[string]string `json:"headers,omitempty" yaml:"headers,omitempty"`
}

// Extra returns the value of the named extra member, if captured.
func (w *Wire) Extra(key string) (json.RawMessage, bool) {
	for i := range w.Extras {
		if w.Extras[i].Key == key {
			return w.Extras[i].Value, true
		}
	}
	return nil, false
}

// AddExtra appends an unmodeled member, preserving encounter order.
func (w *Wire) AddExtra(key string, value json.RawMessage) {
	w.Extras = append(w.Extras, ExtraField{Key: key, Value: value})
}

// RecordShadow stores source bytes for a modeled key whose canonical
// re-marshal differs from the source. Val must be a comparable scalar
// (string, bool, json.Number, int64) — ShadowRaw compares with ==.
func (w *Wire) RecordShadow(key string, raw json.RawMessage, val any) {
	if w.Shadow == nil {
		w.Shadow = make(map[string]Shadow)
	}
	w.Shadow[key] = Shadow{Raw: raw, Val: val}
}

// ShadowRaw returns the recorded source bytes for key when the shadowed
// value still equals val — i.e. the field was not mutated since decode.
func (w *Wire) ShadowRaw(key string, val any) (json.RawMessage, bool) {
	s, ok := w.Shadow[key]
	if !ok || s.Val != val {
		return nil, false
	}
	return s.Raw, true
}

// Hint returns the named wire-shape hint ("" when unset).
func (w *Wire) Hint(key string) string {
	return w.Hints[key]
}

// SetHint records a wire-shape hint.
func (w *Wire) SetHint(key, value string) {
	if w.Hints == nil {
		w.Hints = make(map[string]string)
	}
	w.Hints[key] = value
}

// MarshalCompact is the single JSON marshal entrypoint for encoders.
// It differs from json.Marshal in not HTML-escaping <, >, & — Go's
// default escaping would silently break byte-identity against provider
// output, so codecs must not call bare json.Marshal.
func MarshalCompact(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	b := buf.Bytes()
	// Encoder.Encode appends a newline; strip it.
	if n := len(b); n > 0 && b[n-1] == '\n' {
		b = b[:n-1]
	}
	return b, nil
}

// CompactJSON returns raw with insignificant whitespace removed. Key
// order, string escapes, and number literals are untouched — exactly
// the normalization the preserve-mode passthrough gate needs.
func CompactJSON(raw []byte) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeObject iterates the members of a JSON object in source order,
// handing each key and its verbatim value subtree to fn. Returns an
// error when raw is not a JSON object or fn fails. Duplicate keys are
// passed through as encountered; fidelity guarantees assume providers
// don't emit them.
func DecodeObject(raw []byte, fn func(key string, value json.RawMessage) error) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return fmt.Errorf("llmcall: not a JSON object")
	}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return err
		}
		key, ok := keyTok.(string)
		if !ok {
			return fmt.Errorf("llmcall: non-string object key")
		}
		var val json.RawMessage
		if err := dec.Decode(&val); err != nil {
			return err
		}
		if err := fn(key, val); err != nil {
			return err
		}
	}
	_, err = dec.Token() // Consume the closing brace.
	return err
}

// ObjectEncoder builds a compact JSON object member by member, letting
// codecs interleave canonically-marshaled fields with verbatim extras
// in Wire.KeyOrder sequence. Errors are sticky and surfaced by Bytes.
type ObjectEncoder struct {
	buf bytes.Buffer
	n   int
	err error
}

// NewObjectEncoder returns an encoder with the object opened.
func NewObjectEncoder() *ObjectEncoder {
	e := &ObjectEncoder{}
	e.buf.WriteByte('{')
	return e
}

func (e *ObjectEncoder) key(key string) {
	if e.n > 0 {
		e.buf.WriteByte(',')
	}
	e.n++
	kb, err := MarshalCompact(key)
	if err != nil && e.err == nil {
		e.err = err
		return
	}
	e.buf.Write(kb)
	e.buf.WriteByte(':')
}

// Raw appends a member with a verbatim (pre-encoded) value.
func (e *ObjectEncoder) Raw(key string, value json.RawMessage) {
	if e.err != nil {
		return
	}
	e.key(key)
	e.buf.Write(value)
}

// Field appends a member, marshaling v via MarshalCompact.
func (e *ObjectEncoder) Field(key string, v any) {
	if e.err != nil {
		return
	}
	vb, err := MarshalCompact(v)
	if err != nil {
		e.err = err
		return
	}
	e.key(key)
	e.buf.Write(vb)
}

// Bytes closes the object and returns it, or the first error.
func (e *ObjectEncoder) Bytes() ([]byte, error) {
	if e.err != nil {
		return nil, e.err
	}
	e.buf.WriteByte('}')
	return e.buf.Bytes(), nil
}

// EncodeRaw implements the preserve-mode passthrough gate shared by all
// codecs. fresh is the encoder's compact output built from the IR; raw
// is the original body captured at decode time (Wire.Raw). When fresh
// equals json.Compact(raw) — the encoder is byte-faithful modulo
// whitespace — the original raw bytes are returned, restoring
// formatting (providers pretty-print responses) with exact=true. Any
// difference, including post-decode mutation, falls back to fresh.
//
// The equality gate is what keeps the golden corpus non-vacuous: a bug
// anywhere in the KeyOrder/Extras/Shadow machinery breaks the equality,
// the passthrough doesn't fire, and the corpus sees the byte diff.
func EncodeRaw(fresh []byte, raw json.RawMessage) (body []byte, exact bool) {
	if len(raw) == 0 {
		return fresh, false
	}
	compacted, err := CompactJSON(raw)
	if err != nil {
		return fresh, false
	}
	if bytes.Equal(fresh, compacted) {
		return raw, true
	}
	return fresh, false
}
