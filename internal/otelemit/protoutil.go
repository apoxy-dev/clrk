package otelemit

import (
	"encoding/hex"
	"sort"
	"strconv"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// FlattenAttrs stringifies an OTLP KeyValue slice into a plain
// map[string]string. Returns nil for empty input so callers can
// short-circuit allocations.
func FlattenAttrs(in []*commonpb.KeyValue) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for _, kv := range in {
		out[kv.GetKey()] = AnyValueString(kv.GetValue())
	}
	return out
}

// AttrsToKV is the inverse of FlattenAttrs: it rebuilds an OTLP
// KeyValue slice from a flat string map, every value as a string
// AnyValue. The typing loss is inherent to the round trip — the
// chwriter schema flattened all attribute values to strings via
// AnyValueString, so reconstruction cannot recover the original
// int/bool/double/bytes AnyValue kinds. Keys are emitted in sorted
// order so the reconstructed signal (and its protojson form) is
// deterministic. Returns nil for empty input, mirroring FlattenAttrs so
// an absent map round-trips to an absent attribute slice.
func AttrsToKV(m map[string]string) []*commonpb.KeyValue {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*commonpb.KeyValue, 0, len(m))
	for _, k := range keys {
		out = append(out, &commonpb.KeyValue{
			Key:   k,
			Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: m[k]}},
		})
	}
	return out
}

// MergeAttrs returns the union of a and b. Keys in b win on conflict.
// Returns the non-empty operand directly when the other is empty —
// callers commonly merge resource attrs (a) with record attrs (b)
// where one side is often nil.
func MergeAttrs(a, b map[string]string) map[string]string {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	out := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// AnyValueString stringifies an OTLP AnyValue, preserving primitive
// types and falling back to the proto's text form for arrays / maps /
// bytes (rare on the egress signals we capture).
func AnyValueString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(x.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(x.DoubleValue, 'g', -1, 64)
	case *commonpb.AnyValue_BytesValue:
		return hex.EncodeToString(x.BytesValue)
	default:
		return v.String()
	}
}
