package otelemit

import (
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"

	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
)

// AgentAttrs is the OTel-attribute counterpart of identityLogFields
// in internal/worker/sandbox/log.go. Empty UID / Revision /
// InvocationID are omitted so "absent" is distinguishable from "empty
// string" at the query layer. Kind renders as the integer kind so log
// and span joins line up with the slog rendering.
func AgentAttrs(id proxyproto.AgentIdentity) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String(AttrAgentKind, strconv.Itoa(int(id.Kind))),
		attribute.String(AttrAgentNamespace, id.Namespace),
		attribute.String(AttrAgentName, id.Name),
	}
	if id.UID != "" {
		attrs = append(attrs, attribute.String(AttrAgentUID, id.UID))
	}
	if id.Revision != "" {
		attrs = append(attrs, attribute.String(AttrAgentRevision, id.Revision))
	}
	if id.InvocationID != "" {
		attrs = append(attrs, attribute.String(AttrInvocationID, id.InvocationID))
	}
	return attrs
}

// AddLogAttrs writes each span-attribute KV onto rec via
// rec.AddAttributes, one KV at a time. Avoids the intermediate
// []otellog.KeyValue slice an adapter would otherwise allocate per
// emit. Slice / unknown attribute kinds fall back to v.Emit();
// production callers stamp scalar-only attrs, so this never triggers
// in practice.
func AddLogAttrs(rec *otellog.Record, kvs []attribute.KeyValue) {
	for _, kv := range kvs {
		rec.AddAttributes(otellog.KeyValue{
			Key:   string(kv.Key),
			Value: attrValueToLogValue(kv.Value),
		})
	}
}

func attrValueToLogValue(v attribute.Value) otellog.Value {
	switch v.Type() {
	case attribute.BOOL:
		return otellog.BoolValue(v.AsBool())
	case attribute.INT64:
		return otellog.Int64Value(v.AsInt64())
	case attribute.FLOAT64:
		return otellog.Float64Value(v.AsFloat64())
	case attribute.STRING:
		return otellog.StringValue(v.AsString())
	default:
		return otellog.StringValue(v.Emit())
	}
}
