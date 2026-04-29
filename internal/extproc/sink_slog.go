package extproc

import "log/slog"

// slogSink is the fallback sink used when an EgressGateway has no OTLP
// endpoint configured (or when EG resolution fails). Emits one
// structured log line per record via the default slog handler.
type slogSink struct{}

func (slogSink) Emit(r Record) {
	slog.Info("clrk egress HTTP transaction",
		"agent.kind", r.AgentKind,
		"agent.namespace", r.AgentNamespace,
		"agent.name", r.AgentName,
		"agent.uid", r.AgentUID,
		"agent.revision", r.AgentRevision,
		"invocation.id", r.InvocationID,
		"req.method", r.RequestHeaders[":method"],
		"req.authority", r.RequestHeaders[":authority"],
		"req.path", r.RequestHeaders[":path"],
		"req.body_bytes", len(r.RequestBody),
		"req.truncated", r.RequestTruncated,
		"resp.status", r.ResponseHeaders[":status"],
		"resp.body_bytes", len(r.ResponseBody),
		"resp.truncated", r.ResponseTruncated,
	)
}
