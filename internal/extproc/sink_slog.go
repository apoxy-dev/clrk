package extproc

import "log/slog"

// slogSink is the fallback sink used when an EgressGateway has no OTLP
// endpoint configured (or when EG resolution fails). Emits one
// structured log line per record via the default slog handler.
type slogSink struct{}

func (slogSink) Emit(r Record) {
	attrs := []any{
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
	}
	if r.Provider != nil {
		// gen_ai.* attributes for the AI-provider acceptance line. These
		// match the keys we publish on the OTLP sink so log greps are
		// consistent across both backends.
		attrs = append(attrs,
			"gen_ai.system", r.Provider.System,
			"gen_ai.operation.name", r.Provider.Operation,
			"gen_ai.request.model", r.Provider.RequestModel,
		)
		if r.Provider.ResponseModel != "" {
			attrs = append(attrs, "gen_ai.response.model", r.Provider.ResponseModel)
		}
		if r.Provider.InputTokens > 0 {
			attrs = append(attrs, "gen_ai.usage.input_tokens", r.Provider.InputTokens)
		}
		if r.Provider.OutputTokens > 0 {
			attrs = append(attrs, "gen_ai.usage.output_tokens", r.Provider.OutputTokens)
		}
		if r.Provider.StreamResponse {
			attrs = append(attrs, "gen_ai.response.stream", true)
		}
	}
	if r.MatchedRouteName != "" {
		attrs = append(attrs,
			"clrk.aiproviderroute.namespace", r.MatchedRouteNamespace,
			"clrk.aiproviderroute.name", r.MatchedRouteName,
		)
	}
	if r.BudgetDenied {
		attrs = append(attrs,
			"clrk.budget.denied", true,
			"clrk.budget.daily_used", r.BudgetDailyUsed,
			"clrk.budget.daily_max", r.BudgetDailyMax,
		)
		slog.Warn("clrk egress request denied by TokenBudget", attrs...)
		return
	}
	slog.Info("clrk egress HTTP transaction", attrs...)
}
