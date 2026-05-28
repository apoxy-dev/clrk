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
	if r.MCP != nil {
		attrs = append(attrs, "mcp.method", r.MCP.Method)
		if r.MCP.ToolName != "" {
			attrs = append(attrs, "mcp.tool.name", r.MCP.ToolName)
		}
		if r.MCP.ResourceURI != "" {
			attrs = append(attrs, "mcp.resource.uri", r.MCP.ResourceURI)
		}
	}
	if r.MatchedMCPRouteName != "" {
		attrs = append(attrs,
			"clrk.mcproute.matched", true,
			"clrk.mcproute.namespace", r.MatchedMCPRouteNamespace,
			"clrk.mcproute.name", r.MatchedMCPRouteName,
		)
	}
	if r.MCPToolPolicyDecision != "" {
		attrs = append(attrs, "clrk.mcproute.toolpolicy.decision", r.MCPToolPolicyDecision)
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

func (slogSink) EmitL4(r L4Record) {
	dur := r.EndAt.Sub(r.Timestamp)
	if r.Timestamp.IsZero() || r.EndAt.IsZero() {
		dur = 0
	}
	attrs := make([]any, 0, 18) // 8 KV pairs (16) + optional DstName KV (2).
	attrs = append(attrs,
		"agent.kind", r.AgentKind,
		"agent.namespace", r.AgentNamespace,
		"agent.name", r.AgentName,
		"agent.uid", r.AgentUID,
		"agent.revision", r.AgentRevision,
		"invocation.id", r.InvocationID,
		"clrk.l4.bytes_upstream", r.BytesUpstream,
		"clrk.duration_ms", int(dur/1e6),
	)
	if r.DstName != "" {
		attrs = append(attrs, "clrk.dst.name", r.DstName)
	}
	slog.Info("clrk egress L4 connection", attrs...)
}
