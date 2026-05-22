package otelemit

// String values are part of the on-the-wire contract — operator
// dashboards filter on these keys verbatim, so they're frozen even if
// the Go identifier changes. gen_ai.* are OTel GenAI semconv keys;
// semconv/v1.26.0 doesn't ship typed helpers for them yet, so we
// declare the strings here.
const (
	AttrAgentKind      = "agent.kind"
	AttrAgentNamespace = "agent.namespace"
	AttrAgentName      = "agent.name"
	AttrAgentUID       = "agent.uid"
	AttrAgentRevision  = "agent.revision"
	AttrInvocationID   = "invocation.id"

	AttrReqBytes      = "clrk.req.bytes"
	AttrRespBytes     = "clrk.resp.bytes"
	AttrReqTruncated  = "clrk.req.truncated"
	AttrRespTruncated = "clrk.resp.truncated"
	AttrDurationMs    = "clrk.duration_ms"
	AttrTraceID       = "trace_id"
	AttrSpanID        = "span_id"
	AttrBodyBytes     = "clrk.body.bytes"
	AttrBodyTruncated = "clrk.body.truncated"
	AttrBodyB64       = "clrk.body.b64"
	AttrRespChunks    = "clrk.resp.chunks"

	AttrGenAISystem        = "gen_ai.system"
	AttrGenAIOperationName = "gen_ai.operation.name"
	AttrGenAIRequestModel  = "gen_ai.request.model"
	AttrGenAIResponseModel = "gen_ai.response.model"
	AttrGenAIInputTokens   = "gen_ai.usage.input_tokens"
	AttrGenAIOutputTokens  = "gen_ai.usage.output_tokens"
	AttrGenAIStream        = "gen_ai.response.stream"

	AttrAPRRouteMatched   = "clrk.aiproviderroute.matched"
	AttrAPRRouteName      = "clrk.aiproviderroute.name"
	AttrAPRRouteNamespace = "clrk.aiproviderroute.namespace"
	AttrBodyUsageVisible  = "clrk.body.usage_visible"
	AttrBodyReqRewritten  = "clrk.body.request_rewritten"

	AttrBudgetDenied    = "clrk.budget.denied"
	AttrBudgetDailyUsed = "clrk.budget.daily_used"
	AttrBudgetDailyMax  = "clrk.budget.daily_max"

	AttrL4BytesUpstream = "clrk.l4.bytes_upstream"
	AttrL4DstName       = "clrk.dst.name"

	AttrComponent  = "clrk.component"
	AttrWorkerPool = "clrk.worker.pool"
	AttrSandboxID  = "clrk.sandbox.id"

	AttrImageRef    = "clrk.image.ref"
	AttrImageDigest = "clrk.image.digest"

	AttrEgressDenyReason = "clrk.egress.deny_reason"
	AttrEgressDstAddr    = "clrk.egress.dst_addr"
)
