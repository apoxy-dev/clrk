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

	AttrEgressDenyReason    = "clrk.egress.deny_reason"
	AttrEgressDstAddr       = "clrk.egress.dst_addr"
	AttrEgressFailureReason = "clrk.egress.failure_reason"
	AttrEgressPolicyDefault = "clrk.egress.policy.default"
	AttrEgressBackendAddr   = "clrk.egress.backend.addr"
	AttrEgressProxyError    = "clrk.egress.proxy_error"

	// Stamped on the ingress.dispatch span emitted per inbound TaskAgent
	// request. The pool key already lives at AttrWorkerPool.
	AttrTaskAgentNamespace = "clrk.taskagent.namespace"
	AttrTaskAgentName      = "clrk.taskagent.name"
	AttrTaskAgentRevision  = "clrk.taskagent.revision"
	AttrWorkerAddr         = "clrk.worker.addr"
	AttrIngressOutcome     = "clrk.ingress.outcome"
)

// IngressOutcome* are the enumerated values for AttrIngressOutcome.
// Operator dashboards filter on these verbatim, so they're frozen.
const (
	IngressOutcomeOK              = "ok"
	IngressOutcomeBadRequest      = "bad_request"
	IngressOutcomeNotFound        = "task_agent_not_found"
	IngressOutcomeNoReadyRevision = "no_ready_revision"
	IngressOutcomeNoReadyWorker   = "no_ready_worker"
	IngressOutcomeAtMaxConcurrent = "at_max_concurrent"
	IngressOutcomeInternal        = "internal_error"
)

// DenyReason values are wire-frozen — operator dashboards filter on
// AttrEgressDenyReason verbatim. Used by EgressBridge to categorise
// every "we refused to forward" event into one queryable enum.
type DenyReason string

const (
	DenyReasonLoopback          DenyReason = "loopback"
	DenyReasonUnspecified       DenyReason = "unspecified"
	DenyReasonLinkLocal         DenyReason = "link_local"
	DenyReasonMulticast         DenyReason = "multicast"
	DenyReasonWorkerLocalIfAddr DenyReason = "worker_local_ifaddr"
	DenyReasonPolicy            DenyReason = "policy"
	DenyReasonOrphanSandbox     DenyReason = "orphan_sandbox"
)

// FailureReason values are wire-frozen — see DenyReason. Used by
// EgressBridge to categorise post-allow upstream and handoff failures.
type FailureReason string

const (
	FailureReasonDirectDial  FailureReason = "direct_dial"
	FailureReasonBackendDial FailureReason = "backend_dial"
	FailureReasonProxyEncode FailureReason = "proxy_encode"
	FailureReasonProxyWrite  FailureReason = "proxy_write"
)
