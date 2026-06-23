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

	// AttrBodyContentEncoding names the wire content-encoding the captured
	// body arrived in (e.g. "gzip", "br", "zstd"), emitted on a body span
	// event only when that encoding was non-identity. clrk.body.b64 then
	// carries the DECODED bytes whenever the body was inflatable -- the
	// usual case -- so the attribute is the breadcrumb that the body was
	// "gzipped on the wire" even though the inspector renders plaintext. It
	// is the raw on-wire bytes only when the body was truncated (a
	// header-less keep-last-N tail) or failed to inflate; consumers detect
	// that via clrk.body.truncated and UTF-8/JSON validity.
	AttrBodyContentEncoding = "clrk.body.content_encoding"

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

	// Backend re-selection attributes — emitted only when ext_proc
	// re-pointed the request to a clrk Backend at RequestBody EOS, so
	// records for single-backend / non-reselectable routes are unchanged.
	AttrBackendReselected = "clrk.backend.reselected"
	AttrBackendName       = "clrk.backend.name"
	AttrBackendNamespace  = "clrk.backend.namespace"
	AttrBackendSchema     = "clrk.backend.schema"

	// Per-attempt fallback attributes — emitted only when the request
	// was pinned onto a synthesized LLM route and at least one upstream
	// attempt recorded facts. clrk.backend.* / clrk.translation.*
	// describe the SERVING (final) attempt; clrk.attempts is the walk
	// length and clrk.attempt.backends the ordered "<ns>/<name>" list
	// of backends the attempts targeted. Intermediate attempts'
	// response statuses are not observable (the upstream filter is
	// request-only by design); Envoy cluster retry stats carry those.
	// clrk.retry.ineligible surfaces a pinned request whose body
	// exceeded the router's retry buffer — Envoy silently disables
	// retries for it, so an attached fallback policy cannot fire.
	AttrAttempts        = "clrk.attempts"
	AttrAttemptBackends = "clrk.attempt.backends"
	AttrRetryIneligible = "clrk.retry.ineligible"

	// Cross-schema translation attributes (APO-742) — emitted only when
	// translation applied, skipped candidates, or failed, so records for
	// same-schema traffic are unchanged. gen_ai.* on a translated record
	// reflects the UPSTREAM conversation (the `to` schema); these attrs
	// are how an operator recovers what the agent originally spoke.
	AttrTranslationApplied         = "clrk.translation.applied"
	AttrTranslationFrom            = "clrk.translation.from"
	AttrTranslationTo              = "clrk.translation.to"
	AttrTranslationSkippedBackends = "clrk.translation.skipped_backends"
	AttrTranslationDroppedExtras   = "clrk.translation.dropped_extras"
	AttrTranslationError           = "clrk.translation.error"

	AttrMCPMethod             = "mcp.method"
	AttrMCPProtocolVersion    = "mcp.protocol.version"
	AttrMCPToolName           = "mcp.tool.name"
	AttrMCPResourceURI        = "mcp.resource.uri"
	AttrMCPRequestID          = "mcp.request.id"
	AttrMCPErrorCode          = "mcp.error.code"
	AttrMCPRouteMatched       = "clrk.mcproute.matched"
	AttrMCPRouteName          = "clrk.mcproute.name"
	AttrMCPRouteNamespace     = "clrk.mcproute.namespace"
	AttrMCPToolPolicyDecision = "clrk.mcproute.toolpolicy.decision"

	AttrBudgetDenied    = "clrk.budget.denied"
	AttrBudgetDailyUsed = "clrk.budget.daily_used"
	AttrBudgetDailyMax  = "clrk.budget.daily_max"

	AttrL4BytesUpstream = "clrk.l4.bytes_upstream"
	AttrL4DstName       = "clrk.dst.name"

	AttrComponent  = "clrk.component"
	AttrWorkerPool = "clrk.worker.pool"
	AttrSandboxID  = "clrk.sandbox.id"

	// AttrIoStream is the OTel semconv record-level attribute marking a
	// log record's source stream (IoStreamStdout / IoStreamStderr). It
	// is wired onto sandbox stdio LogRecords in APO-718; the read API
	// uses it to split a component's stdio into the two streams.
	AttrIoStream = "log.iostream"

	// AttrEgressGateway is the resource attribute every producer
	// (worker, ingress/egress ext_proc) stamps with "<ns>/<name>" of
	// the EgressGateway the signal belongs to. The cm OTLP receiver
	// uses it to (a) persist with EGRef as the leading sort key in
	// the embedded ClickHouse and (b) pick the per-EG forwarder when
	// re-exporting to a customer endpoint.
	AttrEgressGateway = "clrk.egress_gateway"

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

// Component* are the canonical clrk.component resource-attribute
// values — the emitting process/entity. Wire-frozen: the read API's
// source filter is clrk.component (split further by AttrIoStream for
// stdio). sentrystack is reserved for the in-Sentry gVisor netstack
// emitter (internal/sentrystack), which emits nothing yet — it is
// populated when the egress forwarder callbacks gain their own emitter.
const (
	ComponentIngressExtproc = "ingress-extproc"
	ComponentEgressExtproc  = "egress-extproc"
	ComponentWorker         = "worker"
	ComponentSentryStack    = "sentrystack"
)

// IoStream* are the enumerated values for AttrIoStream.
const (
	IoStreamStdout = "stdout"
	IoStreamStderr = "stderr"
)

// RetryIneligible* are the enumerated values for AttrRetryIneligible.
const (
	RetryIneligibleBodyTooLarge = "body_too_large"
)

// SpanName* are the wire-frozen span names clrk producers emit, declared
// once so the producer and any span-name-sensitive reader share a single
// constant. SpanNameIngressDispatch names the ingress routing span: the
// ingress ext_proc opens it to pick a worker and rewrite :authority, and
// it always carries a synthesized trace parent, so it is never a trace
// root.
const (
	SpanNameIngressDispatch = "ingress.dispatch"
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
