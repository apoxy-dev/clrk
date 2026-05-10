// Package ports holds TCP/gRPC port and resource-name constants
// shared across the worker, the controller, and the clrk CLI.
//
// It exists as a leaf package so callers (in particular cmd/clrk,
// which is the only binary that must build standalone via
// `go build`) can reach for these values without dragging in the
// rest of internal/controller — which transitively depends on
// generated protobuf code that is not committed to this repo.
package ports

// DispatchPort is the TCP port the worker dispatcher binds to and
// the WorkerPool Service exposes. The per-TaskAgent ingress ext_proc
// rewrites :authority to <podIP>:<DispatchPort> on each request.
const DispatchPort int32 = 8090

// WorkerStatusPort is the gRPC port each worker pod serves
// WorkerStatusService on. controller-manager opens one streaming
// Watch per pod sourced from the WorkerPool's EndpointSlice and
// feeds the in-memory routing state map.
const WorkerStatusPort int32 = 8091

// IngressExtProcBackendName is the name of the per-namespace EG
// Backend that the per-TaskAgent EnvoyExtensionPolicy points its
// extProc.backendRefs at. The Backend's FQDN/IP+port is filled in
// by the ingress controller from runtime flags so EG can reach the
// controller-manager's ingress ext_proc gRPC server.
const IngressExtProcBackendName = "clrk-ingress-extproc"

// Dispatch headers carried on every TaskAgent invocation. The HTTPRoute
// filter (or cron HTTP invoker) sets HeaderTaskAgent + HeaderTrigger;
// the ingress ext_proc reads HeaderExecutionID for tie-breaking and
// stamps HeaderWorkerEndpoint with the picked pod IP for telemetry;
// the worker dispatcher reads HeaderTaskAgent and writes HeaderExitCode
// on the response.
//
// HeaderInvocationID is generated (or accepted from HeaderExecutionID)
// at the ingress edge and forwarded to the worker so the dispatcher
// can use it as the per-sandbox PROXY v2 invocation.id TLV. That same
// id is the key the egress ext_proc uses to look up the inbound
// trace parent in the controller-manager-local invocationctx store —
// it's how the inbound traceparent reaches outbound LLM/MCP calls
// without requiring agent-side instrumentation.
const (
	HeaderTaskAgent      = "X-Clrk-TaskAgent"
	HeaderTrigger        = "X-Clrk-Trigger"
	HeaderExitCode       = "X-Clrk-Exit-Code"
	HeaderExecutionID    = "X-Clrk-Execution-ID"
	HeaderInvocationID   = "X-Clrk-Invocation-ID"
	HeaderWorkerEndpoint = "X-Clrk-Worker-Endpoint"
)

// Sandbox metadata service. The worker exposes a per-execution
// IMDS-style HTTP server inside the sandbox netns, bound on link-
// local addresses so agents can reach it at well-known endpoints
// without configuration. Mirrors the AWS/GCP IMDS convention
// (169.254.169.254:80) so existing CloudEvents-aware SDKs and
// curl|wget recipes "just work" inside a clrk sandbox.
//
// The IPv6 address is the AWS IPv6 IMDS ULA. We don't run a full
// IPv6 dual-stack inside sandboxes — only the metadata /128 is
// reachable, via a peer route on the per-sandbox TAP.
const (
	MetadataAddrV4 = "169.254.169.254"
	MetadataAddrV6 = "fd00:ec2::254"
	MetadataPort   = 80
)
