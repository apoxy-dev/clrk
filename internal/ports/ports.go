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
const (
	HeaderTaskAgent       = "X-Clrk-TaskAgent"
	HeaderTrigger         = "X-Clrk-Trigger"
	HeaderExitCode        = "X-Clrk-Exit-Code"
	HeaderExecutionID     = "X-Clrk-Execution-ID"
	HeaderWorkerEndpoint  = "X-Clrk-Worker-Endpoint"
)
