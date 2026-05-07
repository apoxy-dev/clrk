package controller

import (
	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// Re-exports of the shared label constants so reconcilers in this package
// can reference them without reaching into the api package. Any label the
// controllers project onto children should live in api/clrk/v1alpha1/labels.go
// so the worker can read them without importing this package.
const (
	labelAgent      = clrkv1alpha1.LabelAgent
	labelAgentKind  = clrkv1alpha1.LabelAgentKind
	labelGeneration = clrkv1alpha1.LabelGeneration
	labelWorkerPool = clrkv1alpha1.LabelWorkerPool
	labelComponent  = "clrk.apoxy.dev/component"
)

const (
	defaultGatewayClassName = "envoy"
	envoyGatewayGroup       = "gateway.envoyproxy.io"

	maxRevisionHistory = 10
)

// DispatchPort is the TCP port the worker dispatcher binds to and the
// WorkerPool Service exposes. TaskAgent HTTPRoutes target this port on
// the WorkerPool Service; EG resolves the Service to live Endpoints
// (default routingType=Endpoint) and load-balances across worker pods.
const DispatchPort int32 = 8090

// WorkerStatusPort is the gRPC port each worker pod serves
// WorkerStatusService on. The controller-manager opens one Watch
// stream per pod sourced from the WorkerPool's EndpointSlice and
// feeds the in-memory state map that the ingress ext_proc consults.
const WorkerStatusPort int32 = 8091

// IngressExtProcBackendName is the name of the per-namespace EG
// Backend that the per-TaskAgent EnvoyExtensionPolicy points its
// extProc.backendRefs at. The Backend's FQDN/IP+port is filled in
// by the ingress controller from the `--ingress-extproc-host` /
// `--ingress-extproc-port` flags so EG can reach the controller-
// manager's ingress ext_proc gRPC server.
const IngressExtProcBackendName = "clrk-ingress-extproc"

// Status.Conditions Type values written across the split reconcilers.
// Distinct values matter: meta.SetStatusCondition matches by Type, so each
// reconciler half (revision/ingress for TaskAgent, status/deployment for
// WorkerPool) owns its own subset and the slices merge cleanly.
const (
	condWorkerPoolReady  = "WorkerPoolReady"
	condEgressConfigured = "EgressConfigured"
	condRevisionReady    = "RevisionReady"
	condAccepted         = "Accepted"
	condGatewayReady     = "GatewayReady"
	condConfigured       = "Configured"
	condAvailable        = "Available"
	condProgressing      = "Progressing"
	condScheduled        = "Scheduled"
)

// Reason values for the Scheduled condition.
const (
	reasonScheduleRegistered = "ScheduleRegistered"
	reasonNotScheduled       = "NotScheduled"
	reasonParseError         = "ParseError"
	reasonLastFireFailed     = "LastFireFailed"
)
