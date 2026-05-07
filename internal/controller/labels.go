package controller

import (
	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/ports"
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

// Re-exports of port + name consts so existing controller-package
// callers don't have to rewrite import paths. See internal/ports for
// the source of truth — that leaf package has no transitive deps,
// which is what lets cmd/clrk reach for them without pulling in
// generated protobuf code.
const (
	DispatchPort              = ports.DispatchPort
	WorkerStatusPort          = ports.WorkerStatusPort
	IngressExtProcBackendName = ports.IngressExtProcBackendName
)

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
