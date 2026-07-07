// Package notify records control-plane notifications as events.k8s.io/v1 Events
// in clrk's embedded apiserver. It owns the frozen Event reason vocabulary, a
// client-go events broadcaster (aggregating repeats into series.count), the
// data-plane security bridge that turns worker egress-denial OTLP into Events,
// the JetStream terminal-phase watcher for failed runs, and a pruner that keeps
// the kine-backed Event store bounded.
package notify

import corev1 "k8s.io/api/core/v1"

// Event reasons. This is the wire vocabulary the console's categorize() map
// mirrors -- changing a string here is a breaking change for the UI grouping and
// for anything filtering Events by reason. Keep additions in sync with
// console/apps/clrk-console/src/views/notifications-data.ts.
//
// A few reasons are reserved: declared here (and mirrored by the console) so the
// category vocabulary is complete, but their emit sites are pending. These are
// intentionally not yet recorded, NOT dead code:
//   - ReasonCredentialInjectionFailed: awaits the CIP status reconciler hook
//     (config-level); runtime injection failures have no attribute yet.
//   - ReasonRolloutStalled: awaits stall-past-deadline detection in the revision
//     reconcilers (RevisionReady, its Normal counterpart, is already emitted).
const (
	// Security (also reported to api.apoxy.dev by internal/sentry).
	ReasonEgressDenied              = "EgressDenied"
	ReasonOrphanSandbox             = "OrphanSandbox"
	ReasonEgressUpstreamFailed      = "EgressUpstreamFailed"
	ReasonCredentialInjectionFailed = "CredentialInjectionFailed" // Reserved -- see above.
	ReasonSecurityAdvisory          = "SecurityAdvisory"

	// Agent run failures.
	ReasonInvocationFailed   = "InvocationFailed"
	ReasonInvocationTimeout  = "InvocationTimeout"
	ReasonInvocationRejected = "InvocationRejected"

	// Rollouts.
	ReasonRevisionReady  = "RevisionReady"
	ReasonRolloutStalled = "RolloutStalled" // Reserved -- see above.

	// Fleet health.
	ReasonWorkerPoolDegraded = "WorkerPoolDegraded"
	ReasonWorkerPoolHealthy  = "WorkerPoolHealthy"
)

// Event types. Re-exported upstream constants so callers have one import site.
const (
	TypeNormal  = corev1.EventTypeNormal
	TypeWarning = corev1.EventTypeWarning
)

// Actions -- the machine-readable verb on an events.k8s.io Event.
const (
	ActionDeny    = "Deny"
	ActionDial    = "Dial"
	ActionInject  = "Inject"
	ActionRun     = "Run"
	ActionAdmit   = "Admit"
	ActionRollout = "Rollout"
	ActionScale   = "Scale"
	ActionAdvise  = "Advise"
)

// securityReasons is the set the phone-home reporter (internal/sentry) forwards
// to api.apoxy.dev. Kept beside the reason constants so the vocabulary has one
// home.
var securityReasons = map[string]struct{}{
	ReasonEgressDenied:              {},
	ReasonOrphanSandbox:             {},
	ReasonEgressUpstreamFailed:      {},
	ReasonCredentialInjectionFailed: {},
	ReasonSecurityAdvisory:          {},
}

// IsSecurityReason reports whether reason is a security notification that the
// phone-home reporter forwards to api.apoxy.dev.
func IsSecurityReason(reason string) bool {
	_, ok := securityReasons[reason]
	return ok
}
