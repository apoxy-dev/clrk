// Package sandbox is the public, tenant-neutral gVisor/runsc sandbox spine of
// the clrk worker.
//
// It is the neutral CORE carved out of the clrk worker's sandbox runtime
// (internal/worker/sandbox + internal/sentrystack): the bare
// pull -> OCI bundle -> runsc -> cgroup-v2 -> sentrystack-loopback lifecycle,
// with the agent-lineage, identity, egress, trust and persistent-state
// coupling kept OUT. clrk's own internal/worker/sandbox re-points onto this
// package and layers those tenant/egress concerns back on as a thin wrapper,
// so the existing worker keeps its full egress-capable behavior; external
// consumers (the workerd host, apoxy-cloud) import this package directly for a
// bare single-tenant, inbound-capable sandbox. One source of truth, imported
// across the module boundary via the APO-703 commit/pseudo-version pin — not a
// second copy. See docs/workerd-runtime-mvp.md §3.6 (apoxy-cloud) for the full
// carve map.
//
// The seam is the [Runtime] interface — the tenant-neutral lifecycle
// (Create/Start/Stop/Kill/Wait/Delete/Purge/Status/List/Cleanup) — plus the
// optional [EgressController] extension. Above the seam is policy + fan-out
// (which tenant, which revision, how many resident); below it is mechanism
// (ORAS pull, OCI bundle, runsc, cgroup-v2, the sentrystack loopback NIC).
// Nothing below the seam knows about tenants, Kubernetes, or revisions — which
// is why a workerd sandbox needs zero workerd-specific code here: it is just an
// OCI image whose [Spec.Command] is `workerd serve`. The tenant wrapper in
// internal/worker/sandbox plugs egress, identity, trust and persistent state
// back in through the core's extension seams ([Spec.Mounts], the
// [EgressController] setters, the sentrystack init payload).
//
// This change (APO-713) lands the neutral types and interfaces; the concrete
// *Manager, the ORAS image store, the runsc dispatch and the sentrystack
// loopback NIC are carved down from internal/worker/sandbox in the following
// sub-PRs.
package sandbox
