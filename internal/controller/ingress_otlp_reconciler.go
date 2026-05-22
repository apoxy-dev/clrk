package controller

import (
	"context"
	"reflect"
	"sync"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/admin"
	"github.com/apoxy-dev/clrk/internal/extproc/ingress"
	"github.com/apoxy-dev/clrk/internal/otelemit"
)

// closeShutdownTimeout caps the predecessor emitter's Close drain.
// Matches internal/extproc/sinks.go's sinkShutdownTimeout — the OTLP
// exporter's batch flush is bounded by this deadline, so a sick
// telemetry backend never wedges a swap.
const closeShutdownTimeout = 5 * time.Second

// IngressOTLPReconciler watches EgressGateway objects and keeps the
// ingress ext_proc's OTLP emitter in sync with the resolved EG's
// spec.OTLP. The reconciler re-runs ingress.Resolve on every event,
// compares against the last-applied snapshot, and atomically swaps
// the live emitter on the Server when the spec changes.
//
// Why a reconciler and not a per-stream lookup (the egress sink's
// pattern at internal/extproc/sinks.go:96-166): the ingress ext_proc
// has no natural per-stream seam where re-resolution would land
// cheaply, and the EG-OTLP-changes-once-per-day cadence makes
// event-driven reconciliation a much better fit than spending a
// client.Get on every inbound request.
type IngressOTLPReconciler struct {
	client.Client

	// RuntimeNS is the controller-manager's namespace. The resolver
	// falls back to single-EG-in-RuntimeNS when CLRK_INGRESS_OTLP_GATEWAY
	// is unset, mirroring ingress.BuildEmitter's behavior.
	RuntimeNS string

	// Server is the live ingress ext_proc Server. The reconciler
	// holds a pointer (not a copy) so SwapEmitter mutates the
	// running data plane.
	Server *ingress.Server

	// Admin is the optional /admin mux. When non-nil, the reconciler
	// publishes the latest emitter state after every reconcile via
	// admin.Mux.SetIngressOTLPStatus so operators and integration
	// tests can read steady-state without driving requests.
	Admin *admin.Mux

	// InitialSpec seeds the reconciler's last-applied snapshot with
	// the controller-manager's warm-start resolution result, so the
	// first reconcile after the cache syncs short-circuits (no
	// spurious swap-and-Close on the warm-start emitter). Zero-value
	// means cold-boot resolved nothing.
	InitialSpec clrkv1alpha1.OTLPLogsSinkSpec

	once         sync.Once
	mu           sync.Mutex
	last         clrkv1alpha1.OTLPLogsSinkSpec
	lastPubReason string
}

// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=egressgateways,verbs=get;list;watch

// Reconcile re-resolves the active OTLP emitter and swaps it on the
// Server when spec.OTLP has changed. Triggered by any EgressGateway
// event — apply / spec change / status change / delete — and short-
// circuits on no-spec-change so dashboard-only EG churn (which
// re-emits status updates frequently) doesn't tear down a live
// emitter.
//
// On any resolver / construction failure the reconciler falls back
// to otelemit.Noop() and logs the reason. The contract is
// "ingress.dispatch should never block on telemetry": a sick
// collector or a misconfigured EG must not affect request handling.
func (r *IngressOTLPReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("ingress-otlp")

	// Initialize last on the first reconcile from the warm-start
	// snapshot the caller seeded. This is what makes the first
	// reconcile a no-op when nothing has changed since boot.
	r.once.Do(func() {
		r.mu.Lock()
		r.last = r.InitialSpec
		r.mu.Unlock()
	})

	resolved, err := ingress.Resolve(ctx, r.Client, r.RuntimeNS)
	if err != nil {
		logger.Info("EgressGateway not resolved; ingress emitter dropping to noop",
			"reason", err.Error(),
			"triggered_by", req.NamespacedName)
		r.applyResolution(resolved, "no_eg")
		return ctrl.Result{}, nil
	}

	r.mu.Lock()
	if reflect.DeepEqual(resolved.Spec, r.last) {
		r.mu.Unlock()
		// Spec unchanged — don't republish status. LastReason and
		// LastSwapTime are meant to describe the most recent
		// state-changing event, not "the reconciler ran again". EG
		// status churn fires many reconciles per second and would
		// otherwise clobber the swap reason an operator is watching for.
		return ctrl.Result{}, nil
	}
	prevSpec := r.last
	r.last = resolved.Spec
	r.mu.Unlock()

	prev := r.Server.SwapEmitter(resolved.Emitter)
	swappedAt := time.Now()
	reason := "swap"
	if resolved.Spec.Endpoint == "" {
		reason = "swap_to_noop"
	}
	logger.Info("Ingress OTLP emitter swapped",
		"egress_gateway", resolved.EGRef,
		"new_endpoint", resolved.Spec.Endpoint,
		"old_endpoint", prevSpec.Endpoint,
		"reason", reason)
	r.publishStatus(resolved, reason, swappedAt)

	if prev != nil {
		// Close the predecessor in a goroutine so the reconciler
		// returns promptly. The deadline ensures a sick OTLP backend
		// can't wedge the close indefinitely.
		go func(em otelemit.Emitter) {
			shutCtx, cancel := context.WithTimeout(context.Background(), closeShutdownTimeout)
			defer cancel()
			if err := em.Close(shutCtx); err != nil {
				logger.Error(err, "Closing displaced ingress OTLP emitter")
			}
		}(prev)
	}

	return ctrl.Result{}, nil
}

// applyResolution drops the live emitter to noop when the resolver
// errors. Spec snapshot is reset to zero so a later "EG re-appears"
// event re-triggers a swap. We publish status iff this is the first
// time we're reporting `reason` — subsequent identical reconciles
// (e.g. repeated "no EG in namespace" lookups) leave the surface
// untouched, so LastSwapTime continues to reflect the actual state
// transition rather than the latest poll.
func (r *IngressOTLPReconciler) applyResolution(resolved ingress.ResolvedEmitter, reason string) {
	r.mu.Lock()
	wasNoop := r.last.Endpoint == ""
	reasonAlreadyPublished := r.lastPubReason == reason
	r.last = clrkv1alpha1.OTLPLogsSinkSpec{}
	r.mu.Unlock()

	if !wasNoop {
		// State change: we had a live emitter, now we don't.
		prev := r.Server.SwapEmitter(resolved.Emitter)
		r.publishStatus(resolved, reason, time.Now())
		if prev != nil {
			go func(em otelemit.Emitter) {
				shutCtx, cancel := context.WithTimeout(context.Background(), closeShutdownTimeout)
				defer cancel()
				_ = em.Close(shutCtx)
			}(prev)
		}
		return
	}
	if reasonAlreadyPublished {
		return
	}
	// First time we've observed this no-EG / build-failure state —
	// publish so the surface tells operators the reconciler has
	// actually run and decided.
	r.publishStatus(resolved, reason, time.Now())
}

func (r *IngressOTLPReconciler) publishStatus(resolved ingress.ResolvedEmitter, reason string, swappedAt time.Time) {
	r.mu.Lock()
	r.lastPubReason = reason
	r.mu.Unlock()
	if r.Admin == nil {
		return
	}
	r.Admin.SetIngressOTLPStatus(admin.IngressOTLPStatus{
		EgressGateway: resolved.EGRef,
		Endpoint:      resolved.Spec.Endpoint,
		LastSwapTime:  swappedAt,
		LastReason:    reason,
		Noop:          resolved.Spec.Endpoint == "",
	})
}

// SetupWithManager wires the reconciler onto EgressGateway events.
// No predicate: change detection happens in Reconcile via the
// reflect.DeepEqual short-circuit on spec.OTLP, so EG status churn
// is filtered without predicate boilerplate. Named distinct from
// the EG provisioning reconciler so controller-runtime logs each
// independently.
func (r *IngressOTLPReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clrkv1alpha1.EgressGateway{}).
		Named("ingress-otlp-emitter").
		Complete(r)
}
