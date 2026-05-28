package otelforward

import (
	"context"
	"reflect"
	"sync"
	"time"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// closeTimeout bounds how long a displaced Forwarder gets to drain
// after a Swap. Matches the close window used by ingress OTLP and
// the egress sink registry.
const closeTimeout = 5 * time.Second

// Registry maps EGRef ("ns/name") to the live Forwarder for that
// EgressGateway. The reconciler swaps entries on spec changes; the
// OTLP receiver looks them up per inbound request. Lookups are
// RWMutex-protected for read-mostly access.
//
// controller-runtime serializes reconciles per object key, so
// concurrent Apply calls for the same EGRef are not expected.
type Registry struct {
	ctx context.Context

	mu    sync.RWMutex
	by    map[string]*entry
	specs map[string]clrkv1alpha1.OTLPLogsSinkSpec
}

type entry struct {
	fwd    *Forwarder
	cancel context.CancelFunc
}

// NewRegistry returns an empty Registry. ctx is the lifetime of
// every Forwarder spawned through Apply — cancelling it stops every
// pump.
func NewRegistry(ctx context.Context) *Registry {
	return &Registry{
		ctx:   ctx,
		by:    make(map[string]*entry),
		specs: make(map[string]clrkv1alpha1.OTLPLogsSinkSpec),
	}
}

// Get returns the live Forwarder for egRef, or nil when no forwarder
// is registered. The OTLP receiver calls this on every inbound
// request — a nil return means "don't forward, only persist".
func (r *Registry) Get(egRef string) *Forwarder {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e, ok := r.by[egRef]; ok {
		return e.fwd
	}
	return nil
}

// Apply reconciles the registry to spec for egRef:
//   - empty endpoint → remove + close the existing entry (if any).
//   - same spec as last apply → no-op.
//   - changed spec → build a new Forwarder, install it, close the old.
//
// The new Forwarder starts pumping immediately on Apply's return.
// Old Forwarders close asynchronously with a 5s drain deadline.
func (r *Registry) Apply(egRef string, spec clrkv1alpha1.OTLPLogsSinkSpec) {
	r.mu.Lock()
	prev, hadPrev := r.by[egRef]
	lastSpec, hadLast := r.specs[egRef]

	if spec.Endpoint == "" {
		if hadPrev {
			delete(r.by, egRef)
			delete(r.specs, egRef)
		}
		r.mu.Unlock()
		if hadPrev {
			go closeOld(prev)
		}
		return
	}
	if hadLast && reflect.DeepEqual(lastSpec, spec) {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	fwd := NewForwarder(egRef, spec)
	pumpCtx, cancel := context.WithCancel(r.ctx)
	go fwd.Run(pumpCtx)

	r.mu.Lock()
	r.by[egRef] = &entry{fwd: fwd, cancel: cancel}
	r.specs[egRef] = spec
	r.mu.Unlock()

	if hadPrev {
		go closeOld(prev)
	}
}

// Remove tears down the forwarder for egRef. Called by the
// reconciler when the EG is deleted.
func (r *Registry) Remove(egRef string) {
	r.mu.Lock()
	prev, ok := r.by[egRef]
	if !ok {
		r.mu.Unlock()
		return
	}
	delete(r.by, egRef)
	delete(r.specs, egRef)
	r.mu.Unlock()
	go closeOld(prev)
}

// Shutdown closes every live forwarder. Best-effort, bounded by the
// passed deadline.
func (r *Registry) Shutdown(ctx context.Context) {
	r.mu.Lock()
	all := r.by
	r.by = make(map[string]*entry)
	r.specs = make(map[string]clrkv1alpha1.OTLPLogsSinkSpec)
	r.mu.Unlock()
	for _, e := range all {
		e.cancel()
	}
	for _, e := range all {
		_ = e.fwd.Close(ctx)
	}
}

func closeOld(e *entry) {
	e.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()
	_ = e.fwd.Close(ctx)
}
