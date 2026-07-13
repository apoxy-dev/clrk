// Package workerpoolmetrics backs the Tier-1 metrics.clrk.apoxy.dev
// WorkerPoolMetrics kind with query-time ClickHouse aggregation over the
// otel_traces table, the way agentmetrics and egressmetrics back the
// other Tier-1 snapshot kinds. Each List/Get is one pool-scoped scan over
// the pool's ingress.dispatch spans (invocations dispatched, dispatch
// errors, dispatch-latency percentiles) joined to the point-in-time gauges
// read from the WorkerPool CR (replica counts, execution-slot capacity,
// in-flight executions).
//
// Pools are enumerated from their CRs, not from telemetry, so an idle pool
// still appears (with zero dispatch usage but live gauges), label
// selectors filter on the pool's labels, and a Get on a nonexistent pool
// is a proper 404. Unlike the agent and gateway kinds, a WorkerPool
// snapshot has no nested breakdown: it is a flat {timestamp, window,
// usage} object.
//
// The series subresource the agent and gateway kinds carry is deliberately
// absent here. The Tier-2 catalog recipes all read agent/gateway spans
// (tokens, tool calls, egress requests), none of which carry
// clrk.worker.pool -- that attribute lives only on the ingress.dispatch
// span. A pool time-series therefore needs its own dispatch-sourced
// recipes rather than a scope over the existing catalog; it is tracked as
// a follow-up.
package workerpoolmetrics

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	registryrest "k8s.io/apiserver/pkg/registry/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	metricsv1 "github.com/apoxy-dev/clrk/api/metrics/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/apiserver/chsql"
)

// Doer is the shared ClickHouse-pool seam (internal/apiserver/chsql),
// aliased here so the package's Deps/field references stay local.
type Doer = chsql.Doer

// ClientFunc resolves the controller-runtime client used to enumerate the
// WorkerPool CRs. Evaluated per request rather than captured at
// construction: the apiserver Storage is built before the
// controller-manager's client exists (same lazy pattern as agentmetrics).
type ClientFunc func() client.Client

// defaultWindow is the rollup look-back when Deps.Window is unset,
// matching the agent and gateway snapshots' 24h.
const defaultWindow = 24 * time.Hour

// Deps are the storage dependencies.
type Deps struct {
	// Pool is the ClickHouse read pool (the shared LazyPool in prod).
	Pool Doer
	// Client resolves the kube client used to enumerate CRs.
	Client ClientFunc
	// Window is the rollup look-back. Zero means defaultWindow.
	Window time.Duration
}

// Storage is the rest.Storage backing the workerpoolmetrics GVR.
type Storage struct {
	deps           Deps
	gr             schema.GroupResource
	tableConvertor registryrest.TableConvertor
}

var (
	_ registryrest.Storage              = (*Storage)(nil)
	_ registryrest.Scoper               = (*Storage)(nil)
	_ registryrest.Lister               = (*Storage)(nil)
	_ registryrest.Getter               = (*Storage)(nil)
	_ registryrest.TableConvertor       = (*Storage)(nil)
	_ registryrest.SingularNameProvider = (*Storage)(nil)
)

// NewWorkerPoolMetricsProvider returns the StorageProvider for the
// workerpoolmetrics GVR. The *runtime.Scheme / RESTOptionsGetter args are
// unused (storage is computed, not kept in the generic registry).
func NewWorkerPoolMetricsProvider(deps Deps) func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
	gr := schema.GroupResource{Group: metricsv1.GroupName, Resource: "workerpoolmetrics"}
	return func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
		return &Storage{
			deps:           deps,
			gr:             gr,
			tableConvertor: registryrest.NewDefaultTableConvertor(gr),
		}, nil
	}
}

func (s *Storage) New() runtime.Object     { return &metricsv1.WorkerPoolMetrics{} }
func (s *Storage) NewList() runtime.Object { return &metricsv1.WorkerPoolMetricsList{} }
func (s *Storage) Destroy()                {}
func (s *Storage) NamespaceScoped() bool   { return true }
func (s *Storage) GetSingularName() string { return "workerpoolmetrics" }

func (s *Storage) ConvertToTable(ctx context.Context, obj runtime.Object, opts runtime.Object) (*metav1.Table, error) {
	return s.tableConvertor.ConvertToTable(ctx, obj, opts)
}

// window resolves the effective look-back.
func (s *Storage) window() time.Duration {
	if s.deps.Window > 0 {
		return s.deps.Window
	}
	return defaultWindow
}

// client resolves the kube client, or nil when the control plane is not
// yet up.
func (s *Storage) client() client.Client {
	if s.deps.Client == nil {
		return nil
	}
	return s.deps.Client()
}

// poolRef is the per-pool identity + status gauges the snapshot needs from
// the WorkerPool CR: name/namespace/labels are stamped onto the metrics
// object (so label selectors and display work), and the gauges are the
// point-in-time reads joined alongside the dispatch aggregates.
type poolRef struct {
	name                string
	namespace           string
	labels              map[string]string
	readyReplicas       int32
	desiredReplicas     int32
	active              int32
	maxExecutions       int32
	availableExecutions int32
}

// poolRefOf projects a WorkerPool CR into a poolRef. Spec.Replicas is a
// pointer with a kubebuilder default of 1; a nil (pre-admission or
// hand-written) value is read as the same 1 so desired_replicas is never
// a spurious 0.
func poolRefOf(wp *clrkv1alpha1.WorkerPool) poolRef {
	desired := int32(1)
	if wp.Spec.Replicas != nil {
		desired = *wp.Spec.Replicas
	}
	return poolRef{
		name:                wp.Name,
		namespace:           wp.Namespace,
		labels:              wp.Labels,
		readyReplicas:       wp.Status.ReadyReplicas,
		desiredReplicas:     desired,
		active:              wp.Status.ActiveExecutions,
		maxExecutions:       wp.Status.Capacity.MaxExecutions,
		availableExecutions: wp.Status.Capacity.AvailableExecutions,
	}
}

// List returns one snapshot per WorkerPool in scope. Pools are enumerated
// from their CRs and joined against a single GROUP BY (pool, namespace)
// ClickHouse scan over the window, so every pool appears -- idle ones with
// zero dispatch usage but live CR gauges. Like the other Tier-1 snapshots
// the list is unpaginated and not watchable: a snapshot is one object per
// pool CR in scope, and every read is a fresh aggregation.
func (s *Storage) List(ctx context.Context, opts *internalversion.ListOptions) (runtime.Object, error) {
	c := s.client()
	if c == nil {
		return nil, apierrors.NewServiceUnavailable("control plane not ready")
	}
	ns, _ := request.NamespaceFrom(ctx)
	var sel labels.Selector
	if opts != nil {
		sel = opts.LabelSelector
	}

	listOpts := []client.ListOption{client.InNamespace(ns)}
	if sel != nil && !sel.Empty() {
		listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: sel})
	}
	var pools clrkv1alpha1.WorkerPoolList
	if err := c.List(ctx, &pools, listOpts...); err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("list worker pools: %w", err))
	}

	now := time.Now().UTC()
	window := s.window()
	rows, err := scanUsage(ctx, s.deps.Pool, scanBody(ns, "", now.Add(-window), now))
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("list workerpoolmetrics: %w", err))
	}
	// Join the dispatch aggregates to the enumerated CRs. In a namespaced
	// list the scan's WHERE pins agent.namespace = ns, so keying by pool
	// name alone is exact; a cluster-scoped list spans namespaces and must
	// key by namespace/name to keep two same-named pools apart.
	namespaced := ns != ""
	byKey := make(map[string]poolUsage, len(rows))
	for _, r := range rows {
		if namespaced {
			byKey[r.pool] = r
		} else {
			byKey[r.namespace+"/"+r.pool] = r
		}
	}

	list := &metricsv1.WorkerPoolMetricsList{}
	ts := metav1.NewTime(now)
	dur := metav1.Duration{Duration: window}
	for i := range pools.Items {
		ref := poolRefOf(&pools.Items[i])
		var u poolUsage
		if namespaced {
			u = byKey[ref.name]
		} else {
			u = byKey[ref.namespace+"/"+ref.name]
		}
		list.Items = append(list.Items, *s.buildObject(ref, u, ts, dur))
	}
	return list, nil
}

// Get returns the snapshot for one named pool. The CR must exist (a
// missing pool is a 404); a pool with no dispatches in the window yields a
// zero-dispatch snapshot (live CR gauges, zero aggregates) rather than an
// error.
func (s *Storage) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	c := s.client()
	if c == nil {
		return nil, apierrors.NewServiceUnavailable("control plane not ready")
	}
	ns, _ := request.NamespaceFrom(ctx)

	var wp clrkv1alpha1.WorkerPool
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &wp); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, apierrors.NewNotFound(s.gr, name)
		}
		return nil, apierrors.NewInternalError(fmt.Errorf("get worker pool: %w", err))
	}

	now := time.Now().UTC()
	window := s.window()
	rows, err := scanUsage(ctx, s.deps.Pool, scanBody(ns, name, now.Add(-window), now))
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("get workerpoolmetrics: %w", err))
	}
	var u poolUsage
	if len(rows) > 0 {
		u = rows[0]
	}
	return s.buildObject(poolRefOf(&wp), u, metav1.NewTime(now), metav1.Duration{Duration: window}), nil
}

// buildObject assembles one snapshot: the CH-derived dispatch usage plus
// the point-in-time gauges read from the WorkerPool CR status/spec. The
// gauges are read from the CR, NOT recomputed, so the metrics agree with
// `kubectl get workerpool`; the trade-off is a bounded reconcile-cadence
// staleness on them (the dispatch keys are fresh window aggregates).
func (s *Storage) buildObject(ref poolRef, u poolUsage, ts metav1.Time, window metav1.Duration) *metricsv1.WorkerPoolMetrics {
	usage := u.usageList()
	g := func(n int32) resource.Quantity { return *resource.NewQuantity(int64(n), resource.DecimalSI) }
	usage[metricsv1.UsageReadyReplicas] = g(ref.readyReplicas)
	usage[metricsv1.UsageDesiredReplicas] = g(ref.desiredReplicas)
	usage[metricsv1.UsageActive] = g(ref.active)
	usage[metricsv1.UsageMaxExecutions] = g(ref.maxExecutions)
	usage[metricsv1.UsageAvailableExecutions] = g(ref.availableExecutions)
	return &metricsv1.WorkerPoolMetrics{
		ObjectMeta: metav1.ObjectMeta{Name: ref.name, Namespace: ref.namespace, Labels: ref.labels, CreationTimestamp: ts},
		Timestamp:  ts,
		Window:     window,
		Usage:      usage,
	}
}
