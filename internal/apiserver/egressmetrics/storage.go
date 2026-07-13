// Package egressmetrics backs the Tier-1 metrics.clrk.apoxy.dev
// EgressGatewayMetrics kind with query-time ClickHouse aggregation over
// the otel_traces table, the way agentmetrics backs the agent snapshot
// kinds. Each List/Get is one EGRef-scoped conditional-aggregate scan
// that counts the gateway's proxied L7 exchanges over a stated look-back
// window and buckets them by response status class.
//
// Gateways are enumerated from their CRs, not from telemetry, so an idle
// gateway still appears (with zero usage), label selectors filter on the
// gateway's labels, and a Get on a nonexistent gateway is a proper 404.
// The per-listener/per-route nesting (the PodMetrics.Containers analogue)
// joins the scan's route-attributed rows against the route CR topology:
// spans carry route identity (clrk.aiproviderroute.* / clrk.mcproute.*),
// and a route's parentRef sectionName places it under a listener.
package egressmetrics

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

// ClientFunc resolves the controller-runtime client used to enumerate
// the gateway and route CRs. Evaluated per request rather than captured
// at construction: the apiserver Storage is built before the
// controller-manager's client exists (same lazy pattern as agentmetrics).
type ClientFunc func() client.Client

// defaultWindow is the rollup look-back when Deps.Window is unset,
// matching the agent snapshots' 24h.
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

// Storage is the rest.Storage backing the egressgatewaymetrics GVR.
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

// NewEgressGatewayMetricsProvider returns the StorageProvider for the
// egressgatewaymetrics GVR. The *runtime.Scheme / RESTOptionsGetter args
// are unused (storage is computed, not kept in the generic registry).
func NewEgressGatewayMetricsProvider(deps Deps) func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
	gr := schema.GroupResource{Group: metricsv1.GroupName, Resource: "egressgatewaymetrics"}
	return func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
		return &Storage{
			deps:           deps,
			gr:             gr,
			tableConvertor: registryrest.NewDefaultTableConvertor(gr),
		}, nil
	}
}

func (s *Storage) New() runtime.Object     { return &metricsv1.EgressGatewayMetrics{} }
func (s *Storage) NewList() runtime.Object { return &metricsv1.EgressGatewayMetricsList{} }
func (s *Storage) Destroy()                {}
func (s *Storage) NamespaceScoped() bool   { return true }
func (s *Storage) GetSingularName() string { return "egressgatewaymetrics" }

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

// List returns one snapshot per EgressGateway in scope. Gateways are
// enumerated from their CRs and joined against a single GROUP BY
// (EGRef, route) ClickHouse scan over the window, so every gateway
// appears — idle ones with zero usage. Like the agent snapshots the
// list is unpaginated and not watchable: a snapshot is one object per
// gateway CR in scope, and every read is a fresh aggregation.
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
	var egs clrkv1alpha1.EgressGatewayList
	if err := c.List(ctx, &egs, listOpts...); err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("list egress gateways: %w", err))
	}

	aprs, mcps, err := listRoutes(ctx, c)
	if err != nil {
		return nil, apierrors.NewInternalError(err)
	}

	now := time.Now().UTC()
	window := s.window()
	rows, err := scanUsage(ctx, s.deps.Pool, scanBody("", ns, now.Add(-window), now))
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("list egressgatewaymetrics: %w", err))
	}
	byEG := groupRows(rows)

	list := &metricsv1.EgressGatewayMetricsList{}
	ts := metav1.NewTime(now)
	dur := metav1.Duration{Duration: window}
	for i := range egs.Items {
		eg := &egs.Items[i]
		list.Items = append(list.Items, *buildObject(eg, byEG[eg.Namespace+"/"+eg.Name], aprs, mcps, ts, dur))
	}
	return list, nil
}

// Get returns the snapshot for one named gateway. The CR must exist (a
// missing gateway is a 404); a gateway with no traffic in the window
// yields a zero-usage snapshot rather than an error.
func (s *Storage) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	c := s.client()
	if c == nil {
		return nil, apierrors.NewServiceUnavailable("control plane not ready")
	}
	ns, _ := request.NamespaceFrom(ctx)

	var eg clrkv1alpha1.EgressGateway
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &eg); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, apierrors.NewNotFound(s.gr, name)
		}
		return nil, apierrors.NewInternalError(fmt.Errorf("get egress gateway: %w", err))
	}

	aprs, mcps, err := listRoutes(ctx, c)
	if err != nil {
		return nil, apierrors.NewInternalError(err)
	}

	now := time.Now().UTC()
	window := s.window()
	rows, err := scanUsage(ctx, s.deps.Pool, scanBody(ns+"/"+name, "", now.Add(-window), now))
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("get egressgatewaymetrics: %w", err))
	}
	return buildObject(&eg, rows, aprs, mcps, metav1.NewTime(now), metav1.Duration{Duration: window}), nil
}

// listRoutes enumerates the route CRs cluster-wide. Routes may attach to
// a gateway across namespaces (a parentRef carries an explicit
// namespace), so the enumeration cannot be pinned to the request's
// namespace; attachedRoutes filters per gateway.
func listRoutes(ctx context.Context, c client.Client) ([]clrkv1alpha1.AIProviderRoute, []clrkv1alpha1.MCPRoute, error) {
	var aprs clrkv1alpha1.AIProviderRouteList
	if err := c.List(ctx, &aprs); err != nil {
		return nil, nil, fmt.Errorf("list aiproviderroutes: %w", err)
	}
	var mcps clrkv1alpha1.MCPRouteList
	if err := c.List(ctx, &mcps); err != nil {
		return nil, nil, fmt.Errorf("list mcproutes: %w", err)
	}
	return aprs.Items, mcps.Items, nil
}

// groupRows indexes the scan rows by their EGRef.
func groupRows(rows []routeUsage) map[string][]routeUsage {
	byEG := make(map[string][]routeUsage)
	for _, r := range rows {
		byEG[r.egRef] = append(byEG[r.egRef], r)
	}
	return byEG
}

// buildObject assembles one snapshot: the gateway-wide totals summed over
// the raw rows (so unrouted traffic and stale route attributions still
// count), plus the per-listener breakdown joined against the route CR
// topology.
func buildObject(eg *clrkv1alpha1.EgressGateway, rows []routeUsage, aprs []clrkv1alpha1.AIProviderRoute, mcps []clrkv1alpha1.MCPRoute, ts metav1.Time, window metav1.Duration) *metricsv1.EgressGatewayMetrics {
	total := usageOf(0, 0, 0, 0)
	usageByRoute := make(map[string]metricsv1.UsageList, len(rows))
	for _, r := range rows {
		total = sumUsage(total, r.usageList())
		if k := r.key(); k != "" {
			usageByRoute[k] = r.usageList()
		}
	}
	routes := attachedRoutes(eg, aprs, mcps)
	return &metricsv1.EgressGatewayMetrics{
		ObjectMeta: metav1.ObjectMeta{Name: eg.Name, Namespace: eg.Namespace, Labels: eg.Labels, CreationTimestamp: ts},
		Timestamp:  ts,
		Window:     window,
		Usage:      total,
		Listeners:  buildListeners(eg, routes, usageByRoute),
	}
}
