package metricsquery

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	registryrest "k8s.io/apiserver/pkg/registry/rest"

	metricsv1 "github.com/apoxy-dev/clrk/api/metrics/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/apiserver/chsql"
)

// Doer is the shared ClickHouse-pool seam (internal/apiserver/chsql), aliased
// here so the read model's Deps/field references stay local. The manager passes
// the shared LazyPool (which satisfies it structurally); unit tests inject a
// fake or a real ephemeral pool.
type Doer = chsql.Doer

// Deps are the dependencies shared by the metrics-query mounts.
type Deps struct {
	// Pool is the ClickHouse read pool (the shared LazyPool in prod).
	Pool Doer
}

// catalogStorage backs the `metrics` resource: a read-only Lister/Getter over
// the in-code catalog registry. The LIST is the catalog (so `kubectl get
// metrics` and the console's metric menu come from a typed object), and a GET
// returns one metric's descriptor. The time-series query is the `series`
// connect subresource, not this resource -- a descriptor and a query result are
// different kinds, so the read stays honest (GET metrics/{id} is the recipe,
// GET metrics/{id}/series is the data).
type catalogStorage struct {
	gr schema.GroupResource
}

var (
	_ registryrest.Storage              = (*catalogStorage)(nil)
	_ registryrest.Scoper               = (*catalogStorage)(nil)
	_ registryrest.Lister               = (*catalogStorage)(nil)
	_ registryrest.Getter               = (*catalogStorage)(nil)
	_ registryrest.TableConvertor       = (*catalogStorage)(nil)
	_ registryrest.SingularNameProvider = (*catalogStorage)(nil)
)

func (s *catalogStorage) New() runtime.Object     { return &metricsv1.Metric{} }
func (s *catalogStorage) NewList() runtime.Object { return &metricsv1.MetricList{} }
func (s *catalogStorage) Destroy()                {}
func (s *catalogStorage) NamespaceScoped() bool   { return true }
func (s *catalogStorage) GetSingularName() string { return "metric" }

// List returns the whole catalog. It is the same in every namespace (the
// recipes are global), and is unfiltered: the registry is small and fixed.
func (s *catalogStorage) List(_ context.Context, _ *internalversion.ListOptions) (runtime.Object, error) {
	// Deep-copy each entry (not a shallow copy): the items share their
	// GroupBy / Measures slice headers with the package-global catalogMetrics,
	// and a consumer that mutates a returned item in place would corrupt the
	// immutable registry. Get already deep-copies for the same reason.
	items := make([]metricsv1.Metric, len(catalogMetrics))
	for i := range catalogMetrics {
		items[i] = *catalogMetrics[i].DeepCopy()
	}
	return &metricsv1.MetricList{Items: items}, nil
}

// Get returns one metric's descriptor, or a 404 for an unknown id.
func (s *catalogStorage) Get(_ context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	m, ok := catalogByName[name]
	if !ok {
		return nil, apierrors.NewNotFound(s.gr, name)
	}
	return m.DeepCopy(), nil
}

// ConvertToTable renders the catalog for `kubectl get metrics` with the columns
// a human scans by: id, type, unit, and backing source.
func (s *catalogStorage) ConvertToTable(_ context.Context, obj runtime.Object, _ runtime.Object) (*metav1.Table, error) {
	t := &metav1.Table{
		ColumnDefinitions: []metav1.TableColumnDefinition{
			{Name: "Name", Type: "string", Format: "name", Description: "metric id"},
			{Name: "Type", Type: "string", Description: "counter, gauge, or histogram"},
			{Name: "Unit", Type: "string", Description: "value unit"},
			{Name: "Source", Type: "string", Description: "backing table"},
		},
	}
	add := func(m *metricsv1.Metric) {
		t.Rows = append(t.Rows, metav1.TableRow{
			Cells:  []interface{}{m.Name, string(m.Type), m.Unit, m.Source},
			Object: runtime.RawExtension{Object: m},
		})
	}
	switch o := obj.(type) {
	case *metricsv1.Metric:
		add(o)
	case *metricsv1.MetricList:
		for i := range o.Items {
			add(&o.Items[i])
		}
	}
	return t, nil
}

// seriesConnecter backs the `series` connect subresource that runs a metric
// query and returns a typed MetricSeriesSet through content negotiation (the
// metrics analogue of pods/log). One connecter type serves both surfaces:
//
//   - fleet (kind == ""): a subresource of `metrics`, so the path element is the
//     metric id and the scope is the namespace plus an optional scopeKind /
//     scopeName refinement.
//   - per-entity (kind != ""): a subresource of taskagentmetrics /
//     daemonagentmetrics, so the path element is the agent name, the metric id
//     comes from ?metric=, and the scope is fixed by the path (scopeKind /
//     scopeName are rejected).
type seriesConnecter struct {
	deps     Deps
	kind     EntityKind // "" => fleet surface
	singular string
}

var (
	_ registryrest.Storage              = (*seriesConnecter)(nil)
	_ registryrest.Scoper               = (*seriesConnecter)(nil)
	_ registryrest.Connecter            = (*seriesConnecter)(nil)
	_ registryrest.SingularNameProvider = (*seriesConnecter)(nil)
)

// New returns the response kind: a connecter that returns a typed object via
// responder.Object must advertise that kind here so the apiserver encodes the
// body to the right GroupVersion under content negotiation.
func (c *seriesConnecter) New() runtime.Object      { return &metricsv1.MetricSeriesSet{} }
func (c *seriesConnecter) Destroy()                 {}
func (c *seriesConnecter) NamespaceScoped() bool    { return true }
func (c *seriesConnecter) GetSingularName() string  { return c.singular }
func (c *seriesConnecter) ConnectMethods() []string { return []string{http.MethodGet} }

// NewConnectOptions returns no typed options. The apoxy apiserver builder
// installs connect handlers with the metav1-only ParameterCodec (it passes nil
// to buildAPIGroupInfos, which defaults to metav1.ParameterCodec), so a
// registered custom options object cannot decode -- the apiserver 400s with
// "no kind is registered for the type ... MetricQueryOptions". The handler reads
// the query string directly instead, exactly like the telemetry read connecter.
// The URL parameters are unchanged (metric, groupBy, since, until, step,
// quantiles, scopeKind, scopeName); they are parsed and validated in ServeHTTP.
func (c *seriesConnecter) NewConnectOptions() (runtime.Object, bool, string) {
	return nil, false, ""
}

// Connect captures the path scope (the namespace plus the path element -- the
// metric id on the fleet surface, the agent name on a per-entity surface) and
// returns the handler. The query string is parsed in ServeHTTP, where the
// request is available.
func (c *seriesConnecter) Connect(ctx context.Context, name string, _ runtime.Object, responder registryrest.Responder) (http.Handler, error) {
	ns := request.NamespaceValue(ctx)
	if ns == "" {
		return nil, apierrors.NewBadRequest("namespace is required")
	}
	if c.kind != "" && name == "" {
		return nil, apierrors.NewBadRequest("entity name is required in the request path")
	}
	return &seriesHandler{deps: c.deps, kind: c.kind, name: name, namespace: ns, responder: responder}, nil
}

// seriesHandler resolves the metric + scope from the path and query string, runs
// the query, and writes the typed result. All validation surfaces through the
// responder's error path as a clean Status.
type seriesHandler struct {
	deps      Deps
	kind      EntityKind // "" => fleet surface
	name      string     // metric id (fleet) or agent name (per-entity)
	namespace string
	responder registryrest.Responder
}

func (h *seriesHandler) ServeHTTP(_ http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	var base scope
	var metricID string
	if h.kind == "" {
		// Fleet surface: the path element is the metric id; the scope is the
		// namespace, optionally narrowed by scopeKind / scopeName. A stray
		// ?metric= is a misleading no-op here (the id comes from the path), so
		// reject it -- symmetric with the per-entity surface rejecting a stray
		// scopeKind / scopeName.
		if q.Get("metric") != "" {
			h.responder.Error(apierrors.NewBadRequest("metric is set by the path element on the fleet (metrics) surface; ?metric= is only for the per-agent surfaces"))
			return
		}
		metricID = h.name
		sc, err := refineFleetScope(scope{namespace: h.namespace}, q.Get("scopeKind"), q.Get("scopeName"))
		if err != nil {
			h.responder.Error(apierrors.NewBadRequest(err.Error()))
			return
		}
		base = sc
	} else {
		// Per-entity surface: the path element is the agent name; the metric id
		// comes from ?metric=. The scope is fixed by the path, so a scope
		// refinement param is a misleading no-op -- reject it.
		if q.Get("scopeKind") != "" || q.Get("scopeName") != "" {
			h.responder.Error(apierrors.NewBadRequest("scopeKind/scopeName are only valid on the fleet (metrics) surface; the per-entity path already fixes the scope"))
			return
		}
		metricID = strings.TrimSpace(q.Get("metric"))
		if metricID == "" {
			h.responder.Error(apierrors.NewBadRequest("metric is required (?metric=<id>)"))
			return
		}
		base = entityScope(h.kind, h.namespace, h.name)
	}

	p, err := parseParams(q, metricID, time.Now())
	if err != nil {
		h.responder.Error(apierrors.NewBadRequest(err.Error()))
		return
	}
	if h.deps.Pool == nil {
		h.responder.Error(apierrors.NewServiceUnavailable("clickhouse read pool not ready"))
		return
	}
	res, err := queryMetric(r.Context(), h.deps.Pool, p, base)
	if err != nil {
		h.responder.Error(toAPIError(err))
		return
	}
	// responder.Object encodes res through content negotiation (json / yaml /
	// protobuf per the Accept header), to the response writer the apiserver
	// bound this responder to.
	h.responder.Object(http.StatusOK, res)
}

// toAPIError maps a query failure to an apierror: a cancelled or deadline-
// exceeded context (the LazyPool blocking on a CH that is not up yet) surfaces
// as 503; an apimachinery Status passes through; everything else is a 500 (the
// SQL is server-built, so a real query error is our bug).
func toAPIError(err error) error {
	var statusErr *apierrors.StatusError
	if errors.As(err, &statusErr) {
		return statusErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return apierrors.NewServiceUnavailable("clickhouse not ready: " + err.Error())
	}
	return apierrors.NewInternalError(err)
}

// NewCatalogProvider adapts the catalog Lister/Getter to the apoxy-cli builder
// StorageProvider shape (the `metrics` resource).
func NewCatalogProvider(_ Deps) func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
	gr := metricsv1.SchemeGroupVersion.WithResource("metrics").GroupResource()
	return func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
		return &catalogStorage{gr: gr}, nil
	}
}

// NewFleetSeriesProvider adapts the fleet `metrics/series` connecter.
func NewFleetSeriesProvider(deps Deps) func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
	return func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
		return &seriesConnecter{deps: deps, kind: "", singular: "series"}, nil
	}
}

// NewEntitySeriesProvider adapts a per-entity `{agentmetrics}/series` connecter.
// kind selects the agent kind the path scope is derived from.
func NewEntitySeriesProvider(deps Deps, kind EntityKind) func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
	return func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
		return &seriesConnecter{deps: deps, kind: kind, singular: "series"}, nil
	}
}
