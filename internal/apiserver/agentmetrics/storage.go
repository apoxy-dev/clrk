// Package agentmetrics backs the Tier-1 metrics.clrk.apoxy.dev snapshot
// kinds — TaskAgentMetrics and DaemonAgentMetrics — with query-time
// ClickHouse aggregation over the otel_traces table. Each List/Get is a
// scoped conditional-aggregate scan that computes one UsageList per agent
// over a stated look-back window. There is no separate metrics store and
// no MeterProvider: the agent's spans already carry every dimension the
// rollup sums or counts, so the snapshot is a named aggregation recipe
// over them (see apoxy-cloud/docs/clrk-metrics-api.md).
//
// The shape mirrors metrics.k8s.io (PodMetrics / NodeMetrics): listable,
// point-in-time {timestamp, window, usage} objects whose name/namespace
// match the agent they summarize, so the console agents page is one List
// call rather than a get-per-agent fan-out.
//
// Agents are enumerated from their CRs (TaskAgent / DaemonAgent), not from
// telemetry, so an idle agent with no spans in the window still appears
// (with zero usage), label selectors filter on the agent's labels, and a
// Get on a nonexistent agent is a proper 404. The window aggregates come
// from ClickHouse; the `active` gauge is the in-flight count read from the
// agent's CR status — the one value that is a point-in-time read rather
// than a window aggregate (TaskAgent only).
package agentmetrics

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

	metricsv1 "github.com/apoxy-dev/clrk/api/metrics/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/apiserver/chsql"
)

// Doer is the shared ClickHouse-pool seam (internal/apiserver/chsql),
// aliased here so the package's Deps/field references stay local. Unit
// tests inject a fake that records the issued ch.Query and populates
// Result columns from a canned dataset.
type Doer = chsql.Doer

// ClientFunc resolves the controller-runtime client used to enumerate the
// agent CRs. It is evaluated per request rather than captured at
// construction because the apiserver Storage is built before the
// controller-manager's client exists; by the time List/Get runs the
// manager is up and its cache is warm.
type ClientFunc func() client.Client

// defaultWindow is the rollup look-back when Deps.Window is unset — the
// 24h the console agents page renders.
const defaultWindow = 24 * time.Hour

// Deps are the dependencies shared by both metrics GVRs.
type Deps struct {
	// Pool is the ClickHouse read pool (the shared LazyPool in prod).
	Pool Doer
	// Client resolves the kube client used to enumerate agent CRs.
	Client ClientFunc
	// Window is the rollup look-back. Zero means defaultWindow.
	Window time.Duration
}

// Storage is the rest.Storage backing one metrics GVR. The per-kind
// behavior is delegated to a kindAdapter so TaskAgentMetrics and
// DaemonAgentMetrics share this implementation.
type Storage struct {
	deps           Deps
	adapter        kindAdapter
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

// newStorage constructs a Storage for the given kind adapter.
func newStorage(deps Deps, adapter kindAdapter) *Storage {
	gr := schema.GroupResource{Group: metricsv1.GroupName, Resource: adapter.resource()}
	return &Storage{
		deps:           deps,
		adapter:        adapter,
		gr:             gr,
		tableConvertor: registryrest.NewDefaultTableConvertor(gr),
	}
}

// NewTaskAgentMetricsProvider returns the StorageProvider for the
// taskagentmetrics GVR. The *runtime.Scheme / RESTOptionsGetter args are
// unused (storage is computed, not kept in the generic registry).
func NewTaskAgentMetricsProvider(deps Deps) func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
	return func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
		return newStorage(deps, taskKind{}), nil
	}
}

// NewDaemonAgentMetricsProvider returns the StorageProvider for the
// daemonagentmetrics GVR.
func NewDaemonAgentMetricsProvider(deps Deps) func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
	return func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
		return newStorage(deps, daemonKind{}), nil
	}
}

func (s *Storage) New() runtime.Object     { return s.adapter.newObject() }
func (s *Storage) NewList() runtime.Object { return s.adapter.newList() }
func (s *Storage) Destroy()                {}
func (s *Storage) NamespaceScoped() bool   { return true }
func (s *Storage) GetSingularName() string { return s.adapter.resource() }

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

// List returns one snapshot per agent of this kind in scope. Agents are
// enumerated from their CRs and joined against a single GROUP BY Agent
// ClickHouse scan over the window, so every agent appears — idle ones
// with zero usage. The list is not watchable and carries no
// resourceVersion: every read is a fresh aggregation, matching
// metrics-server semantics.
//
// It is intentionally unpaginated (no LIMIT / continue token), like
// metrics.k8s.io PodMetrics lists: a snapshot is one object per agent in
// scope. The response size is bounded by the number of agent CRs in
// scope, not by span volume — the GROUP BY collapses every span of an
// agent into a single row, and only rows that join an enumerated CR are
// emitted. A LIMIT is deliberately avoided: without a total ordering over
// a stable key it would silently drop real agents rather than bound cost.
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

	refs, err := s.adapter.listAgents(ctx, c, ns, sel)
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("list %s agents: %w", s.adapter.resource(), err))
	}

	now := time.Now().UTC()
	window := s.window()
	rows, err := scanUsage(ctx, s.deps.Pool, listBody(s.adapter.agentKind(), ns, now.Add(-window), now))
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("list %s: %w", s.adapter.resource(), err))
	}
	// Join the CH aggregates to the enumerated CRs. In a namespaced list
	// the scan's WHERE pins agent.namespace = ns, so every row shares that
	// namespace: key by agent name alone, which is exact even if a span
	// carried an empty agent.namespace (it was already excluded by the
	// WHERE). A cluster-scoped list spans namespaces, so it must key by
	// namespace/name to keep two same-named agents apart; a row whose
	// telemetry-derived namespace is empty can't be placed by that key, so
	// it is recovered by name only when the name is unambiguous among the
	// CRs (otherwise it is left unattributed rather than risk crediting the
	// wrong agent).
	namespaced := ns != ""
	byKey := make(map[string]agentUsage, len(rows))
	orphanByName := make(map[string]agentUsage) // cluster scope: empty-namespace rows
	for _, r := range rows {
		switch {
		case namespaced:
			byKey[r.agent] = r
		case r.namespace == "":
			orphanByName[r.agent] = r
		default:
			byKey[r.namespace+"/"+r.agent] = r
		}
	}
	nameCount := make(map[string]int, len(refs))
	if !namespaced {
		for _, ref := range refs {
			nameCount[ref.name]++
		}
	}

	list := s.adapter.newList()
	ts := metav1.NewTime(now)
	dur := metav1.Duration{Duration: window}
	for _, ref := range refs {
		var u agentUsage
		switch {
		case namespaced:
			u = byKey[ref.name]
		default:
			if hit, ok := byKey[ref.namespace+"/"+ref.name]; ok {
				u = hit
			} else if orphan, ok := orphanByName[ref.name]; ok && nameCount[ref.name] == 1 {
				u = orphan
			}
		}
		s.adapter.appendTo(list, s.buildObject(ref, u, ts, dur))
	}
	return list, nil
}

// Get returns the snapshot for one named agent. The agent CR must exist
// (a missing agent is a 404); an agent with no spans in the window yields
// a zero-usage snapshot rather than an error.
func (s *Storage) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	c := s.client()
	if c == nil {
		return nil, apierrors.NewServiceUnavailable("control plane not ready")
	}
	ns, _ := request.NamespaceFrom(ctx)

	ref, err := s.adapter.getAgent(ctx, c, ns, name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, apierrors.NewNotFound(s.gr, name)
		}
		return nil, apierrors.NewInternalError(fmt.Errorf("get %s agent: %w", s.adapter.resource(), err))
	}

	now := time.Now().UTC()
	window := s.window()
	rows, err := scanUsage(ctx, s.deps.Pool, getBody(s.adapter.agentKind(), ns, name, now.Add(-window), now))
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("get %s: %w", s.adapter.resource(), err))
	}
	var u agentUsage
	if len(rows) > 0 {
		u = rows[0]
	}
	return s.buildObject(ref, u, metav1.NewTime(now), metav1.Duration{Duration: window}), nil
}

// buildObject assembles one snapshot: the CH-derived usage list plus the
// kind's point-in-time status gauge under its gaugeKey -- `warm` (pre-warmed
// sandbox count) for a TaskAgent, `running` (0/1 process liveness) for a
// DaemonAgent.
//
// The gauge is read from the agent CR status, NOT recomputed here, by
// deliberate choice. Warm capacity (TaskAgent.Status.WarmSandboxes) is the
// per-worker WarmCount summed by the revision controller off the
// WorkerStatus stream; running is projected from DaemonAgent.Status.Phase.
// Re-deriving either in the storage would fan out a per-agent read in a List
// (vs the single GROUP BY scan) or fork the controller's rollup logic into a
// parallel copy. More importantly the CR status is the single source of
// truth: the metrics gauge agreeing with `kubectl get taskagent` / `kubectl
// get daemonagent` is the least-surprising behavior, where an
// independently-recomputed value could silently disagree. The trade-off is a
// bounded reconcile-cadence staleness on the gauge (every other usage key is
// a fresh window aggregate).
func (s *Storage) buildObject(ref agentRef, u agentUsage, ts metav1.Time, window metav1.Duration) runtime.Object {
	usage := u.usageList(s.adapter.includeLatency())
	if key := s.adapter.gaugeKey(); key != "" {
		usage[key] = *resource.NewQuantity(int64(ref.gauge), resource.DecimalSI)
	}
	return s.adapter.object(ref, ts, window, usage)
}
