package apiserver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	apiserver "k8s.io/apiserver/pkg/server"
	genericfilters "k8s.io/apiserver/pkg/server/filters"
	apiserveropts "k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/apoxy-dev/apoxy/api/resource"
	builder "github.com/apoxy-dev/apoxy/pkg/apiserver/server/builder"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	clrkopenapi "github.com/apoxy-dev/clrk/api/generated"
	metricsv1alpha1 "github.com/apoxy-dev/clrk/api/metrics/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/apiserver/agentmetrics"
	"github.com/apoxy-dev/clrk/internal/apiserver/egressmetrics"
	"github.com/apoxy-dev/clrk/internal/apiserver/invocation"
	"github.com/apoxy-dev/clrk/internal/apiserver/invoke"
	"github.com/apoxy-dev/clrk/internal/apiserver/metricsquery"
	"github.com/apoxy-dev/clrk/internal/apiserver/telemetry"
	"github.com/apoxy-dev/clrk/internal/apiserver/workerpoolmetrics"
	"github.com/apoxy-dev/clrk/internal/invevent"
)

// Option configures the Manager.
type Option func(*options)

type options struct {
	sqlitePath          string
	sqliteConnArgs      map[string]string
	certDir             string
	bindAddress         string
	bindPort            int
	resources           []resource.Object
	clientConfig        *rest.Config
	disableAuth         bool
	insecureAllowPublic bool
	leaderElection      bool
	leaderElectID       string
	leaderElectNS       string
	metricsBindAddr     string
	healthBindAddr      string

	clickHouseAddress  string
	clickHouseDatabase string
	clickHouseTTLDays  int

	natsProvider              invocation.NATSProvider
	allowInvocationTestWrites bool
}

// WithSQLitePath sets the SQLite file path. Use "file::memory:" for in-memory.
func WithSQLitePath(path string) Option {
	return func(o *options) { o.sqlitePath = path }
}

// WithSQLiteConnArgs overrides the SQLite DSN parameters.
func WithSQLiteConnArgs(args map[string]string) Option {
	return func(o *options) { o.sqliteConnArgs = args }
}

// WithCertDir sets the TLS certificate directory. When empty, the apiserver
// generates self-signed certs on the fly and the manager uses an insecure
// loopback client.
func WithCertDir(dir string) Option {
	return func(o *options) { o.certDir = dir }
}

// WithBindAddress sets the apiserver bind address. Defaults to 127.0.0.1.
// Binding to a non-loopback address while authentication is disabled (the
// current default) requires WithInsecureAllowPublic — otherwise Start
// refuses to launch a publicly reachable, unauthenticated apiserver.
func WithBindAddress(addr string) Option {
	return func(o *options) { o.bindAddress = addr }
}

// WithBindPort sets the apiserver bind port. Defaults to 8443.
func WithBindPort(port int) Option {
	return func(o *options) { o.bindPort = port }
}

// WithResource registers a resource type with the apiserver.
func WithResource(obj resource.Object) Option {
	return func(o *options) { o.resources = append(o.resources, obj) }
}

// WithResources registers a batch of resource types.
func WithResources(objs ...resource.Object) Option {
	return func(o *options) { o.resources = append(o.resources, objs...) }
}

// WithClientConfig overrides the client *rest.Config used by the embedded
// ctrl.Manager. The default is a loopback insecure config.
func WithClientConfig(cfg *rest.Config) Option {
	return func(o *options) { o.clientConfig = cfg }
}

// WithDisableAuth disables authentication and authorization. Dev-only.
func WithDisableAuth() Option {
	return func(o *options) { o.disableAuth = true }
}

// WithInsecureAllowPublic acknowledges that the apiserver is intentionally
// being bound to a non-loopback address while authentication is disabled.
// Without this, Start refuses such a configuration to prevent accidental
// exposure of an unauthenticated control plane (e.g. a Service published
// on a host network or a docker -p mapping). Required only when
// disableAuth is true and bindAddress is non-loopback.
func WithInsecureAllowPublic() Option {
	return func(o *options) { o.insecureAllowPublic = true }
}

// WithLeaderElection enables controller-runtime leader election against the
// embedded apiserver using the given lease name and namespace.
func WithLeaderElection(id, namespace string) Option {
	return func(o *options) {
		o.leaderElection = true
		o.leaderElectID = id
		o.leaderElectNS = namespace
	}
}

// WithMetricsBindAddress sets the ctrl.Manager metrics endpoint.
func WithMetricsBindAddress(addr string) Option {
	return func(o *options) { o.metricsBindAddr = addr }
}

// WithHealthBindAddress sets the ctrl.Manager healthz endpoint.
func WithHealthBindAddress(addr string) Option {
	return func(o *options) { o.healthBindAddr = addr }
}

// WithClickHouseAddress overrides the address (host:port) the
// embedded-CH Invocation storage dials. Defaults to 127.0.0.1:9000
// (the loopback the in-cm ClickHouse supervisor exposes).
func WithClickHouseAddress(addr string) Option {
	return func(o *options) { o.clickHouseAddress = addr }
}

// WithClickHouseDatabase overrides the CH schema name. Defaults to
// "default" — same schema the otel_logs / otel_traces tables live in.
func WithClickHouseDatabase(db string) Option {
	return func(o *options) { o.clickHouseDatabase = db }
}

// WithClickHouseTTLDays overrides the Invocation row retention. The
// rendered TTL is enforced at the CH MergeTree level. Defaults to 90
// days.
func WithClickHouseTTLDays(d int) Option {
	return func(o *options) { o.clickHouseTTLDays = d }
}

// WithNATS supplies the embedded NATS/JetStream server the Invocation
// pub/sub uses: the materializing consumer, the Watch ordered
// consumers, and the test-write publisher. When nil (NATS disabled),
// Invocation Watch degrades to an empty watch and the test-write door
// stays 405.
func WithNATS(p invocation.NATSProvider) Option {
	return func(o *options) { o.natsProvider = p }
}

// WithAllowInvocationTestWrites opens the header-gated Create/Update
// door on the top-level Invocation resource (still requires the
// per-request X-Clrk-Invocation-Write header). Dev/CI only; leave off
// in production so the system-written billing record can't be forged.
func WithAllowInvocationTestWrites(allow bool) Option {
	return func(o *options) { o.allowInvocationTestWrites = allow }
}

func defaultOptions() *options {
	return &options{
		sqliteConnArgs: map[string]string{
			"cache":         "shared",
			"_journal_mode": "WAL",
			"_busy_timeout": "30000",
		},
		bindAddress:        "127.0.0.1",
		bindPort:           8443,
		disableAuth:        true,
		metricsBindAddr:    "0",
		healthBindAddr:     "0",
		clickHouseAddress:  "127.0.0.1:9000",
		clickHouseDatabase: "default",
		clickHouseTTLDays:  90,
	}
}

// isLoopbackBind reports whether the given bind address is loopback-only,
// i.e. unreachable from any other host. The empty string is treated as
// loopback because the apiserver later defaults it via SecureServingOptions.
func isLoopbackBind(addr string) bool {
	if addr == "" {
		return true
	}
	ip := net.ParseIP(addr)
	if ip == nil {
		// Hostname — assume non-loopback unless explicitly "localhost".
		return strings.EqualFold(addr, "localhost")
	}
	return ip.IsLoopback()
}

func (o *options) loopbackHost() string {
	ip := net.ParseIP(o.bindAddress)
	if ip != nil && ip.IsUnspecified() {
		return "localhost"
	}
	if o.bindAddress == "" {
		return "localhost"
	}
	return o.bindAddress
}

func (o *options) loopbackHostPort() string {
	return net.JoinHostPort(o.loopbackHost(), strconv.Itoa(o.bindPort))
}

// Manager runs an embedded apiserver and a controller-runtime manager bound
// to it. Callers register reconcilers on the ctrl.Manager returned by
// CtrlManager() after ReadyCh fires.
type Manager struct {
	ReadyCh chan error

	// StartCh gates the ctrl.Manager start. Start() signals ReadyCh after
	// the apiserver and ctrl.Manager are set up, then blocks on StartCh
	// before calling ctrl.Manager.Start. Callers (cmd/controller-manager,
	// clrk dev) use this window to SetupWithManager the reconcilers and
	// do APIService bootstrap that must happen before the ctrl.Manager
	// cache starts discovery.
	StartCh chan struct{}

	opts    *options
	ctrlMgr manager.Manager
	invPool *invocation.LazyPool
}

// New returns an unstarted Manager.
func New() *Manager {
	return &Manager{
		ReadyCh: make(chan error, 1),
		StartCh: make(chan struct{}),
	}
}

// CtrlManager returns the controller-runtime manager after Start signals
// ready.
func (m *Manager) CtrlManager() manager.Manager {
	return m.ctrlMgr
}

// InvocationPool returns the shared LazyPool backing the Invocation read
// model, or nil if startAPIServer hasn't run yet. Callers (the TaskAgent
// revision controller's active-execution counter) read through it after
// ReadyCh fires; the pool itself resolves asynchronously as the embedded
// ClickHouse comes up, and a Do before resolution blocks (bounded by the
// caller's ctx) then returns "storage unavailable" if the dial failed.
func (m *Manager) InvocationPool() *invocation.LazyPool {
	return m.invPool
}

// Start starts the apiserver, waits for readyz, builds the ctrl.Manager, and
// blocks on ctrl.Manager.Start.
func (m *Manager) Start(ctx context.Context, opts ...Option) error {
	o := defaultOptions()
	for _, fn := range opts {
		fn(o)
	}
	m.opts = o

	if o.disableAuth && !isLoopbackBind(o.bindAddress) && !o.insecureAllowPublic {
		err := fmt.Errorf("refusing to start: authentication is disabled and bind address %q is not loopback; pass WithInsecureAllowPublic (or --insecure-allow-public) to acknowledge exposing an unauthenticated apiserver", o.bindAddress)
		m.ReadyCh <- err
		return err
	}

	if err := m.startAPIServer(ctx); err != nil {
		m.ReadyCh <- err
		return err
	}

	clientConfig := o.clientConfig
	if clientConfig == nil {
		clientConfig = newLoopbackConfig(o.loopbackHostPort())
	}

	mgr, err := ctrl.NewManager(clientConfig, ctrl.Options{
		Scheme:                        Scheme,
		LeaderElection:                o.leaderElection,
		LeaderElectionID:              o.leaderElectID,
		LeaderElectionNamespace:       o.leaderElectNS,
		LeaderElectionResourceLock:    "leases",
		LeaderElectionReleaseOnCancel: true,
		HealthProbeBindAddress:        o.healthBindAddr,
		Metrics:                       metricsserver.Options{BindAddress: o.metricsBindAddr},
	})
	if err != nil {
		m.ReadyCh <- err
		return fmt.Errorf("building ctrl.Manager: %w", err)
	}
	m.ctrlMgr = mgr

	close(m.ReadyCh)

	if m.invPool != nil {
		defer m.invPool.Close()
	}

	// Block until the caller is done registering reconcilers and doing
	// any APIService / CRD bootstrap it needs. Without this, ctrl.Manager
	// would start its cache discovery against an apiserver that doesn't
	// yet know about the aggregated types.
	select {
	case <-m.StartCh:
	case <-ctx.Done():
		return ctx.Err()
	}

	slog.Info("Starting controller-runtime manager")
	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("ctrl.Manager failed: %w", err)
	}
	return nil
}

func (m *Manager) startAPIServer(ctx context.Context) error {
	o := m.opts

	logrus.SetOutput(io.Discard)
	klog.SetLogger(logr.New(klogSlogAdapter{}))

	if o.sqlitePath != "" && !strings.Contains(o.sqlitePath, ":memory:") {
		if _, err := os.Stat(o.sqlitePath); os.IsNotExist(err) {
			if err := os.MkdirAll(filepath.Dir(o.sqlitePath), 0o755); err != nil {
				return fmt.Errorf("creating database directory: %w", err)
			}
			if _, err := os.Create(o.sqlitePath); err != nil {
				return fmt.Errorf("creating database file: %w", err)
			}
		}
	}

	kineStore, err := newKineStorage(ctx, o.sqlitePath, o.sqliteConnArgs, "json")
	if err != nil {
		return fmt.Errorf("creating kine storage: %w", err)
	}

	// Opt into the apoxy-cli builder's CRD-style metadata.generation
	// handling: PrepareForCreate seeds generation=1, PrepareForUpdate
	// bumps it on Spec changes. Required for revision controllers that
	// key off da.Generation (e.g. DaemonAgentRevisionReconciler), and
	// for any controller using GenerationChangedPredicate.
	// See APO-508.
	srvBuilder := builder.NewServerBuilder().WithGenerationTracking()
	for _, r := range o.resources {
		srvBuilder = srvBuilder.WithResourceAndStorage(r, kineStore)
	}

	// Invocation: real ClickHouse-backed storage at three GVRs
	// (top-level + per-parent subresources) sharing one chpool. Not
	// registered via WithResourceAndStorage because we want our own
	// rest.Storage rather than the builder's kine-backed generic
	// store.
	//
	// Both ClickHouse and the JetStream bus come up in a background
	// goroutine: cmd/controller-manager starts the apiserver Manager
	// before the embedded clickhouse supervisor binds 9000 and before
	// the NATS server accepts connections, so synchronous setup would
	// hold the /healthz bind and trip the pod's liveness probe. The
	// LazyPool Doer blocks individual Invocation requests until CH
	// resolves; the Bus leaves Watch as an empty watch and the
	// test-write door at 405 until NATS resolves; everything else
	// (Discovery, kine-backed resources, ctrl.Manager) is unaffected.
	lazyPool := invocation.NewLazyPool()
	m.invPool = lazyPool
	highWater := new(atomic.Uint64)
	bus := invocation.NewBus()
	go m.runInvocationBackend(ctx, lazyPool, bus, highWater)

	deps := invocation.Deps{
		Pool:            lazyPool,
		HighWater:       highWater,
		Bus:             bus,
		AllowTestWrites: o.allowInvocationTestWrites,
	}
	invocationGR := schema.GroupResource{
		Group:    clrkv1alpha1.GroupName,
		Resource: "invocations",
	}
	srvBuilder = srvBuilder.WithAdditionalSchemeInstallers(clrkv1alpha1.Install)
	for _, sub := range []struct {
		resource   string
		parentKind clrkv1alpha1.InvocationParentKind
	}{
		{"invocations", ""},
		{"taskagents/invocations", clrkv1alpha1.InvocationParentTaskAgent},
		{"daemonagents/invocations", clrkv1alpha1.InvocationParentDaemonAgent},
	} {
		srvBuilder = srvBuilder.WithStorage(
			clrkv1alpha1.SchemeGroupVersion.WithResource(sub.resource),
			invocation.NewProvider(deps, invocationGR, "invocation", sub.parentKind),
		)
	}

	// taskagents/{name}/invoke: the run-task write-side connect
	// subresource. Pure transport -- it reverse-proxies to the per-TA
	// ingress Envoy, which admits the request and publishes the lifecycle
	// -- so it shares no state with the read-model Storage above. The kube
	// client is resolved lazily: m.ctrlMgr is created after
	// startAPIServer, but Connect only runs once the control plane is up.
	// EG provisions every TaskAgent's data-plane Service in the
	// controller-manager's own namespace (ENVOY_GATEWAY_NAMESPACE), so the
	// Connecter resolves clrk-<ta> there, not in the TaskAgent's namespace.
	systemNS := os.Getenv("POD_NAMESPACE")
	if systemNS == "" {
		systemNS = "clrk"
	}
	srvBuilder = srvBuilder.WithStorage(
		clrkv1alpha1.SchemeGroupVersion.WithResource("taskagents/invoke"),
		invoke.NewProvider("invoke", systemNS, func() client.Client {
			mgr := m.CtrlManager()
			if mgr == nil {
				return nil
			}
			return mgr.GetClient()
		}),
	)

	// taskagents/{name}/{logs,traces} + daemonagents/{name}/{logs,traces}:
	// the OTel read API. Each Connecter reads the embedded ClickHouse
	// otel_logs / otel_traces tables (the same LazyPool the Invocation
	// read model dials) and reconstructs OTLP/JSON scoped to the agent
	// named in the path. agent.kind pins the parent kind so a TaskAgent
	// and a DaemonAgent sharing a name can't read each other's telemetry.
	telemetryDeps := telemetry.Deps{Pool: lazyPool}
	for _, sub := range []struct {
		resource  string
		signal    telemetry.Signal
		agentKind string
	}{
		{"taskagents/logs", telemetry.SignalLogs, clrkv1alpha1.AgentKindTask},
		{"taskagents/traces", telemetry.SignalTraces, clrkv1alpha1.AgentKindTask},
		{"daemonagents/logs", telemetry.SignalLogs, clrkv1alpha1.AgentKindDaemon},
		{"daemonagents/traces", telemetry.SignalTraces, clrkv1alpha1.AgentKindDaemon},
	} {
		srvBuilder = srvBuilder.WithStorage(
			clrkv1alpha1.SchemeGroupVersion.WithResource(sub.resource),
			telemetry.NewProvider(telemetryDeps, sub.signal, string(sub.signal), sub.agentKind),
		)
	}

	// metrics.clrk.apoxy.dev/v1alpha1: the Tier-1 snapshot surface. Two
	// listable kinds -- taskagentmetrics / daemonagentmetrics -- back the
	// console agents page with a per-agent {timestamp, window, usage}
	// rollup, computed at read time by aggregating the same otel_traces
	// table the telemetry read API serves (no separate metrics store, no
	// MeterProvider). Registered like Invocation: a custom rest.Storage
	// over the shared LazyPool, not the kine-backed generic store. Its own
	// aggregated group, so it needs its own scheme installer.
	srvBuilder = srvBuilder.WithAdditionalSchemeInstallers(metricsv1alpha1.Install)
	// Resolved per request: the Storage is built before the ctrl.Manager
	// client exists, but List/Get enumerate the CRs only once the control
	// plane is up (same lazy pattern as the invoke subresource).
	metricsClient := func() client.Client {
		mgr := m.CtrlManager()
		if mgr == nil {
			return nil
		}
		return mgr.GetClient()
	}
	agentMetricsDeps := agentmetrics.Deps{
		Pool:   lazyPool,
		Client: metricsClient,
	}
	srvBuilder = srvBuilder.WithStorage(
		metricsv1alpha1.SchemeGroupVersion.WithResource("taskagentmetrics"),
		agentmetrics.NewTaskAgentMetricsProvider(agentMetricsDeps),
	)
	srvBuilder = srvBuilder.WithStorage(
		metricsv1alpha1.SchemeGroupVersion.WithResource("daemonagentmetrics"),
		agentmetrics.NewDaemonAgentMetricsProvider(agentMetricsDeps),
	)

	// egressgatewaymetrics: the Tier-1 gateway snapshot (the egress list +
	// miller view). Same read-time aggregation over otel_traces, keyed by
	// the EGRef leading sort key; the per-listener/route nesting joins the
	// scan's route-attributed rows to the route CR topology.
	srvBuilder = srvBuilder.WithStorage(
		metricsv1alpha1.SchemeGroupVersion.WithResource("egressgatewaymetrics"),
		egressmetrics.NewEgressGatewayMetricsProvider(egressmetrics.Deps{
			Pool:   lazyPool,
			Client: metricsClient,
		}),
	)

	// workerpoolmetrics: the Tier-1 pool snapshot (the console pools list).
	// A flat {timestamp, window, usage} object per WorkerPool joining
	// point-in-time CR gauges (replica counts, execution-slot capacity,
	// in-flight executions) to a window aggregate over the pool's
	// ingress.dispatch spans (invocations dispatched, dispatch errors,
	// dispatch-latency percentiles). No series subresource: the Tier-2
	// catalog recipes read agent/gateway spans that never carry
	// clrk.worker.pool, so a pool time-series needs its own dispatch-sourced
	// recipes rather than a scope over the existing catalog (follow-up).
	srvBuilder = srvBuilder.WithStorage(
		metricsv1alpha1.SchemeGroupVersion.WithResource("workerpoolmetrics"),
		workerpoolmetrics.NewWorkerPoolMetricsProvider(workerpoolmetrics.Deps{
			Pool:   lazyPool,
			Client: metricsClient,
		}),
	)

	// Tier-2: the metric catalog + time-series query (the charts the Tier-1
	// snapshot shape can't carry -- labeled series, histogram percentiles,
	// free-dimension groupBy). Same query-time aggregation over otel_traces /
	// otel_logs, no separate metrics store; all typed kinds in the metrics group
	// (internal/apiserver/metricsquery). The catalog is the `metrics` resource
	// (a Lister/Getter over the recipe registry); the query is its `series`
	// connect subresource, returning a typed MetricSeriesSet via content
	// negotiation. Per-agent scoping rides taskagentmetrics/daemonagentmetrics as
	// their own `series` subresource (the metrics analogue of pods/log), so the
	// scope is path-derived and server-enforced; per-gateway and cross-cutting
	// scopes use the fleet metrics/{id}/series with scopeKind / scopeName.
	metricsQueryDeps := metricsquery.Deps{Pool: lazyPool}
	srvBuilder = srvBuilder.WithStorage(
		metricsv1alpha1.SchemeGroupVersion.WithResource("metrics"),
		metricsquery.NewCatalogProvider(metricsQueryDeps),
	)
	srvBuilder = srvBuilder.WithStorage(
		metricsv1alpha1.SchemeGroupVersion.WithResource("metrics/series"),
		metricsquery.NewFleetSeriesProvider(metricsQueryDeps),
	)
	for _, sub := range []struct {
		resource string
		kind     metricsquery.EntityKind
	}{
		{"taskagentmetrics/series", metricsquery.EntityTaskAgent},
		{"daemonagentmetrics/series", metricsquery.EntityDaemonAgent},
		{"egressgatewaymetrics/series", metricsquery.EntityEgressGateway},
	} {
		srvBuilder = srvBuilder.WithStorage(
			metricsv1alpha1.SchemeGroupVersion.WithResource(sub.resource),
			metricsquery.NewEntitySeriesProvider(metricsQueryDeps, sub.kind),
		)
	}

	// events.k8s.io/v1: the real Kubernetes Events API, served by the embedded
	// apiserver and backed by the shared kine store (persistent + watchable).
	// This is where control-plane notifications live -- the controllers and the
	// data-plane security bridge record Events here via a client-go events
	// broadcaster pointed at EmbeddedLoopbackConfig(), and the console lists and
	// watches them. clrk owns Event GC (internal/notify.Pruner); this only serves
	// them, on the embedded/loopback surface (never aggregated -- see WithEvents).
	srvBuilder = srvBuilder.WithEvents(kineStore)

	if o.disableAuth {
		srvBuilder = srvBuilder.DisableAuthorization()
	}

	serverOpts, err := srvBuilder.
		WithOpenAPIDefinitions("clrk", "0.1.0", clrkopenapi.GetOpenAPIDefinitions).
		WithOptionsFns(func(so *builder.ServerOptions) *builder.ServerOptions {
			so.StdErr = io.Discard
			so.StdOut = io.Discard

			so.RecommendedOptions.CoreAPI = nil
			if o.disableAuth {
				so.RecommendedOptions.Authentication = nil
				so.RecommendedOptions.Authorization = nil
			}
			so.RecommendedOptions.Admission = nil

			// Priority-and-fairness needs a core Kubernetes client (kube-apiserver
			// FlowSchema/PriorityLevelConfiguration). We run standalone, so disable
			// it; otherwise Features.ApplyTo fails validation before the apiserver
			// can start.
			if so.RecommendedOptions.Features != nil {
				so.RecommendedOptions.Features.EnablePriorityAndFairness = false
			}

			secure := &apiserveropts.SecureServingOptionsWithLoopback{
				SecureServingOptions: &apiserveropts.SecureServingOptions{
					BindAddress: netutils.ParseIPSloppy(o.bindAddress),
					BindPort:    o.bindPort,
				},
			}
			if o.certDir != "" {
				secure.ServerCert.CertDirectory = o.certDir
				secure.ServerCert.PairName = "tls"
			}
			so.RecommendedOptions.SecureServing = secure
			return so
		}).
		WithConfigFns(func(c *apiserver.RecommendedConfig) *apiserver.RecommendedConfig {
			// FlowControl post-start hook LISTs FlowSchema / PriorityLevelConfiguration
			// against CoreAPI, which we don't run. Nilling it prevents readyz from failing.
			c.FlowControl = nil

			// The {logs,traces} follow subresources stream for as long as
			// the client watches (?follow=true polls ClickHouse on an
			// interval). Without intervention the generic apiserver's
			// default 60s request timeout fires mid-stream -- after the 200
			// and headers are already flushed -- aborting the handler with
			// http.ErrAbortHandler and resetting the HTTP/2 stream (the
			// client sees "stream error ... INTERNAL_ERROR"). Mark a follow
			// request long-running so this apiserver's
			// WithTimeoutForNonLongRunningRequests filter leaves it alone;
			// a non-follow GET stays short and keeps the 60s safety bound.
			//
			// This fn runs twice (start.go brackets ApplyTo with two config
			// passes), so rebuild the generic default rather than chain to
			// c.LongRunningFunc, which would compound the wrapper on the
			// second pass.
			//
			// Note: the FRONT kube-apiserver aggregator imposes its own 60s
			// wall on the same request (a named subresource is never
			// long-running there) which clrk cannot configure; the CLI
			// follow loop reconnects to ride across it. This override
			// removes the backend as a second, redundant 60s cap and keeps
			// a direct (non-aggregated) reader's stream open.
			defaultLongRunning := genericfilters.BasicLongRunningRequestCheck(sets.NewString("watch"), sets.NewString())
			c.LongRunningFunc = func(r *http.Request, ri *apirequest.RequestInfo) bool {
				return defaultLongRunning(r, ri) || IsTelemetryFollowRequest(r, ri)
			}

			// Test-write door: when enabled, an outermost middleware
			// stamps the request context for requests carrying the
			// X-Clrk-Invocation-Write header so the Invocation Storage
			// permits Create/Update on the top-level resource. Off by
			// default, so production never installs this middleware and
			// the header is inert.
			if o.allowInvocationTestWrites {
				prev := c.BuildHandlerChainFunc
				if prev == nil {
					prev = apiserver.DefaultBuildHandlerChain
				}
				c.BuildHandlerChainFunc = func(apiHandler http.Handler, cfg *apiserver.Config) http.Handler {
					chain := prev(apiHandler, cfg)
					return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.Header.Get(invocation.TestWriteHeader) == invocation.TestWriteHeaderValue {
							r = r.WithContext(invocation.WithTestWrite(r.Context()))
						}
						chain.ServeHTTP(w, r)
					})
				}
			}
			return c
		}).
		WithoutEtcd().
		Build()
	if err != nil {
		return fmt.Errorf("building apiserver: %w", err)
	}

	if _, err := serverOpts.RunApoxyServer(ctx); err != nil {
		return fmt.Errorf("starting apiserver: %w", err)
	}

	if err := waitForReadyz("https://"+o.loopbackHostPort(), 300*time.Second); err != nil {
		return fmt.Errorf("waiting for /readyz: %w", err)
	}
	slog.Info("CLRK apiserver is ready", "host", o.loopbackHostPort())
	return nil
}

func newLoopbackConfig(hostPort string) *rest.Config {
	return &rest.Config{
		QPS:             -1,
		Host:            "https://" + hostPort,
		TLSClientConfig: rest.TLSClientConfig{Insecure: true},
		UserAgent:       "clrk-apiserver",
	}
}

// EmbeddedLoopbackConfig returns a rest.Config for the embedded apiserver's
// loopback endpoint. The notifications recorder and the advisory materializer
// use it to read/write events.k8s.io/v1 Events against clrk's own apiserver --
// never the customer cluster, whose built-in Events group an APIService cannot
// override. Valid only after Start has brought the apiserver up (the caller
// resolves it lazily, like the invoke/agentmetrics client seams).
func (m *Manager) EmbeddedLoopbackConfig() *rest.Config {
	return newLoopbackConfig(m.opts.loopbackHostPort())
}

// dialDeadline caps how long the background goroutine retries the CH
// dial before giving up and resolving the LazyPool with an error. The
// dev controller-manager image ships without the embedded clickhouse
// binary, so without a deadline the dial would loop indefinitely and
// every Invocation request would block until ctx.Done; with one, the
// LazyPool returns "storage unavailable" promptly and the rest of the
// apiserver is unaffected.
const dialDeadline = 30 * time.Second

// runInvocationBackend brings up the Invocation read/write backend in
// the background so apiserver startup never blocks on ClickHouse or
// NATS. It dials CH, ensures the events table, and resolves the
// LazyPool; then (when NATS is configured) connects in-process,
// resolves the Bus with a JetStream handle + Publisher, and runs the
// JetStream->ClickHouse materializer for ctx's lifetime. Failures are
// logged and leave the storage degraded (Invocation reads/writes
// unavailable) rather than crashing the control plane.
func (m *Manager) runInvocationBackend(ctx context.Context, lazyPool *invocation.LazyPool, bus *invocation.Bus, highWater *atomic.Uint64) {
	o := m.opts
	chPool, err := dialClickHouseWithRetry(ctx, o)
	if err != nil {
		lazyPool.Set(nil, err)
		return
	}
	if err := invocation.EnsureTable(ctx, chPool, o.clickHouseTTLDays); err != nil {
		chPool.Close()
		lazyPool.Set(nil, err)
		return
	}
	lazyPool.Set(chPool, nil)

	if o.natsProvider == nil {
		// NATS disabled: Watch stays an empty watch and the test-write
		// door stays 405. List/Get still work against ClickHouse.
		return
	}
	if err := o.natsProvider.Ready(ctx); err != nil {
		if ctx.Err() == nil {
			slog.Error("Invocation backend: NATS not ready", "err", err)
		}
		return
	}
	nc, err := o.natsProvider.Connect("clrk-apiserver-invocations")
	if err != nil {
		slog.Error("Invocation backend: NATS connect", "err", err)
		return
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		slog.Error("Invocation backend: jetstream", "err", err)
		return
	}
	bus.Set(js, invevent.NewPublisher(js))

	// Materialize JetStream -> ClickHouse for the process lifetime.
	if err := invocation.NewConsumer(lazyPool, js, highWater).Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("Invocation materializer exited", "err", err)
	}
}

// dialClickHouseWithRetry calls chpool.Dial until it succeeds, ctx is
// cancelled, or dialDeadline elapses. Each attempt has a 5s timeout
// (matches chwriter's dialTimeout); attempts are spaced by an
// exponential backoff capped at 2s.
func dialClickHouseWithRetry(ctx context.Context, o *options) (*chpool.Pool, error) {
	deadlineCtx, cancelDeadline := context.WithTimeout(ctx, dialDeadline)
	defer cancelDeadline()
	backoff := 200 * time.Millisecond
	const maxBackoff = 2 * time.Second
	var lastErr error
	for {
		dialCtx, cancel := context.WithTimeout(deadlineCtx, 5*time.Second)
		pool, err := chpool.Dial(dialCtx, chpool.Options{
			ClientOptions: ch.Options{
				Address:  o.clickHouseAddress,
				Database: o.clickHouseDatabase,
			},
		})
		cancel()
		if err == nil {
			return pool, nil
		}
		lastErr = err
		select {
		case <-deadlineCtx.Done():
			return nil, fmt.Errorf("dialing clickhouse at %s: %w (last: %v)", o.clickHouseAddress, deadlineCtx.Err(), lastErr)
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func waitForReadyz(url string, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	retry := 200 * time.Millisecond
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Timeout: retry,
	}
	for {
		resp, err := client.Get(url + "/readyz")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		select {
		case <-deadline.C:
			return errors.New("timed out waiting for readyz")
		case <-time.After(retry):
		}
	}
}
