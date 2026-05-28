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
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/go-logr/logr"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apiserver "k8s.io/apiserver/pkg/server"
	apiserveropts "k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/apoxy-dev/apoxy/api/resource"
	builder "github.com/apoxy-dev/apoxy/pkg/apiserver/server/builder"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	clrkopenapi "github.com/apoxy-dev/clrk/api/generated"
	"github.com/apoxy-dev/clrk/internal/apiserver/invocation"
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
	// The CH dial happens in a goroutine: cmd/controller-manager
	// starts the apiserver Manager before the embedded clickhouse
	// supervisor binds 9000, so a synchronous dial would hold the
	// /healthz bind and trip the pod's liveness probe. Storage's
	// LazyPool Doer blocks individual Invocation requests until the
	// pool resolves; everything else (Discovery, kine-backed
	// resources, ctrl.Manager) is unaffected by CH availability.
	lazyPool := invocation.NewLazyPool()
	m.invPool = lazyPool
	go func() {
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
	}()

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
			invocation.NewProvider(lazyPool, invocationGR, "invocation", sub.parentKind),
		)
	}

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

// dialClickHouseWithRetry calls chpool.Dial until it succeeds or ctx
// is cancelled. Each attempt has a 5s timeout (matches chwriter's
// dialTimeout); attempts are spaced by an exponential backoff capped
// at 2s. Returns the first non-nil pool or ctx.Err() wrapped with a
// hint about the address that failed.
func dialClickHouseWithRetry(ctx context.Context, o *options) (*chpool.Pool, error) {
	backoff := 200 * time.Millisecond
	const maxBackoff = 2 * time.Second
	var lastErr error
	for {
		dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
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
		case <-ctx.Done():
			return nil, fmt.Errorf("dialing clickhouse at %s: %w (last: %v)", o.clickHouseAddress, ctx.Err(), lastErr)
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
