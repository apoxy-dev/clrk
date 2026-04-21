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

	"github.com/go-logr/logr"
	"github.com/sirupsen/logrus"
	apiserver "k8s.io/apiserver/pkg/server"
	apiserveropts "k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/apoxy-dev/apoxy/api/resource"
	builder "github.com/apoxy-dev/apoxy/pkg/apiserver/server/builder"

	clrkopenapi "github.com/apoxy-dev/clrk/api/generated"
)

// Option configures the Manager.
type Option func(*options)

type options struct {
	sqlitePath      string
	sqliteConnArgs  map[string]string
	certDir         string
	bindAddress     string
	bindPort        int
	resources       []resource.Object
	clientConfig    *rest.Config
	disableAuth     bool
	leaderElection  bool
	leaderElectID   string
	leaderElectNS   string
	metricsBindAddr string
	healthBindAddr  string
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

// WithBindAddress sets the apiserver bind address. Defaults to 0.0.0.0.
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

func defaultOptions() *options {
	return &options{
		sqliteConnArgs: map[string]string{
			"cache":         "shared",
			"_journal_mode": "WAL",
			"_busy_timeout": "30000",
		},
		bindAddress:     "0.0.0.0",
		bindPort:        8443,
		disableAuth:     true,
		metricsBindAddr: "0",
		healthBindAddr:  "0",
	}
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

	opts    *options
	ctrlMgr manager.Manager
}

// New returns an unstarted Manager.
func New() *Manager {
	return &Manager{ReadyCh: make(chan error, 1)}
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
	})
	if err != nil {
		m.ReadyCh <- err
		return fmt.Errorf("building ctrl.Manager: %w", err)
	}
	m.ctrlMgr = mgr

	close(m.ReadyCh)

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

	srvBuilder := builder.NewServerBuilder()
	for _, r := range o.resources {
		srvBuilder = srvBuilder.WithResourceAndStorage(r, kineStore)
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
