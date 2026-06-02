package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	envoytlsv3 "github.com/apoxy-dev/envoy-go/envoy/service/tls/v3"
	egextpb "github.com/envoyproxy/gateway/proto/extension"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/admin"
	"github.com/apoxy-dev/clrk/internal/apiserver"
	"github.com/apoxy-dev/clrk/internal/apiserver/invocation"
	"github.com/apoxy-dev/clrk/internal/certprovider"
	"github.com/apoxy-dev/clrk/internal/chwriter"
	"github.com/apoxy-dev/clrk/internal/clickhouse"
	"github.com/apoxy-dev/clrk/internal/controller"
	"github.com/apoxy-dev/clrk/internal/crds"
	"github.com/apoxy-dev/clrk/internal/egcontrolplane"
	"github.com/apoxy-dev/clrk/internal/egextension"
	"github.com/apoxy-dev/clrk/internal/egidentity"
	"github.com/apoxy-dev/clrk/internal/extproc"
	ingressextproc "github.com/apoxy-dev/clrk/internal/extproc/ingress"
	"github.com/apoxy-dev/clrk/internal/extproc/invocationctx"
	"github.com/apoxy-dev/clrk/internal/healthcheck"
	clrknats "github.com/apoxy-dev/clrk/internal/nats"
	"github.com/apoxy-dev/clrk/internal/otelemit"
	"github.com/apoxy-dev/clrk/internal/otelforward"
	"github.com/apoxy-dev/clrk/internal/otelreceiver"
)

const (
	defaultEnvoyImage = "us-west1-docker.pkg.dev/apoxy-dev/public/envoy:665aebbf"
	defaultGRPCPort   = 9443
	// defaultNamespace is the controller-manager's fallback runtime
	// namespace when POD_NAMESPACE is unset (e.g. `go run`,
	// out-of-cluster invocations). Production runs in-cluster set
	// POD_NAMESPACE via downward API. Threaded into EG's control
	// plane (`ENVOY_GATEWAY_NAMESPACE`) and every clrk-side reference
	// to the controller's runtime namespace.
	defaultNamespace = "clrk"
)

// runtimeNamespace returns the namespace this process is running in,
// from POD_NAMESPACE (downward API) or the fallback constant. Same
// pattern as cmd/worker/main.go.
func runtimeNamespace() string {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	return defaultNamespace
}

func main() {
	runtimeNS := runtimeNamespace()
	var (
		dbPath           = flag.String("db", defaultDBPath(), "SQLite database path for the embedded apiserver.")
		bindAddr         = flag.String("bind-addr", "127.0.0.1", "Apiserver bind address. Defaults to loopback so the unauthenticated apiserver is not exposed by accident; pass --insecure-allow-public to bind a non-loopback address while auth remains disabled.")
		bindPort         = flag.Int("bind-port", 8443, "Apiserver bind port.")
		insecureAllowPub = flag.Bool("insecure-allow-public", false, "Acknowledge binding the unauthenticated apiserver to a non-loopback address. Required when --bind-addr is non-loopback (e.g. 0.0.0.0 inside a container with -p 8443:8443).")
		certDir          = flag.String("cert-dir", "", "TLS cert directory; empty means self-signed in-memory.")
		leaderElection   = flag.Bool("leader-election", false, "Enable leader election.")
		leaderID         = flag.String("leader-election-id", "clrk-controller-manager", "Leader election lease name.")
		leaderNS         = flag.String("leader-election-namespace", runtimeNS, "Leader election lease namespace.")
		metricsAddr      = flag.String("metrics-addr", "0", "Controller-manager metrics bind address (0 disables).")
		// Default :8082 to avoid colliding with the supervised
		// envoy-gateway child whose controller-runtime manager hardcodes
		// :8081 for its own healthz/probe endpoint.
		healthAddr           = flag.String("health-addr", ":8082", "Controller-manager healthz bind address.")
		ingressController    = flag.Bool("ingress-controller", false, "Reconcile TaskAgent → Gateway/HTTPRoute. Requires gateway-api CRDs in the target cluster.")
		egController         = flag.Bool("egressgateway-controller", false, "Reconcile EgressGateway → Envoy Gateway infra (GatewayClass, EnvoyProxy, Gateway) and mint the per-EG MITM CA.")
		envoyImage           = flag.String("envoy-image", defaultEnvoyImage, "Container image used for Envoy Gateway-managed Envoy pods. Must contain the clrk grpc_certificate_provider handshaker extension.")
		grpcAddr             = flag.String("grpc-addr", fmt.Sprintf(":%d", defaultGRPCPort), "gRPC bind address for the cert-provider / ext_proc / Envoy Gateway extension services.")
		grpcAdvertiseURI     = flag.String("grpc-advertise-uri", fmt.Sprintf("controller-manager.%s.svc:%d", runtimeNS, defaultGRPCPort), "gRPC target URI the EG extension programs into Envoy's cert-provider + ext_proc filter configs.")
		devEgressBackendHost = flag.String("dev-egress-backend-host", "", "When set, EgressGateway.Status.Listeners[*].BackendAddress entries are published as <host>:<NodePort> instead of the in-cluster Service DNS name. Used by clrk dev where workers run on the docker network and can't route to k3s ClusterIPs; in-cluster deployments leave this empty.")
		envoyGatewayBinary   = flag.String("envoy-gateway-binary", "/usr/local/bin/envoy-gateway", "Path to the upstream envoy-gateway binary that this process supervises as a child for the EG control plane.")
		crdInstallMode       = flag.String("crd-install-mode", "always", "How to apply embedded Gateway API + Envoy Gateway CRDs at startup. One of: always | if-missing | skip.")
		ingressExtProcAddr   = flag.String("ingress-extproc-addr", ":9444", "Bind address for the ingress (TaskAgent) ext_proc gRPC server. Per-TA EnvoyExtensionPolicy points at this server via a per-namespace Backend.")
		ingressExtProcHost   = flag.String("ingress-extproc-host", "", "FQDN/IP the in-cluster Backend uses to reach this controller-manager's ingress ext_proc port. Required when --ingress-controller is on.")
		ingressExtProcPort   = flag.Int("ingress-extproc-port", 9444, "TCP port the in-cluster Backend uses to reach this controller-manager's ingress ext_proc port.")
		adminAddr            = flag.String("admin-addr", ":8085", "Bind address for the /admin/* system-state HTTP endpoint. Read-only, unauthenticated; gate via network reachability (bind to loopback or to the in-cluster Service only).")
		// Embedded clickhouse: the cm process supervises a
		// clickhouse-server child if the binary is layered into the
		// image. Listener is 127.0.0.1-only — the engine is private
		// to the cm process; workers reach it indirectly via cm's
		// existing gRPC surface. Empty binary path or a missing file
		// = no embedded engine, cm still boots (Invocation storage
		// is unavailable until the binary lands).
		chBinary  = flag.String("embedded-clickhouse-binary", clickhouse.DefaultBinaryPath, "Path to the clickhouse binary supervised as an embedded engine. Empty disables the engine.")
		chDataDir = flag.String("embedded-clickhouse-data-dir", clickhouse.DefaultDataDir, "On-disk data directory for the embedded clickhouse engine. Backed by a PVC in the cm pod.")
		// OTLP receiver. The cm process runs an OTLP/HTTP receiver
		// that producers (worker, ingress/egress ext_proc) dial. Every
		// inbound signal is persisted to the embedded ClickHouse via
		// internal/chwriter and best-effort re-exported to a per-EG
		// customer endpoint via internal/otelforward.
		otlpAddr         = flag.String("otlp-addr", ":4318", "Bind address for the OTLP/HTTP receiver that captures producer signals into ClickHouse and re-exports to customer endpoints. Empty disables the receiver.")
		otlpTTLDur       = flag.Duration("otlp-ch-ttl", 7*24*time.Hour, "Retention for OTLP rows in the embedded ClickHouse. The rendered TTL is the ceiling of this in days.")
		otlpAdvertiseURI = flag.String("otlp-advertise-uri", fmt.Sprintf("http://controller-manager.%s.svc.cluster.local:4318", runtimeNS), "OTLP/HTTP URL stamped onto worker pods as CLRK_CM_OTLP_ENDPOINT. Workers dial this for every signal; cm persists + per-EG forwards.")
		// devOTLPFallback wires `clrk dev`'s in-process TUI receiver
		// as an unconditional fan-out destination on the cm OTLP
		// receiver. Every inbound payload is mirrored there regardless
		// of clrk.egress_gateway attribution, so the TUI lights up
		// even without an EgressGateway in the cluster. Empty in prod.
		devOTLPFallback = flag.String("dev-otlp-fallback-endpoint", "", "OTLP/HTTP URL the cm receiver mirrors every inbound payload to. clrk dev sets this to its in-process TUI receiver; prod leaves it empty.")
		// Embedded NATS/JetStream: the cm process runs an in-process
		// nats-server with JetStream as the transport for Invocation
		// lifecycle events. Worker pods publish to it over the cm
		// Service (so the client port binds a wildcard host, not
		// loopback); cm-local clients (the JetStream->ClickHouse
		// consumer, ingress ext_proc publisher, and Invocation Watch)
		// connect over an in-process pipe. Empty addr disables it.
		natsAddr     = flag.String("embedded-nats-addr", fmt.Sprintf("0.0.0.0:%d", clrknats.DefaultPort), "Bind address for the embedded NATS/JetStream client port. Worker pods dial this via the cm Service; cm-local clients use in-process connections. Empty disables the server.")
		natsStoreDir = flag.String("nats-store-dir", clrknats.DefaultStoreDir, "On-disk JetStream store for the embedded NATS server. PVC-backed in the cm pod so the durable INVOCATIONS stream survives restarts.")
		natsMaxAge   = flag.Duration("nats-stream-max-age", 72*time.Hour, "Retention window for the INVOCATIONS JetStream stream. Bounds how far back a Watch can resume; ClickHouse (separate TTL) is the long-term store.")
		natsMaxBytes = flag.Int64("nats-stream-max-bytes", 0, "Hard cap on the INVOCATIONS JetStream store size in bytes (0 = unbounded, retained only by --nats-stream-max-age). Bounds the store-dir PVC under high invocation volume; oldest events are evicted first.")
		// natsAdvertiseAddr is the cm NATS client address (host:port)
		// stamped onto worker pods as CLRK_CM_NATS_ADDR. Workers dial it
		// over the cm Service to publish Running + terminal Invocation
		// lifecycle events. Defaults to the in-cluster cm Service DNS;
		// `clrk dev` overrides it with the dev Service name.
		natsAdvertiseAddr = flag.String("nats-advertise-addr", fmt.Sprintf("controller-manager.%s.svc.cluster.local:%d", runtimeNS, clrknats.DefaultPort), "NATS client address stamped onto worker pods as CLRK_CM_NATS_ADDR for Invocation lifecycle publishing. Empty disables worker-side publishing.")
		// Test-write door for the top-level Invocation resource. Off by
		// default so the system-written billing record can't be forged;
		// dev/CI sets it true to seed invocations via the API (still
		// requires the per-request X-Clrk-Invocation-Write header).
		enableInvTestWrites = flag.Bool("enable-invocation-test-writes", false, "Open the header-gated Create/Update door on the top-level Invocation resource. Dev/CI only.")
	)
	// Read KUBECONFIG from env rather than a flag — sigs.k8s.io/controller-runtime
	// already registers a --kubeconfig flag via init() and we'd collide with it.
	// When set, the controller-runtime manager talks to that apiserver and
	// reconcilers see clrk.apoxy.dev via an aggregated APIService; empty means
	// loopback against the embedded apiserver (single-binary mode).
	kubeconfig := os.Getenv("KUBECONFIG")
	flag.Parse()

	// Single logging pipeline: stdlib slog (text) is the default for our
	// own slog.Info/Error sites; controller-runtime's logr is bridged into
	// the same handler so reconciler logs share format with everything
	// else this binary emits.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctrl.SetLogger(logr.FromSlogHandler(slog.Default().Handler()))
	log := ctrl.Log.WithName("controller-manager")

	mgr := apiserver.New()
	ctx := ctrl.SetupSignalHandler()

	// Embedded NATS/JetStream — the transport for the Invocation write
	// path. Created before the apiserver Manager starts so it can be
	// handed to the Invocation storage (Watch + test-write publisher)
	// via WithNATS; the apiserver's background goroutine connects to it
	// in-process once it's ready. Workers publish to it over the cm
	// Service; empty --embedded-nats-addr disables it (cm still boots,
	// Invocation lifecycle recording simply unavailable).
	natsErrCh := make(chan error, 1)
	var natsSrv *clrknats.Server
	if *natsAddr != "" {
		host, port, err := splitListenAddr(*natsAddr, clrknats.DefaultPort, "0.0.0.0")
		if err != nil {
			log.Error(err, "Invalid --embedded-nats-addr", "addr", *natsAddr)
			os.Exit(1)
		}
		natsSrv, err = clrknats.New(
			clrknats.WithListen(host, port),
			clrknats.WithStoreDir(*natsStoreDir),
			clrknats.WithStreamMaxAge(*natsMaxAge),
			clrknats.WithStreamMaxBytes(*natsMaxBytes),
		)
		if err != nil {
			log.Error(err, "Unable to construct embedded NATS server")
			os.Exit(1)
		}
		go func() {
			if err := natsSrv.Run(ctx); err != nil {
				natsErrCh <- err
			}
		}()
	}

	opts := []apiserver.Option{
		apiserver.WithSQLitePath(*dbPath),
		apiserver.WithBindAddress(*bindAddr),
		apiserver.WithBindPort(*bindPort),
		apiserver.WithAllowInvocationTestWrites(*enableInvTestWrites),
	}
	if natsSrv != nil {
		opts = append(opts, apiserver.WithNATS(natsSrv))
	}
	if *insecureAllowPub {
		opts = append(opts, apiserver.WithInsecureAllowPublic())
	}
	opts = append(opts,
		apiserver.WithMetricsBindAddress(*metricsAddr),
		apiserver.WithHealthBindAddress(*healthAddr),
		apiserver.WithResources(
			&clrkv1alpha1.TaskAgent{},
			&clrkv1alpha1.DaemonAgent{},
			&clrkv1alpha1.WorkerPool{},
			&clrkv1alpha1.AgentSandboxRevision{},
			&clrkv1alpha1.EgressGateway{},
			&clrkv1alpha1.EgressL4Route{},
			&clrkv1alpha1.MCPRoute{},
			&clrkv1alpha1.AIProviderRoute{},
			&clrkv1alpha1.Backend{},
			&clrkv1alpha1.CredentialInjectionPolicy{},
			&clrkv1alpha1.RateLimitPolicy{},
			&clrkv1alpha1.LoggingPolicy{},
			&clrkv1alpha1.EgressDenyPolicy{},
		),
	)
	if *certDir != "" {
		opts = append(opts, apiserver.WithCertDir(*certDir))
	}
	if *leaderElection {
		opts = append(opts, apiserver.WithLeaderElection(*leaderID, *leaderNS))
	}
	var clusterCfg *rest.Config
	if kubeconfig != "" {
		cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			log.Error(err, "Unable to load kubeconfig", "path", kubeconfig)
			os.Exit(1)
		}
		clusterCfg = cfg
		opts = append(opts, apiserver.WithClientConfig(cfg))
	} else if cfg, err := rest.InClusterConfig(); err == nil {
		// Running as a Pod with a mounted ServiceAccount token — use the
		// in-cluster apiserver address as the controller-runtime client's
		// backend so cm reconcilers see apps/v1, core, etc. directly from
		// the cluster. clrk.apoxy.dev/v1alpha1 still flows through the
		// embedded apiserver via the aggregated APIService.
		clusterCfg = cfg
		opts = append(opts, apiserver.WithClientConfig(cfg))
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- mgr.Start(ctx, opts...)
	}()

	if err, open := <-mgr.ReadyCh; open && err != nil {
		log.Error(err, "Apiserver failed to become ready")
		os.Exit(1)
	}

	// Sideload Gateway API + Envoy Gateway CRDs into the cluster the EG
	// control plane will reconcile against. Skipped in single-binary mode
	// (no kubeconfig) where there's no separate cluster to install into.
	if clusterCfg != nil && (*ingressController || *egController) {
		mode, err := parseCRDMode(*crdInstallMode)
		if err != nil {
			log.Error(err, "Invalid --crd-install-mode")
			os.Exit(1)
		}
		if err := crds.Install(ctx, clusterCfg, crds.InstallOptions{Mode: mode}); err != nil {
			log.Error(err, "CRD install failed")
			os.Exit(1)
		}
	}

	cm := mgr.CtrlManager()

	// Kube probes hit :8082 directly on the controller-manager pod;
	// EG is deliberately not in this path. healthz.Ping is sufficient
	// — controller-runtime only serves /readyz once Start reaches the
	// Runnables phase, so it doubles as a cache-synced signal.
	if err := cm.AddHealthzCheck("ping", healthz.Ping); err != nil {
		log.Error(err, "Unable to register healthz check")
		os.Exit(1)
	}
	if err := cm.AddReadyzCheck("ping", healthz.Ping); err != nil {
		log.Error(err, "Unable to register readyz check")
		os.Exit(1)
	}

	// Portable reconcilers: run in all modes.
	if err := (&controller.WorkerPoolStatusReconciler{
		Client: cm.GetClient(),
		Scheme: cm.GetScheme(),
	}).SetupWithManager(cm); err != nil {
		log.Error(err, "Unable to register controller", "controller", "WorkerPoolStatus")
		os.Exit(1)
	}
	// ActiveExecutions counter sourced from the Invocation read model
	// (APO-620). Shares the apiserver's LazyPool — resolves as the
	// embedded ClickHouse comes up. Left nil (reconciler falls back to the
	// per-worker WorkerStatus sum) when the pool isn't wired.
	var invActiveCounter controller.InvocationActiveCounter
	if pool := mgr.InvocationPool(); pool != nil {
		invActiveCounter = invocation.NewActiveCounter(pool, 2*time.Second)
	}
	if err := (&controller.TaskAgentRevisionReconciler{
		Client:     cm.GetClient(),
		Scheme:     cm.GetScheme(),
		ActiveExec: invActiveCounter,
	}).SetupWithManager(cm); err != nil {
		log.Error(err, "Unable to register controller", "controller", "TaskAgentRevision")
		os.Exit(1)
	}
	if err := (&controller.DaemonAgentRevisionReconciler{
		Client: cm.GetClient(),
		Scheme: cm.GetScheme(),
	}).SetupWithManager(cm); err != nil {
		log.Error(err, "Unable to register controller", "controller", "DaemonAgentRevision")
		os.Exit(1)
	}

	// WorkerHealthChecker joins each worker pod's KV-published status
	// (warm/in-flight/cached) with its routable IP + readiness from the
	// pool's EndpointSlices into an in-memory map. The ingress ext_proc
	// consults Pick() per inbound request to route to a worker that
	// already has the revision warm/cached, and to enforce cluster-wide
	// MaxConcurrent. Run in every mode (not gated on --ingress-controller)
	// so even the cron-only/dispatch-only paths can read consistent
	// in-flight state. When NATS is disabled (natsSrv nil) status routing
	// is unavailable and Pick finds no candidates (ingress 503s); pass a
	// true nil interface, not a typed nil *nats.Server.
	var statusNATS healthcheck.NATSProvider
	if natsSrv != nil {
		statusNATS = natsSrv
	}
	healthChecker := healthcheck.NewWorkerHealthChecker(cm.GetClient(), cm.GetCache(), statusNATS)
	if err := cm.Add(healthChecker); err != nil {
		log.Error(err, "Unable to add worker health checker")
		os.Exit(1)
	}

	// /admin system-state surface. Dedicated listener so a wedged
	// handler can't cause kube-probes to fail (controller-runtime
	// owns the healthz listener). Bound to whatever --admin-addr
	// says — defaults to :8085 in the controller-manager Service.
	adminMux := admin.New()
	adminLis, err := net.Listen("tcp", *adminAddr)
	if err != nil {
		log.Error(err, "Unable to bind admin listener", "addr", *adminAddr)
		os.Exit(1)
	}
	adminSrv := &http.Server{Handler: adminMux}
	go func() {
		slog.Info("Serving /admin", "addr", adminLis.Addr().String())
		if err := adminSrv.Serve(adminLis); err != nil && err != http.ErrServerClosed {
			log.Error(err, "admin HTTP server exited")
		}
	}()
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = adminSrv.Shutdown(shutCtx)
	}()

	if *ingressController {
		if *ingressExtProcHost == "" {
			log.Error(fmt.Errorf("--ingress-extproc-host is required when --ingress-controller is on"), "Missing required flag")
			os.Exit(1)
		}
		if err := (&controller.TaskAgentIngressReconciler{
			Client:             cm.GetClient(),
			Scheme:             cm.GetScheme(),
			IngressExtProcHost: *ingressExtProcHost,
			IngressExtProcPort: int32(*ingressExtProcPort),
		}).SetupWithManager(cm); err != nil {
			log.Error(err, "Unable to register controller", "controller", "TaskAgentIngress")
			os.Exit(1)
		}
		// Cron firer is gated on --ingress-controller because the HTTP
		// invoker targets the Gateway URL the ingress reconciler creates.
		// Without ingress, there's nothing to invoke.
		if err := (&controller.TaskAgentCronReconciler{
			Client:  cm.GetClient(),
			Scheme:  cm.GetScheme(),
			Invoker: controller.NewHTTPInvoker(cm.GetClient()),
		}).SetupWithManager(cm); err != nil {
			log.Error(err, "Unable to register controller", "controller", "TaskAgentCron")
			os.Exit(1)
		}
	}
	// WorkerPool → Deployment/Service. Runs in every mode; the legacy
	// dev-only skip (workers as docker containers) is retired now that
	// `clrk dev` runs workers as Pods.
	//
	// Only advertise the cm NATS address to workers when the embedded
	// server is actually running. --nats-advertise-addr defaults to a
	// non-empty Service DNS, so without this guard a cm started with an
	// empty --embedded-nats-addr (NATS disabled) would still stamp
	// CLRK_CM_NATS_ADDR onto every worker, which would then dial a port
	// that never binds and silently drop lifecycle events into a full
	// outbox.
	cmNATSAddr := ""
	if natsSrv != nil {
		cmNATSAddr = *natsAdvertiseAddr
	}
	if err := (&controller.WorkerPoolDeploymentReconciler{
		Client:         cm.GetClient(),
		Scheme:         cm.GetScheme(),
		CMOTLPEndpoint: *otlpAdvertiseURI,
		CMNATSAddr:     cmNATSAddr,
	}).SetupWithManager(cm); err != nil {
		log.Error(err, "Unable to register controller", "controller", "WorkerPoolDeployment")
		os.Exit(1)
	}
	if *egController {
		if err := (&controller.EgressGatewayReconciler{
			Client:                cm.GetClient(),
			Scheme:                cm.GetScheme(),
			EnvoyImage:            *envoyImage,
			DevBackendHost:        *devEgressBackendHost,
			EnvoyGatewayNamespace: runtimeNS,
		}).SetupWithManager(cm); err != nil {
			log.Error(err, "Unable to register controller", "controller", "EgressGateway")
			os.Exit(1)
		}
		// Route status is only meaningful when the egress data plane is
		// active: it validates AIProviderRoute Backend/EgressGateway refs
		// and writes Accepted/ResolvedRefs.
		if err := (&controller.AIProviderRouteStatusReconciler{
			Client: cm.GetClient(),
			Scheme: cm.GetScheme(),
		}).SetupWithManager(cm); err != nil {
			log.Error(err, "Unable to register controller", "controller", "AIProviderRouteStatus")
			os.Exit(1)
		}
	}

	grpcLis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Error(err, "Unable to bind gRPC listener", "addr", *grpcAddr)
		os.Exit(1)
	}
	// invocations carries the inbound W3C trace parent context from
	// the ingress ext_proc to the egress ext_proc, keyed by the per-
	// invocation id we stamp on every TaskAgent request. Both servers
	// share the same store; the reaper runs for the controller-
	// manager's lifetime.
	invocations := invocationctx.NewStore()
	go invocations.Run(ctx)
	defer invocations.Close()

	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(egidentity.UnaryServerInterceptor()),
		grpc.StreamInterceptor(egidentity.StreamServerInterceptor()),
	)
	envoytlsv3.RegisterCertificateProviderServiceServer(grpcSrv, certprovider.New(cm.GetClient()))
	extprocSrv := extproc.New(cm.GetClient(), extproc.WithInvocationContext(invocations))
	extprocv3.RegisterExternalProcessorServer(grpcSrv, extprocSrv)
	// L4 ext_proc service shares the same gRPC endpoint, EG identity
	// resolution, and per-EG sink registry as the HTTP path. Records
	// emitted on this service represent TCP/TLS connections handled
	// by the egress listener's TCP-fallback chain.
	extproc.RegisterNetworkServer(grpcSrv, extprocSrv)
	egExt, err := egextension.New(cm.GetClient(), *grpcAdvertiseURI, *grpcAdvertiseURI)
	if err != nil {
		log.Error(err, "Unable to construct EG extension server")
		os.Exit(1)
	}
	egextpb.RegisterEnvoyGatewayExtensionServer(grpcSrv, egExt)
	go func() {
		slog.Info("Serving control-plane gRPC", "addr", grpcLis.Addr().String())
		if err := grpcSrv.Serve(grpcLis); err != nil {
			log.Error(err, "gRPC server exited")
		}
	}()
	defer func() {
		grpcSrv.GracefulStop()
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		extprocSrv.Stop(shutCtx)
	}()

	// Ingress ext_proc — separate gRPC listener so the egress and
	// ingress ExternalProcessor service registrations don't collide.
	// Only useful when the ingress controller is on (no per-TA EG
	// policy points here otherwise), but cheap to run unconditionally.
	ingressLis, err := net.Listen("tcp", *ingressExtProcAddr)
	if err != nil {
		log.Error(err, "Unable to bind ingress ext_proc listener", "addr", *ingressExtProcAddr)
		os.Exit(1)
	}
	// Ingress OTLP emitter targets the cm-local OTLP receiver via
	// CLRK_CM_OTLP_ENDPOINT (or CLRK_DEV_OTEL_ENDPOINT in dev). The
	// receiver in turn persists every signal to the embedded
	// ClickHouse and re-exports per-EG to customer endpoints via
	// the EgressGatewayOTLPForwardReconciler below.
	ingressEmitter := ingressextproc.BuildEmitter(ctx, cm.GetAPIReader(), runtimeNS)
	ingressSrv := ingressextproc.New(cm.GetClient(), healthChecker, invocations, ingressEmitter)
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		current := ingressSrv.SwapEmitter(otelemit.Noop())
		if current != nil {
			if err := current.Close(shutCtx); err != nil {
				log.Error(err, "Shutting down ingress OTLP emitter")
			}
		}
	}()
	ingressGRPC := grpc.NewServer()
	extprocv3.RegisterExternalProcessorServer(ingressGRPC, ingressSrv)
	go func() {
		slog.Info("Serving ingress ext_proc gRPC", "addr", ingressLis.Addr().String())
		if err := ingressGRPC.Serve(ingressLis); err != nil {
			log.Error(err, "Ingress ext_proc gRPC server exited")
		}
	}()
	defer ingressGRPC.GracefulStop()
	// Publish Dispatched/Rejected Invocation lifecycle events from the
	// ingress admission path to the embedded JetStream stream (APO-617).
	// Connects in-process once NATS is ready; guarded so a typed-nil
	// provider never reaches the publisher. No-op when NATS is disabled.
	if natsSrv != nil {
		go ingressSrv.RunInvocationPublisher(ctx, natsSrv)
	}

	egErrCh := make(chan error, 1)
	if clusterCfg != nil && (*ingressController || *egController) {
		host, port := splitGRPCAddr(*grpcAddr, defaultGRPCPort)
		go func() {
			egErrCh <- egcontrolplane.Run(ctx, egcontrolplane.Config{
				BinaryPath:          *envoyGatewayBinary,
				Kubeconfig:          kubeconfig,
				RestConfig:          clusterCfg,
				ExtensionHost:       host,
				ExtensionPort:       port,
				ControllerNamespace: runtimeNS,
			})
		}()
	}

	// Embedded clickhouse-server child. Supervisor returns nil (no-op)
	// when the binary is missing — that's the path during the
	// apoxy-cloud-side image-layering rollout, where cm boots fine
	// without an engine until the binary lands.
	chErrCh := make(chan error, 1)
	var ch *clickhouse.Embedded
	if *chBinary != "" {
		if *chDataDir == "" {
			log.Error(errors.New("--embedded-clickhouse-data-dir is required when --embedded-clickhouse-binary is set"), "Invalid flag combination")
			os.Exit(1)
		}
		ch = clickhouse.New(
			clickhouse.WithBinaryPath(*chBinary),
			clickhouse.WithDataDir(*chDataDir),
		)
		go func() {
			if err := ch.Run(ctx); err != nil {
				chErrCh <- err
			}
		}()
	}

	// OTLP plane: cm runs an OTLP/HTTP receiver that producers
	// (worker, ingress/egress ext_proc) dial. Every signal is
	// persisted to the embedded ClickHouse via internal/chwriter and
	// best-effort re-exported to a per-EG customer endpoint via
	// internal/otelforward. Gated on the embedded engine because
	// chwriter has no destination without it; gated on the receiver
	// flag so dev/test scenarios can disable the listener entirely.
	chwErrCh := make(chan error, 1)
	if ch != nil && *otlpAddr != "" {
		ttlDays := int(*otlpTTLDur / (24 * time.Hour))
		if ttlDays < 1 {
			ttlDays = 1
		}
		writer := chwriter.New(chwriter.WithTTLDays(ttlDays))
		go func() {
			if err := ch.Ready(ctx); err != nil {
				if ctx.Err() == nil {
					chwErrCh <- fmt.Errorf("wait for clickhouse ready: %w", err)
				}
				return
			}
			if err := writer.Run(ctx); err != nil && ctx.Err() == nil {
				chwErrCh <- err
			}
		}()
		forwards := otelforward.NewRegistry(ctx)
		// devSink fans every inbound OTLP payload to the `clrk dev`
		// TUI receiver regardless of clrk.egress_gateway attribution
		// or EG-registry state. Empty in prod; set to the dev TUI
		// endpoint by --dev-otlp-fallback-endpoint in `clrk dev`.
		var devSink *otelforward.Forwarder
		if *devOTLPFallback != "" {
			devSink = otelforward.NewForwarder("_dev", clrkv1alpha1.OTLPLogsSinkSpec{Endpoint: *devOTLPFallback})
			if devSink != nil {
				go devSink.Run(ctx)
			}
		}
		otelSrv, err := otelreceiver.Start(ctx, *otlpAddr, writer, forwards, devSink)
		if err != nil {
			log.Error(err, "Unable to start OTLP receiver", "addr", *otlpAddr)
			os.Exit(1)
		}
		slog.Info("Serving OTLP receiver", "addr", otelSrv.Addr(), "dev_sink", *devOTLPFallback)
		if err := (&controller.EgressGatewayOTLPForwardReconciler{
			Client:   cm.GetClient(),
			Registry: forwards,
		}).SetupWithManager(cm); err != nil {
			log.Error(err, "Unable to register controller", "controller", "EgressGatewayOTLPForward")
			os.Exit(1)
		}
		defer func() {
			shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			forwards.Shutdown(shutCtx)
			if devSink != nil {
				_ = devSink.Close(shutCtx)
			}
		}()
	}

	slog.Info("Controller-manager running",
		"apiserver", *bindAddr,
		"port", *bindPort,
		"db", *dbPath,
		"ingress_controller", *ingressController,
		"kubeconfig", kubeconfig,
	)

	// Release the apiserver.Manager's ctrl.Manager start gate. The
	// apiserver goroutine will now call mgr.Start(ctx) and block there.
	close(mgr.StartCh)

	select {
	case err := <-errCh:
		if err != nil {
			log.Error(err, "Manager exited with error")
			os.Exit(1)
		}
	case err := <-egErrCh:
		if err != nil {
			log.Error(err, "envoy-gateway supervisor exited; tearing down")
			os.Exit(1)
		}
	case err := <-chErrCh:
		if err != nil {
			log.Error(err, "Embedded clickhouse supervisor exited; tearing down")
			os.Exit(1)
		}
	case err := <-chwErrCh:
		if err != nil {
			log.Error(err, "OTLP chwriter exited; tearing down")
			os.Exit(1)
		}
	case err := <-natsErrCh:
		if err != nil {
			log.Error(err, "Embedded NATS server exited; tearing down")
			os.Exit(1)
		}
	}
}

// parseCRDMode maps the --crd-install-mode flag string to crds.Mode.
func parseCRDMode(s string) (crds.Mode, error) {
	switch s {
	case "always":
		return crds.ModeAlways, nil
	case "if-missing":
		return crds.ModeIfMissing, nil
	case "skip":
		return crds.ModeSkip, nil
	default:
		return 0, fmt.Errorf("unknown crd-install-mode %q (want always|if-missing|skip)", s)
	}
}

// splitListenAddr parses a "host:port" or ":port" listen spec, applying
// defaultHost when the host is empty and defaultPort when the port is
// missing/invalid. Unlike splitGRPCAddr it preserves a wildcard host
// (0.0.0.0) so the listener is reachable off-box — the embedded NATS
// client port must accept connections from worker pods.
func splitListenAddr(addr string, defaultPort int, defaultHost string) (string, int, error) {
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("parse %q: %w", addr, err)
	}
	if h == "" {
		h = defaultHost
	}
	port := defaultPort
	if p != "" {
		v, err := strconv.Atoi(p)
		if err != nil {
			return "", 0, fmt.Errorf("parse port in %q: %w", addr, err)
		}
		port = v
	}
	return h, port, nil
}

// splitGRPCAddr splits a "host:port" or ":port" listener spec into a
// loopback-friendly host and int port for the EG child to dial.
func splitGRPCAddr(addr string, defaultPort int) (host string, port int) {
	host = "127.0.0.1"
	port = defaultPort
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		return host, port
	}
	if h != "" && h != "0.0.0.0" && h != "::" {
		host = h
	}
	if v, err := strconv.Atoi(p); err == nil {
		port = v
	}
	return host, port
}

func defaultDBPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "/var/lib/clrk/data.db"
	}
	return filepath.Join(home, ".clrk", "data.db")
}
