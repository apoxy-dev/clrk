package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
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

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/apiserver"
	"github.com/apoxy-dev/clrk/internal/certprovider"
	"github.com/apoxy-dev/clrk/internal/controller"
	"github.com/apoxy-dev/clrk/internal/crds"
	"github.com/apoxy-dev/clrk/internal/egcontrolplane"
	"github.com/apoxy-dev/clrk/internal/egextension"
	"github.com/apoxy-dev/clrk/internal/egidentity"
	"github.com/apoxy-dev/clrk/internal/extproc"
	ingressextproc "github.com/apoxy-dev/clrk/internal/extproc/ingress"
	"github.com/apoxy-dev/clrk/internal/extproc/invocationctx"
	"github.com/apoxy-dev/clrk/internal/healthcheck"
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
		workerDeployment     = flag.Bool("worker-deployment-controller", false, "Reconcile WorkerPool → Deployment/Service. Off in clrk dev where workers run as docker containers on the host (a k8s-managed Deployment would create duplicate workers).")
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

	opts := []apiserver.Option{
		apiserver.WithSQLitePath(*dbPath),
		apiserver.WithBindAddress(*bindAddr),
		apiserver.WithBindPort(*bindPort),
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

	// Portable reconcilers: run in all modes.
	if err := (&controller.WorkerPoolStatusReconciler{
		Client: cm.GetClient(),
		Scheme: cm.GetScheme(),
	}).SetupWithManager(cm); err != nil {
		log.Error(err, "Unable to register controller", "controller", "WorkerPoolStatus")
		os.Exit(1)
	}
	if err := (&controller.TaskAgentRevisionReconciler{
		Client: cm.GetClient(),
		Scheme: cm.GetScheme(),
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

	// WorkerHealthChecker streams every worker pod's status RPC into
	// an in-memory map. The ingress ext_proc consults Pick() per
	// inbound request to route to a worker that already has the
	// revision warm/cached, and to enforce cluster-wide MaxConcurrent.
	// Run in every mode (not gated on --ingress-controller) so even
	// the cron-only/dispatch-only paths can read consistent in-flight
	// state.
	healthChecker := healthcheck.NewWorkerHealthChecker(cm.GetClient())
	if err := cm.Add(healthChecker); err != nil {
		log.Error(err, "Unable to add worker health checker")
		os.Exit(1)
	}

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
	if *workerDeployment {
		if err := (&controller.WorkerPoolDeploymentReconciler{
			Client: cm.GetClient(),
			Scheme: cm.GetScheme(),
		}).SetupWithManager(cm); err != nil {
			log.Error(err, "Unable to register controller", "controller", "WorkerPoolDeployment")
			os.Exit(1)
		}
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
	ingressGRPC := grpc.NewServer()
	extprocv3.RegisterExternalProcessorServer(ingressGRPC, ingressextproc.New(cm.GetClient(), healthChecker, invocations))
	go func() {
		slog.Info("Serving ingress ext_proc gRPC", "addr", ingressLis.Addr().String())
		if err := ingressGRPC.Serve(ingressLis); err != nil {
			log.Error(err, "Ingress ext_proc gRPC server exited")
		}
	}()
	defer ingressGRPC.GracefulStop()

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

	slog.Info("Controller-manager running",
		"apiserver", *bindAddr,
		"port", *bindPort,
		"db", *dbPath,
		"ingress_controller", *ingressController,
		"worker_deployment_controller", *workerDeployment,
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
