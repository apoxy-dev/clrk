package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	envoytlsv3 "github.com/apoxy-dev/envoy-go/envoy/service/tls/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	egextpb "github.com/envoyproxy/gateway/proto/extension"
	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/certprovider"
	"github.com/apoxy-dev/clrk/internal/controller"
	"github.com/apoxy-dev/clrk/internal/egextension"
	"github.com/apoxy-dev/clrk/internal/egidentity"
	"github.com/apoxy-dev/clrk/internal/extproc"
	"github.com/apoxy-dev/clrk/pkg/apiserver"
)

const (
	defaultEnvoyImage = "us-west1-docker.pkg.dev/apoxy-dev/public/envoy:665aebbf"
	defaultGRPCPort   = 9443
)

func main() {
	var (
		dbPath            = flag.String("db", defaultDBPath(), "SQLite database path for the embedded apiserver.")
		bindAddr          = flag.String("bind-addr", "0.0.0.0", "Apiserver bind address.")
		bindPort          = flag.Int("bind-port", 8443, "Apiserver bind port.")
		certDir           = flag.String("cert-dir", "", "TLS cert directory; empty means self-signed in-memory.")
		leaderElection    = flag.Bool("leader-election", false, "Enable leader election.")
		leaderID          = flag.String("leader-election-id", "clrk-controller-manager", "Leader election lease name.")
		leaderNS          = flag.String("leader-election-namespace", "default", "Leader election lease namespace.")
		metricsAddr       = flag.String("metrics-addr", "0", "Controller-manager metrics bind address (0 disables).")
		healthAddr        = flag.String("health-addr", ":8081", "Controller-manager healthz bind address.")
		ingressController = flag.Bool("ingress-controller", false, "Reconcile TaskAgent → Gateway/HTTPRoute. Requires gateway-api CRDs in the target cluster.")
		workerDeployment  = flag.Bool("worker-deployment-controller", false, "Reconcile WorkerPool → Deployment/Service. Off in clrk dev where workers run as docker containers on the host (a k8s-managed Deployment would create duplicate workers).")
		egController      = flag.Bool("egressgateway-controller", false, "Reconcile EgressGateway → Envoy Gateway infra (GatewayClass, EnvoyProxy, Gateway) and mint the per-EG MITM CA.")
		envoyImage        = flag.String("envoy-image", defaultEnvoyImage, "Container image used for Envoy Gateway-managed Envoy pods. Must contain the clrk grpc_certificate_provider handshaker extension.")
		grpcAddr          = flag.String("grpc-addr", fmt.Sprintf(":%d", defaultGRPCPort), "gRPC bind address for the cert-provider / ext_proc / Envoy Gateway extension services.")
		grpcAdvertiseURI  = flag.String("grpc-advertise-uri", fmt.Sprintf("controller-manager.default.svc:%d", defaultGRPCPort), "gRPC target URI the EG extension programs into Envoy's cert-provider + ext_proc filter configs.")
		devEgressBackendHost = flag.String("dev-egress-backend-host", "", "When set, EgressGateway.Status.EgressBackendAddress is published as <host>:<NodePort> instead of the in-cluster Service DNS name. Used by clrk dev where workers run on the docker network and can't route to k3s ClusterIPs; in-cluster deployments leave this empty.")
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
	}
	if *certDir != "" {
		opts = append(opts, apiserver.WithCertDir(*certDir))
	}
	if *leaderElection {
		opts = append(opts, apiserver.WithLeaderElection(*leaderID, *leaderNS))
	}
	if kubeconfig != "" {
		cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			log.Error(err, "Unable to load kubeconfig", "path", kubeconfig)
			os.Exit(1)
		}
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

	if *ingressController {
		if err := (&controller.TaskAgentIngressReconciler{
			Client: cm.GetClient(),
			Scheme: cm.GetScheme(),
		}).SetupWithManager(cm); err != nil {
			log.Error(err, "Unable to register controller", "controller", "TaskAgentIngress")
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
			Client:         cm.GetClient(),
			Scheme:         cm.GetScheme(),
			EnvoyImage:     *envoyImage,
			DevBackendHost: *devEgressBackendHost,
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
	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(egidentity.UnaryServerInterceptor()),
		grpc.StreamInterceptor(egidentity.StreamServerInterceptor()),
	)
	envoytlsv3.RegisterCertificateProviderServiceServer(grpcSrv, certprovider.New(cm.GetClient()))
	extprocSrv := extproc.New(cm.GetClient())
	extprocv3.RegisterExternalProcessorServer(grpcSrv, extprocSrv)
	egExt, err := egextension.New(*grpcAdvertiseURI, *grpcAdvertiseURI)
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

	if err := <-errCh; err != nil {
		log.Error(err, "Manager exited with error")
		os.Exit(1)
	}
}

func defaultDBPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "/var/lib/clrk/data.db"
	}
	return filepath.Join(home, ".clrk", "data.db")
}
