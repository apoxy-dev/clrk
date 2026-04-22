package main

import (
	"flag"
	"log/slog"
	"os"
	"path/filepath"

	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/controller"
	"github.com/apoxy-dev/clrk/pkg/apiserver"
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
	)
	// Read KUBECONFIG from env rather than a flag — sigs.k8s.io/controller-runtime
	// already registers a --kubeconfig flag via init() and we'd collide with it.
	// When set, the controller-runtime manager talks to that apiserver and
	// reconcilers see clrk.apoxy.dev via an aggregated APIService; empty means
	// loopback against the embedded apiserver (single-binary mode).
	kubeconfig := os.Getenv("KUBECONFIG")
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
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
