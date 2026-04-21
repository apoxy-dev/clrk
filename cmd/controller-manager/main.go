package main

import (
	"flag"
	"log/slog"
	"os"
	"path/filepath"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/controller"
	"github.com/apoxy-dev/clrk/pkg/apiserver"
)

func main() {
	var (
		dbPath         = flag.String("db", defaultDBPath(), "SQLite database path for the embedded apiserver.")
		bindAddr       = flag.String("bind-addr", "0.0.0.0", "Apiserver bind address.")
		bindPort       = flag.Int("bind-port", 8443, "Apiserver bind port.")
		certDir        = flag.String("cert-dir", "", "TLS cert directory; empty means self-signed in-memory.")
		leaderElection = flag.Bool("leader-election", false, "Enable leader election.")
		leaderID       = flag.String("leader-election-id", "clrk-controller-manager", "Leader election lease name.")
		leaderNS       = flag.String("leader-election-namespace", "default", "Leader election lease namespace.")
		metricsAddr    = flag.String("metrics-addr", "0", "Controller-manager metrics bind address (0 disables).")
		healthAddr     = flag.String("health-addr", ":8081", "Controller-manager healthz bind address.")
	)
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

	errCh := make(chan error, 1)
	go func() {
		errCh <- mgr.Start(ctx, opts...)
	}()

	if err, open := <-mgr.ReadyCh; open && err != nil {
		log.Error(err, "Apiserver failed to become ready")
		os.Exit(1)
	}

	cm := mgr.CtrlManager()
	if err := (&controller.WorkerPoolReconciler{
		Client: cm.GetClient(),
		Scheme: cm.GetScheme(),
	}).SetupWithManager(cm); err != nil {
		log.Error(err, "Unable to register controller", "controller", "WorkerPool")
		os.Exit(1)
	}
	if err := (&controller.TaskAgentReconciler{
		Client: cm.GetClient(),
		Scheme: cm.GetScheme(),
	}).SetupWithManager(cm); err != nil {
		log.Error(err, "Unable to register controller", "controller", "TaskAgent")
		os.Exit(1)
	}

	slog.Info("Controller-manager running", "apiserver", *bindAddr, "port", *bindPort, "db", *dbPath)

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
