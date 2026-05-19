package main

import (
	"log/slog"
	"os"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/config"
	"github.com/apoxy-dev/clrk/internal/worker"
)

var scheme = runtime.NewScheme()

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = clrkv1alpha1.AddToScheme(scheme)
}

func main() {
	// runsc subcommand demux runs before any other setup. Container.Start
	// re-execs /proc/self/exe with argv[1] like "boot" or "gofer"; those
	// invocations must reach gVisor's runsc main, not the controller-
	// runtime manager. tryDispatchRunsc returns only if argv doesn't
	// match a runsc subcommand.
	tryDispatchRunsc()

	// Single logging pipeline: stdlib slog (text) is the default for our
	// own slog.Info/Error sites; controller-runtime's logr is bridged into
	// the same handler so reconciler logs share format with everything
	// else this binary emits.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctrl.SetLogger(logr.FromSlogHandler(slog.Default().Handler()))
	log := ctrl.Log.WithName("worker")

	restCfg, err := config.FromEnv()
	if err != nil {
		log.Error(err, "Unable to build rest.Config")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: ":8082"},
		HealthProbeBindAddress: ":8083",
		LeaderElection:         false,
	})
	if err != nil {
		log.Error(err, "Unable to create manager")
		os.Exit(1)
	}

	poolName := os.Getenv("CLRK_POOL_NAME")
	if poolName == "" {
		poolName = "default"
	}
	podName := os.Getenv("POD_NAME")
	namespace := os.Getenv("POD_NAMESPACE")

	if err := mgr.Add(&worker.Runtime{
		Client:    mgr.GetClient(),
		Manager:   mgr,
		PoolName:  poolName,
		PodName:   podName,
		Namespace: namespace,
	}); err != nil {
		log.Error(err, "Unable to add worker runtime")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(err, "Unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error(err, "Unable to set up ready check")
		os.Exit(1)
	}

	log.Info("Starting worker")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "Problem running worker")
		os.Exit(1)
	}
}
