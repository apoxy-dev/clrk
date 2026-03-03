package main

import (
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

var scheme = runtime.NewScheme()

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = clrkv1alpha1.AddToScheme(scheme)
}

func main() {
	ctrl.SetLogger(zap.New())
	log := ctrl.Log.WithName("worker")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: ":8082"},
		HealthProbeBindAddress: ":8083",
		LeaderElection:         false,
	})
	if err != nil {
		log.Error(err, "Unable to create manager")
		os.Exit(1)
	}

	// TODO: Register the worker runtime as a manager.Runnable once
	// internal/worker is implemented.
	//
	// if err := mgr.Add(&worker.Runtime{
	// 	Client: mgr.GetClient(),
	// }); err != nil {
	// 	log.Error(err, "Unable to add worker runtime")
	// 	os.Exit(1)
	// }
	//
	// import "github.com/apoxy-dev/clrk/internal/worker"

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
