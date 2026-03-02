package main

import (
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

var scheme = runtime.NewScheme()

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = clrkv1alpha1.AddToScheme(scheme)
}

func main() {
	ctrl.SetLogger(zap.New())
	log := ctrl.Log.WithName("controller-manager")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		MetricsBindAddress:     ":8080",
		HealthProbeBindAddress: ":8081",
		LeaderElection:         true,
		LeaderElectionID:       "clrk-controller-manager",
	})
	if err != nil {
		log.Error(err, "unable to create manager")
		os.Exit(1)
	}

	// TODO: Wire up reconcilers once internal/controller is implemented.
	//
	// if err := (&controller.TaskAgentReconciler{
	// 	Client: mgr.GetClient(),
	// 	Scheme: mgr.GetScheme(),
	// }).SetupWithManager(mgr); err != nil {
	// 	log.Error(err, "unable to create controller", "controller", "TaskAgent")
	// 	os.Exit(1)
	// }
	//
	// if err := (&controller.DaemonAgentReconciler{
	// 	Client: mgr.GetClient(),
	// 	Scheme: mgr.GetScheme(),
	// }).SetupWithManager(mgr); err != nil {
	// 	log.Error(err, "unable to create controller", "controller", "DaemonAgent")
	// 	os.Exit(1)
	// }
	//
	// if err := (&controller.WorkerPoolReconciler{
	// 	Client: mgr.GetClient(),
	// 	Scheme: mgr.GetScheme(),
	// }).SetupWithManager(mgr); err != nil {
	// 	log.Error(err, "unable to create controller", "controller", "WorkerPool")
	// 	os.Exit(1)
	// }
	//
	// import "github.com/apoxy-dev/clrk/internal/controller"

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	log.Info("starting controller-manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "problem running manager")
		os.Exit(1)
	}
}
