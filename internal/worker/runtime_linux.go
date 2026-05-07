package worker

import (
	"context"
	"fmt"
	"os"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	clrkcontroller "github.com/apoxy-dev/clrk/internal/controller"
	"github.com/apoxy-dev/clrk/internal/egress"
)

const (
	clrkStateDir  = "/run/clrk/state"
	clrkRootDir   = "/run/clrk/rootfs"
	clrkImagesDir = "/run/clrk/images"
	clrkLogsDir   = WorkerLogsDir

	heartbeatInterval = 30 * time.Second
)

// Start initializes the runtime and blocks until the context is cancelled.
func (r *Runtime) Start(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx).WithName("worker-runtime")
	log.Info("Starting worker runtime",
		"pool", r.PoolName,
		"pod", r.PodName,
		"namespace", r.Namespace,
	)

	// Create runtime directories.
	for _, dir := range []string{clrkStateDir, clrkRootDir, clrkImagesDir, clrkLogsDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("creating dir %s: %w", dir, err)
		}
	}

	// Initialize egress router.
	router := egress.NewRouter(clrkv1alpha1.EgressPolicyAllowAll)

	// Initialize components.
	imageStore := NewImageStore(clrkImagesDir)
	sandboxMgr := NewSandboxManager(clrkStateDir, clrkRootDir, clrkLogsDir, imageStore, router)

	// Clean up orphaned containers from previous incarnation.
	if err := sandboxMgr.Cleanup(ctx); err != nil {
		log.Error(err, "Failed to cleanup orphaned containers")
	}

	// Daemon supervisor — owns one goroutine per elected DaemonAgent.
	daemonMgr := newDaemonLifecycleManager(ctx, sandboxMgr, r.Client, router, r.PodName)

	// TaskAgent dispatcher — accepts inbound HTTP requests routed to
	// this pod by the per-TaskAgent HTTPRoute and executes one
	// short-lived sandbox per request.
	active := NewActiveCounter()
	disp := NewDispatcher(r.Client, sandboxMgr, router, r.PodName, r.Namespace, active)

	// Set up SandboxState watcher.
	watcher := &sandboxWatcher{
		Client:     r.Client,
		sandboxMgr: sandboxMgr,
		daemonMgr:  daemonMgr,
		dispatcher: disp,
		poolName:   r.PoolName,
		podName:    r.PodName,
		namespace:  r.Namespace,
	}
	if err := watcher.setupWithManager(r.Manager); err != nil {
		return fmt.Errorf("setting up sandbox watcher: %w", err)
	}

	// Set up egress CRD watcher to rebuild routing table on changes.
	egressWatcher := egress.NewConfigWatcher(r.Client, router, r.Namespace)
	if err := egressWatcher.SetupWithManager(r.Manager); err != nil {
		return fmt.Errorf("setting up egress config watcher: %w", err)
	}

	// Start heartbeat goroutine.
	go watcher.heartbeatLoop(ctx, heartbeatInterval)

	// Start the dispatcher HTTP server. Failures get logged; the
	// daemon side stays up so DaemonAgents keep running even if the
	// dispatcher port is busy or misconfigured.
	dispatchAddr := fmt.Sprintf(":%d", clrkcontroller.DispatchPort)
	go func() {
		if err := disp.Run(ctx, dispatchAddr); err != nil {
			log.Error(err, "Dispatcher HTTP server exited", "addr", dispatchAddr)
		}
	}()

	log.Info("Worker runtime started", "dispatchAddr", dispatchAddr)

	// Block until context is cancelled.
	<-ctx.Done()

	// Graceful shutdown — drain daemon loops first so they don't race with
	// SandboxManager teardown. The dispatcher goroutine returns on its
	// own once Shutdown completes (triggered by ctx cancellation).
	log.Info("Shutting down worker runtime")
	daemonMgr.Shutdown()
	shutdown(sandboxMgr)

	return nil
}

// shutdown stops all running sandboxes and cleans up resources.
func shutdown(sandboxMgr *SandboxManager) {
	log := ctrl.Log.WithName("worker-runtime")

	for _, sb := range sandboxMgr.List() {
		log.Info("Stopping sandbox on shutdown", "sandboxID", sb.ID)
		shutdownCtx := context.Background()
		if sb.Phase == SandboxRunning {
			if err := sandboxMgr.Stop(shutdownCtx, sb.ID); err != nil {
				log.Error(err, "Failed to stop sandbox", "sandboxID", sb.ID)
			}
		}
		if err := sandboxMgr.Delete(shutdownCtx, sb.ID); err != nil {
			log.Error(err, "Failed to delete sandbox", "sandboxID", sb.ID)
		}
	}
}
