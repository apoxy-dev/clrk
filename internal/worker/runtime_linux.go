package worker

import (
	"context"
	"fmt"
	"os"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	clrkStateDir  = "/run/clrk/state"
	clrkRootDir   = "/run/clrk/rootfs"
	clrkImagesDir = "/run/clrk/images"

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
	for _, dir := range []string{clrkStateDir, clrkRootDir, clrkImagesDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("creating dir %s: %w", dir, err)
		}
	}

	// Initialize components.
	imageStore := NewImageStore(clrkImagesDir)
	sandboxMgr := NewSandboxManager(clrkStateDir, clrkRootDir, imageStore)

	// Clean up orphaned containers from previous incarnation.
	if err := sandboxMgr.Cleanup(ctx); err != nil {
		log.Error(err, "Failed to cleanup orphaned containers")
	}

	// Set up SandboxState watcher.
	watcher := &sandboxWatcher{
		Client:     r.Client,
		sandboxMgr: sandboxMgr,
		poolName:   r.PoolName,
		podName:    r.PodName,
		namespace:  r.Namespace,
	}
	if err := watcher.setupWithManager(r.Manager); err != nil {
		return fmt.Errorf("setting up sandbox watcher: %w", err)
	}

	// Start heartbeat goroutine.
	go watcher.heartbeatLoop(ctx, heartbeatInterval)

	log.Info("Worker runtime started")

	// Block until context is cancelled.
	<-ctx.Done()

	// Graceful shutdown.
	log.Info("Shutting down worker runtime")
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
