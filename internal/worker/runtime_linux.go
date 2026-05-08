package worker

import (
	"context"
	"fmt"
	"os"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress"
	"github.com/apoxy-dev/clrk/internal/ports"
	"github.com/apoxy-dev/clrk/internal/workerlog"
)

const (
	clrkStateDir  = "/run/clrk/state"
	clrkRootDir   = "/run/clrk/rootfs"
	clrkImagesDir = "/run/clrk/images"
	clrkLogsDir   = workerlog.Dir

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

	// Create runtime directories. clrkStateHostRoot lives on the
	// worker host (not tmpfs) — fail fast if the deployment didn't
	// mount it writable.
	for _, dir := range []string{clrkStateDir, clrkRootDir, clrkImagesDir, clrkLogsDir, clrkStateHostRoot} {
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

	// Warm pool shares the activeCounter's notifier so warm
	// fills/evictions push deltas to the WorkerStatusService stream
	// alongside in-flight changes.
	warmPool := NewWarmPool(sandboxMgr, r.Client, router, active.Notifier(), r.PoolName, r.PodName, 0)
	disp.SetWarmPool(warmPool)
	go func() {
		if err := warmPool.Run(ctx); err != nil {
			log.Error(err, "Warm pool reconciler exited")
		}
	}()

	// Set up SandboxState watcher. The dispatcher reference is no
	// longer needed — ActiveExecutions accounting moved to the
	// WorkerStatusService gRPC stream consumed by the controller.
	_ = disp
	watcher := &sandboxWatcher{
		Client:     r.Client,
		sandboxMgr: sandboxMgr,
		daemonMgr:  daemonMgr,
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

	// dispDone is closed when disp.Run returns so the shutdown
	// sequence can wait for in-flight requests to drain (disp.Run's
	// srv.Shutdown carries a 30s timeout) before SandboxManager
	// teardown SIGTERMs the rest. Failures get logged; the daemon
	// side stays up so DaemonAgents keep running even if the
	// dispatcher port is busy.
	dispatchAddr := fmt.Sprintf(":%d", ports.DispatchPort)
	dispDone := make(chan struct{})
	go func() {
		defer close(dispDone)
		if err := disp.Run(ctx, dispatchAddr); err != nil {
			log.Error(err, "Dispatcher HTTP server exited", "addr", dispatchAddr)
		}
	}()

	// Start the worker status gRPC server. controller-manager opens
	// one Watch stream per pod (sourced from the WorkerPool's
	// EndpointSlice) and feeds the in-memory routing state map.
	statusAddr := fmt.Sprintf(":%d", ports.WorkerStatusPort)
	statusSvc := NewStatusService(sandboxMgr, imageStore, active)
	go func() {
		if err := RunStatusServer(ctx, statusAddr, statusSvc); err != nil {
			log.Error(err, "Worker status server exited", "addr", statusAddr)
		}
	}()

	log.Info("Worker runtime started", "dispatchAddr", dispatchAddr, "statusAddr", statusAddr)

	// Block until context is cancelled.
	<-ctx.Done()

	// Order: stop warm fills → drain in-flight HTTP (bounded to 30s
	// by disp.Run's srv.Shutdown) → drain daemons → destroy
	// unconsumed warm sandboxes → SIGTERM + grace + Destroy on
	// anything still running. DestroyAll runs after the dispatcher
	// has stopped accepting requests so warm sandboxes are guaranteed
	// to be unconsumed.
	log.Info("Shutting down worker runtime")
	warmPool.StopFill()
	<-dispDone
	daemonMgr.Shutdown()
	warmPool.DestroyAll(context.Background())
	shutdown(sandboxMgr)

	return nil
}

// sandboxSIGTERMGrace caps the wait between SIGTERM and the
// libcontainer Destroy that follows (which SIGKILLs whatever's
// left). The wait short-circuits as soon as every sandbox has exited.
const (
	sandboxSIGTERMGrace = 5 * time.Second
	sandboxExitPoll     = 100 * time.Millisecond
)

// shutdown stops every running sandbox, polls up to sandboxSIGTERMGrace
// for them to exit cleanly, then Deletes all remaining (which
// destroys the libcontainer container and SIGKILLs any leftover PIDs
// via the cgroup).
func shutdown(sandboxMgr *SandboxManager) {
	log := ctrl.Log.WithName("worker-runtime")
	shutdownCtx := context.Background()

	all := sandboxMgr.List()
	if len(all) == 0 {
		return
	}

	signaled := false
	for _, sb := range all {
		if sb.Phase != SandboxRunning {
			continue
		}
		log.Info("SIGTERM sandbox on shutdown", "sandboxID", sb.ID)
		if err := sandboxMgr.Stop(shutdownCtx, sb.ID); err != nil {
			log.Error(err, "Failed to stop sandbox", "sandboxID", sb.ID)
			continue
		}
		signaled = true
	}
	if signaled {
		waitForRunningExit(sandboxMgr, sandboxSIGTERMGrace)
	}

	for _, sb := range sandboxMgr.List() {
		log.Info("Destroying sandbox on shutdown", "sandboxID", sb.ID)
		if err := sandboxMgr.Delete(shutdownCtx, sb.ID); err != nil {
			log.Error(err, "Failed to delete sandbox", "sandboxID", sb.ID)
		}
	}
}

// waitForRunningExit polls until no sandbox is in SandboxRunning or
// the deadline elapses, whichever comes first. Lets a clean SIGTERM
// shorten shutdown without giving up the SIGKILL fallback.
func waitForRunningExit(sandboxMgr *SandboxManager, deadline time.Duration) {
	end := time.Now().Add(deadline)
	for {
		anyRunning := false
		for _, sb := range sandboxMgr.List() {
			if sb.Phase == SandboxRunning {
				anyRunning = true
				break
			}
		}
		if !anyRunning {
			return
		}
		left := time.Until(end)
		if left <= 0 {
			return
		}
		sleep := sandboxExitPoll
		if left < sleep {
			sleep = left
		}
		time.Sleep(sleep)
	}
}
