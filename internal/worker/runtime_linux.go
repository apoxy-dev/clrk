package worker

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress"
	"github.com/apoxy-dev/clrk/internal/ports"
	"github.com/apoxy-dev/clrk/internal/sandbox/metadata"
	"github.com/apoxy-dev/clrk/internal/worker/agents"
	"github.com/apoxy-dev/clrk/internal/worker/sandbox"
	"github.com/apoxy-dev/clrk/internal/workerlog"
)

const (
	// clrkStateDir is the runsc --root directory (was libcontainer's
	// state dir). runsc writes one subdirectory per sandbox keyed by
	// container ID; the same on-disk path is used across Create,
	// Start, Wait, Kill, Delete.
	clrkStateDir  = "/run/clrk/runsc"
	clrkRootDir   = "/run/clrk/rootfs"
	clrkImagesDir = "/run/clrk/images"
	clrkLogsDir   = workerlog.Dir
)

// Start initializes the runtime and blocks until the context is cancelled.
func (r *Runtime) Start(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx).WithName("worker-runtime")
	log.Info("Starting worker runtime",
		"pool", r.PoolName,
		"pod", r.PodName,
		"namespace", r.Namespace,
	)

	// Create runtime directories. The host-state root lives on the
	// worker host (not tmpfs) — fail fast if the deployment didn't
	// mount it writable. Path resolved via sandbox.HostStateRoot().
	for _, dir := range []string{clrkStateDir, clrkRootDir, clrkImagesDir, clrkLogsDir, sandbox.HostStateRoot()} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("creating dir %s: %w", dir, err)
		}
	}

	// Fail-closed on cgroup v1 or missing write access — silent
	// fallback would re-create the silent under-enforcement of
	// ExecutionResources we're fixing.
	workerCgroupPath, err := sandbox.InitWorkerCgroup()
	if err != nil {
		return fmt.Errorf("initializing worker cgroup: %w", err)
	}
	log.Info("Worker cgroup ready", "path", workerCgroupPath)

	// Deferred first so Close runs LAST after all sandbox/egress
	// teardown — late stages still emit denies and lifecycle events.
	r.Emitter = buildEmitter(ctx, r)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := r.Emitter.Close(shutdownCtx); err != nil {
			log.Error(err, "Shutting down OTLP emitter")
		}
	}()

	// Initialize egress router.
	router := egress.NewRouter(clrkv1alpha1.EgressPolicyAllowAll)

	// Worker-wide IMDS plumbing. Replaces the per-(TaskAgent, revision)
	// gonet listener: one host-bound HTTP server on 127.0.0.1:<port>
	// accepts PROXY v2 frames from the in-Sentry TCP forwarder of
	// every sandbox on this worker and demuxes by SandboxID TLV.
	metaReg := metadata.NewRegistry()
	imdsHostAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(ports.WorkerIMDSPort)))
	imdsServer, err := metadata.New(imdsHostAddr, metaReg.Lookup)
	if err != nil {
		return fmt.Errorf("starting IMDS server on %s: %w", imdsHostAddr, err)
	}
	defer imdsServer.Close()

	// Worker-wide egress bridge. Every non-IMDS, non-DNS outbound TCP
	// from the Sentry's PluginStack lands here for central policy +
	// MITM dispatch. Same SandboxID demux as the IMDS listener.
	egressHostAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(ports.WorkerEgressPort)))

	// Initialize components. Resolvers feed every Sentry's UDP/DNS
	// forwarder via InitStr.DNSResolvers — the in-Sentry forwarder
	// dials the worker's host resolvers for any :53 traffic.
	resolvers := sandbox.ReadWorkerResolvers()
	imageStore := sandbox.NewImageStore(clrkImagesDir)
	sandboxMgr := sandbox.NewManager(sandbox.ManagerConfig{
		StateDir:         clrkStateDir,
		RootDir:          clrkRootDir,
		LogsDir:          clrkLogsDir,
		PodName:          r.PodName,
		ImageStore:       imageStore,
		IMDSHostAddr:     imdsHostAddr,
		EgressHostAddr:   egressHostAddr,
		Resolvers:        resolvers,
		WorkerCgroupPath: workerCgroupPath,
		// Sandbox stdio fans out as OTLP LogRecords through the same
		// worker emitter the egress bridge uses (clrk.component=worker).
		LogEmitter: r.Emitter.Logger(),
	})

	egressBridge, err := sandbox.NewEgressBridge(egressHostAddr, sandboxMgr.LookupEgressState, r.Emitter)
	if err != nil {
		return fmt.Errorf("starting egress bridge on %s: %w", egressHostAddr, err)
	}
	defer egressBridge.Close()

	// Clean up orphaned containers from previous incarnation.
	if err := sandboxMgr.Cleanup(ctx); err != nil {
		log.Error(err, "Failed to cleanup orphaned containers")
	}

	// Daemon supervisor — owns one goroutine per elected DaemonAgent.
	daemonMgr := agents.NewDaemonLifecycle(ctx, sandboxMgr, r.Client, router, r.PodName)

	// TaskAgent dispatcher — accepts inbound HTTP requests routed to
	// this pod by the per-TaskAgent HTTPRoute and executes one
	// short-lived sandbox per request.
	active := agents.NewActiveCounter()
	disp := agents.NewDispatcher(agents.DispatcherConfig{
		Client:      r.Client,
		Runtime:     sandboxMgr,
		Router:      router,
		PodName:     r.PodName,
		Namespace:   r.Namespace,
		Active:      active,
		MetadataReg: metaReg,
		CMNATSAddr:  r.CMNATSAddr,
	})
	// Ship Running + terminal Invocation lifecycle events to the cm's
	// JetStream (APO-618). No-op when CMNATSAddr is empty; the in-memory
	// outbox + nats.go reconnect keep terminal events durable across a cm
	// bounce while this worker stays up.
	go func() {
		if err := disp.RunInvocationPublisher(ctx); err != nil {
			log.Error(err, "Invocation publisher exited")
		}
	}()

	// Warm pool shares the activeCounter's notifier so warm
	// fills/evictions trigger a status KV Put alongside in-flight
	// changes.
	warmPool := agents.NewWarmPool(sandboxMgr, r.Client, router, active.Notifier(), r.PoolName, r.PodName, 0)
	disp.SetWarmPool(warmPool)
	if err := warmPool.SetupWithManager(r.Manager); err != nil {
		return fmt.Errorf("setting up warm pool reconciler: %w", err)
	}

	// Set up SandboxState watcher. In-flight accounting lives in the
	// WORKER_STATUS KV the controller-manager watches — the watcher
	// only owns image-pull + heartbeat.
	watcher := agents.NewWatcher(r.Client, sandboxMgr, daemonMgr, r.PoolName, r.PodName, r.Namespace)
	if err := watcher.SetupWithManager(r.Manager); err != nil {
		return fmt.Errorf("setting up sandbox watcher: %w", err)
	}

	// Set up egress CRD watcher to rebuild routing table on changes.
	egressWatcher := egress.NewConfigWatcher(r.Client, router, r.Namespace)
	if err := egressWatcher.SetupWithManager(r.Manager); err != nil {
		return fmt.Errorf("setting up egress config watcher: %w", err)
	}

	// Start heartbeat goroutine. Cadence is the agents-package
	// heartbeatInterval const, baked into HeartbeatLoop.
	go watcher.HeartbeatLoop(ctx)

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

	// Publish this worker's routing state into the cm's WORKER_STATUS KV
	// bucket on every in-flight/warm change + a 5s floor. No-op when
	// CMNATSAddr is empty (status routing is then unavailable cluster-wide
	// and the ingress ext_proc 503s). The cm watches the bucket and joins
	// each snapshot to the worker's routable pod IP + readiness from the
	// pool's EndpointSlices.
	if r.CMNATSAddr != "" {
		statusPub := agents.NewStatusPublisher(r.CMNATSAddr, r.Namespace, r.PoolName, r.PodName, sandboxMgr, imageStore, active)
		go func() {
			if err := statusPub.Run(ctx); err != nil {
				log.Error(err, "Worker status publisher exited")
			}
		}()
	}

	log.Info("Worker runtime started", "dispatchAddr", dispatchAddr)

	// Block until context is cancelled.
	<-ctx.Done()

	// Order: flip the dispatcher to 503 new dispatches → stop warm
	// fills → drain in-flight HTTP (bounded to 30s by disp.Run's
	// srv.Shutdown) → drain daemons → destroy unconsumed warm
	// sandboxes → SIGTERM + grace + Destroy on anything still
	// running. Drain() comes before StopFill so the picker upstream
	// stops sending us work while srv.Shutdown is still inside the
	// 30s cooperative window.
	log.Info("Shutting down worker runtime")
	disp.Drain()
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
func shutdown(sandboxMgr *sandbox.Manager) {
	log := ctrl.Log.WithName("worker-runtime")
	shutdownCtx := context.Background()

	all := sandboxMgr.List()
	if len(all) == 0 {
		return
	}

	signaled := false
	for _, sb := range all {
		if sb.Phase != sandbox.SandboxRunning {
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
func waitForRunningExit(sandboxMgr *sandbox.Manager, deadline time.Duration) {
	end := time.Now().Add(deadline)
	for {
		anyRunning := false
		for _, sb := range sandboxMgr.List() {
			if sb.Phase == sandbox.SandboxRunning {
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
