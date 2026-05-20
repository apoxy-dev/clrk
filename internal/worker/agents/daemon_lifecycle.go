//go:build linux

package agents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
	"github.com/apoxy-dev/clrk/internal/worker/sandbox"
)

// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=daemonagents,verbs=get;list;watch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=daemonagents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

const (
	// healthyRunThreshold is how long a sandbox must stay alive before its
	// next exit is treated as a normal restart instead of a crash for
	// back-off purposes.
	healthyRunThreshold = 10 * time.Second
	// maxBackoff caps the exponential restart back-off.
	maxBackoff = 60 * time.Second
)

// DaemonLifecycle owns one goroutine per DaemonAgent that this worker
// has been elected to run. The goroutine drives the sandbox lifecycle
// (Create → Start → Wait → restart per policy) and patches the parent
// DaemonAgent's status.
type DaemonLifecycle struct {
	sandboxMgr *sandbox.Manager
	client     client.Client
	podName    string
	// router is the worker-wide egress router. Used to register the
	// per-EG SandboxPolicy handle so the ConfigWatcher can refresh it
	// in place when EgressGateway / EgressL4Route CRDs change.
	router *egress.Router
	// baseCtx outlives any single reconcile call; loops are derived from
	// it so they don't get torn down when the reconcile that started them
	// returns.
	baseCtx context.Context

	mu    sync.Mutex
	loops map[types.NamespacedName]*daemonLoop
}

type daemonLoop struct {
	revName string
	cancel  context.CancelFunc
	done    chan struct{}
}

func NewDaemonLifecycle(baseCtx context.Context, sandboxMgr *sandbox.Manager, c client.Client, router *egress.Router, podName string) *DaemonLifecycle {
	return &DaemonLifecycle{
		sandboxMgr: sandboxMgr,
		client:     c,
		router:     router,
		podName:    podName,
		baseCtx:    baseCtx,
		loops:      make(map[types.NamespacedName]*daemonLoop),
	}
}

// Ensure starts a daemon loop for da bound to rev. If a loop already exists
// for the same revision it's a no-op; if it exists for a different revision
// it's drained first.
func (m *DaemonLifecycle) Ensure(da *clrkv1alpha1.DaemonAgent, rev *clrkv1alpha1.AgentSandboxRevision) {
	key := types.NamespacedName{Namespace: da.Namespace, Name: da.Name}

	m.mu.Lock()
	existing, ok := m.loops[key]
	if ok && existing.revName == rev.Name {
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()

	if ok {
		m.Stop(key)
	}

	loopCtx, cancel := context.WithCancel(m.baseCtx)
	loop := &daemonLoop{
		revName: rev.Name,
		cancel:  cancel,
		done:    make(chan struct{}),
	}

	m.mu.Lock()
	m.loops[key] = loop
	m.mu.Unlock()

	// Snapshot da/rev so the goroutine doesn't race against the caller's
	// reconciler reusing the pointers on the next reconcile.
	daCopy := da.DeepCopy()
	revCopy := rev.DeepCopy()

	go func() {
		defer close(loop.done)
		m.run(loopCtx, daCopy, revCopy)
		// Intentionally NOT deleting from the loops map. A clean run()
		// return means the supervisor decided to Stop (RestartPolicy
		// honored). The watcher reconciles AgentSandboxRevision on
		// every heartbeat status update; if Ensure could find no entry
		// it would respawn the loop and the agent would churn forever.
		// Stop() (called on rev change or agent deletion) and Shutdown()
		// are the only paths that remove map entries.
	}()
}

// Stop cancels and drains the loop for key, if any.
func (m *DaemonLifecycle) Stop(key types.NamespacedName) {
	m.mu.Lock()
	loop, ok := m.loops[key]
	if !ok {
		m.mu.Unlock()
		return
	}
	delete(m.loops, key)
	m.mu.Unlock()

	loop.cancel()
	<-loop.done
}

// StopByRevision stops the loop, if any, whose current revision matches
// (namespace, revName). The loops map is keyed by DaemonAgent
// namespaced-name; once a revision is deleted its labels are gone, so
// the watcher can no longer recover the agent name from the revision.
// Scan the loops map by stored revName instead.
func (m *DaemonLifecycle) StopByRevision(namespace, revName string) {
	m.mu.Lock()
	var key types.NamespacedName
	found := false
	for k, loop := range m.loops {
		if k.Namespace == namespace && loop.revName == revName {
			key = k
			found = true
			break
		}
	}
	m.mu.Unlock()
	if found {
		m.Stop(key)
	}
}

// GCMissing stops any loop whose current revision isn't present in
// liveRevs. Caller must pass the authoritative set of revisions
// targeting this worker's pool from a successful List(); a partial
// list would falsely GC live loops.
//
// Stops are dispatched in goroutines: Stop blocks on loop.done, and
// the heartbeat caller must not stall on a single supervisor that's
// slow to unwind (e.g. waiting on a SIGTERM-resistant sandbox to
// exit). Stop already removes the map entry atomically, so a
// subsequent GCMissing tick won't re-fire for the same key.
func (m *DaemonLifecycle) GCMissing(liveRevs map[types.NamespacedName]struct{}) {
	m.mu.Lock()
	var stale []types.NamespacedName
	for k, loop := range m.loops {
		revKey := types.NamespacedName{Namespace: k.Namespace, Name: loop.revName}
		if _, ok := liveRevs[revKey]; !ok {
			stale = append(stale, k)
		}
	}
	m.mu.Unlock()
	for _, k := range stale {
		go m.Stop(k)
	}
}

// Shutdown cancels every loop and waits for them all to exit.
func (m *DaemonLifecycle) Shutdown() {
	m.mu.Lock()
	loops := make([]*daemonLoop, 0, len(m.loops))
	for k, loop := range m.loops {
		loops = append(loops, loop)
		delete(m.loops, k)
	}
	m.mu.Unlock()

	for _, loop := range loops {
		loop.cancel()
	}
	for _, loop := range loops {
		<-loop.done
	}
}

// run is the per-DaemonAgent supervisor. It blocks until ctx is cancelled.
// The Create/Start/Wait/Delete sequence for a single attempt lives in
// runSandboxOnce; the per-iteration restart bookkeeping (egress
// readiness gate, backoff, restart decision) lives in runIteration.
func (m *DaemonLifecycle) run(ctx context.Context, da *clrkv1alpha1.DaemonAgent, rev *clrkv1alpha1.AgentSandboxRevision) {
	log := ctrl.LoggerFrom(ctx).WithName("daemon-lifecycle").WithValues(
		"daemonAgent", da.Name, "namespace", da.Namespace, "revision", rev.Name,
	)

	var (
		attempt    int32
		backoffExp int
	)
	for {
		if ctx.Err() != nil {
			return
		}
		if m.runIteration(ctx, da, rev, log, &attempt, &backoffExp) == restartOutcomeStop {
			return
		}
	}
}

// restartOutcome is the verdict of one runIteration pass — either keep
// looping (the supervisor will retry / restart) or unwind run() so the
// per-DaemonAgent goroutine exits.
type restartOutcome int

const (
	restartOutcomeContinue restartOutcome = iota
	restartOutcomeStop
)

// runIteration drives one full supervised attempt: egress readiness
// gate, Create/Start/Wait/Delete via runSandboxOnce, then the restart
// decision and (if applicable) crash-loop backoff. attempt and
// backoffExp are owned by run() and mutated here so the caller's loop
// only has to react to the outcome.
func (m *DaemonLifecycle) runIteration(
	ctx context.Context,
	da *clrkv1alpha1.DaemonAgent,
	rev *clrkv1alpha1.AgentSandboxRevision,
	baseLog logr.Logger,
	attempt *int32,
	backoffExp *int,
) restartOutcome {
	sandboxID := sandbox.SandboxID(fmt.Sprintf("da-%s-%s-%d-%d", da.Namespace, da.Name, rev.Generation, *attempt))
	log := baseLog.WithValues("sandboxID", sandboxID, "attempt", *attempt)

	// Resolve EG dependencies (CA, backend addresses, dialable
	// backend) BEFORE creating any sandbox state. The EG controller
	// bring-up can take 30-60s in dev; without this gate we'd Create
	// + Delete libcontainer state on every retry and burn the
	// per-attempt backoff multiple times before the EG is even
	// reachable.
	caPEM, backends, policy, err := m.waitForEgressReady(ctx, da, log)
	if err != nil {
		if ctx.Err() != nil {
			return restartOutcomeStop
		}
		log.Info("EgressGateway not ready; will retry", "error", err)
		if !m.sleepBackoff(ctx, backoffExp) {
			return restartOutcomeStop
		}
		return restartOutcomeContinue
	}

	identity := newAgentIdentity(proxyproto.AgentKindDaemon, da.Namespace, da.Name, string(da.UID), rev.Name)
	exitCode, ranFor, runErr := m.runSandboxOnce(ctx, da, rev, sandboxID, identity, caPEM, backends, policy, *attempt, log)
	if ctx.Err() != nil {
		// Rollover or shutdown — leave Phase as-is for the next loop.
		return restartOutcomeStop
	}
	if runErr != nil {
		// Create/Start failure already logged inside runSandboxOnce;
		// fall through to backoff so a stuck cold-start path doesn't
		// spin-loop.
		if !m.sleepBackoff(ctx, backoffExp) {
			return restartOutcomeStop
		}
		return restartOutcomeContinue
	}

	nextAttempt := *attempt + 1
	decision := decideRestart(da.Spec.RestartPolicy, exitCode, nil, nextAttempt, da.Spec.MaxRestarts)
	switch decision {
	case daemonDecisionStop:
		m.patchStatus(ctx, da, daemonStatusUpdate{
			phase:        clrkv1alpha1.DaemonPhaseStopped,
			clearUpSince: true,
			restartCount: *attempt,
		})
		return restartOutcomeStop
	case daemonDecisionRestart:
		if ranFor >= healthyRunThreshold {
			*backoffExp = 0
		} else {
			m.patchStatus(ctx, da, daemonStatusUpdate{
				phase:        clrkv1alpha1.DaemonPhaseCrashLoopBackOff,
				clearUpSince: true,
				restartCount: *attempt,
			})
			if !m.sleepBackoff(ctx, backoffExp) {
				return restartOutcomeStop
			}
		}
	}
	*attempt = nextAttempt
	return restartOutcomeContinue
}

// runSandboxOnce executes one supervised Create/Start/Wait/Delete
// cycle for the given sandboxID. Returns (exitCode, ranFor, nil) on a
// completed run (the caller decides whether to restart based on
// exitCode), or (0, 0, err) on a Create/Start failure that the caller
// should treat as a failed attempt and back off. Delete is always
// attempted under context.Background() so a cancelled ctx doesn't
// abort cleanup.
func (m *DaemonLifecycle) runSandboxOnce(
	ctx context.Context,
	da *clrkv1alpha1.DaemonAgent,
	rev *clrkv1alpha1.AgentSandboxRevision,
	sandboxID sandbox.SandboxID,
	identity proxyproto.AgentIdentity,
	caPEM []byte,
	backends []egress.BackendListener,
	policy *egress.SandboxPolicy,
	attempt int32,
	log logr.Logger,
) (int, time.Duration, error) {
	log.Info("Starting daemon sandbox")
	// Wipe any runsc + Sentry PluginStack setup path state left behind
	// by an earlier half-failed Create attempt at the same id. Without
	// this, a transient failure strands the state directory and every
	// subsequent retry rejects with "container with given ID already
	// exists".
	m.sandboxMgr.Purge(ctx, sandboxID)
	if _, err := m.sandboxMgr.Create(ctx, sandbox.CreateRequest{
		ID:        sandboxID,
		AgentRef:  da.Name,
		Identity:  identity,
		CAPEM:     caPEM,
		Sandbox:   rev.Spec.AgentSandbox,
		Resources: da.Spec.Resources,
		Attempt:   attempt,
	}); err != nil {
		log.Error(err, "Failed to create sandbox")
		return 0, 0, err
	}
	if len(backends) > 0 {
		if err := m.sandboxMgr.SetEgressBackends(sandboxID, backends); err != nil {
			log.Error(err, "Set egress backends failed")
		}
	}
	if policy != nil {
		if err := m.sandboxMgr.SetEgressPolicy(sandboxID, policy); err != nil {
			log.Error(err, "Set egress policy failed")
		}
	}

	startedAt := time.Now()
	if err := m.sandboxMgr.Start(ctx, sandboxID); err != nil {
		log.Error(err, "Failed to start sandbox")
		_ = m.sandboxMgr.Delete(context.Background(), sandboxID)
		return 0, 0, err
	}

	now := metav1.NewTime(startedAt)
	m.patchStatus(ctx, da, daemonStatusUpdate{
		phase:        clrkv1alpha1.DaemonPhaseRunning,
		upSince:      &now,
		restartCount: attempt,
	})

	var lifetimeTimer *time.Timer
	if da.Spec.MaxLifetimeSeconds != nil && *da.Spec.MaxLifetimeSeconds > 0 {
		d := time.Duration(*da.Spec.MaxLifetimeSeconds) * time.Second
		lifetimeTimer = time.AfterFunc(d, func() {
			log.Info("MaxLifetimeSeconds reached, stopping sandbox")
			_ = m.sandboxMgr.Stop(context.Background(), sandboxID)
		})
	}

	exitCode, waitErr := m.waitOrCancel(ctx, sandboxID)
	if lifetimeTimer != nil {
		lifetimeTimer.Stop()
	}
	ranFor := time.Since(startedAt)

	// Best-effort delete; use Background so a cancelled ctx doesn't
	// abort cleanup of the runsc + Sentry stack state.
	if err := m.sandboxMgr.Delete(context.Background(), sandboxID); err != nil && !errors.Is(err, sandbox.ErrNotFound) {
		log.Error(err, "Failed to delete sandbox")
	}

	if waitErr != nil {
		log.Error(waitErr, "Sandbox wait failed")
	}
	log.Info("Sandbox exited", "ranFor", ranFor, "exitCode", exitCode)
	return exitCode, ranFor, nil
}

// waitOrCancel blocks on the sandbox's exit, but races against ctx so a
// cancellation triggers a sandbox Stop and lets Wait return. If
// SIGTERM doesn't unblock Wait within sigtermGrace (e.g. the
// container's PID 1 is a `while true` loop that doesn't propagate
// signals), escalate to Kill (SIGKILL via libcontainer; init's death
// reaps the rest of the PID namespace). Wait then returns and run()
// unwinds. Without this escalation a non-cooperating sandbox strands
// its supervisor goroutine forever, blocking GCMissing/Stop callers
// and ultimately the heartbeat loop.
func (m *DaemonLifecycle) waitOrCancel(ctx context.Context, id sandbox.SandboxID) (int, error) {
	type waitResult struct {
		code int
		err  error
	}
	const sigtermGrace = 5 * time.Second
	ch := make(chan waitResult, 1)
	go func() {
		c, e := m.sandboxMgr.Wait(context.Background(), id)
		ch <- waitResult{code: c, err: e}
	}()
	select {
	case r := <-ch:
		return r.code, r.err
	case <-ctx.Done():
		_ = m.sandboxMgr.Stop(context.Background(), id)
		select {
		case r := <-ch:
			return r.code, r.err
		case <-time.After(sigtermGrace):
			_ = m.sandboxMgr.Kill(context.Background(), id)
			r := <-ch
			return r.code, r.err
		}
	}
}

// sleepBackoff sleeps with exponential back-off and returns false if ctx
// was cancelled mid-sleep.
func (m *DaemonLifecycle) sleepBackoff(ctx context.Context, exp *int) bool {
	d := time.Duration(math.Min(float64(maxBackoff), float64(time.Second)*math.Pow(2, float64(*exp))))
	*exp++
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

type daemonDecision int

const (
	daemonDecisionStop daemonDecision = iota
	daemonDecisionRestart
)

// decideRestart maps RestartPolicy + exit state + attempt count to a
// supervisor decision.
func decideRestart(policy clrkv1alpha1.RestartPolicy, exitCode int, waitErr error, nextAttempt int32, maxRestarts *int32) daemonDecision {
	if maxRestarts != nil && *maxRestarts > 0 && nextAttempt > *maxRestarts {
		return daemonDecisionStop
	}
	success := waitErr == nil && exitCode == 0
	switch policy {
	case clrkv1alpha1.RestartPolicyAlways, "":
		return daemonDecisionRestart
	case clrkv1alpha1.RestartPolicyOnFailure:
		if success {
			return daemonDecisionStop
		}
		return daemonDecisionRestart
	case clrkv1alpha1.RestartPolicyNever:
		return daemonDecisionStop
	}
	return daemonDecisionStop
}

type daemonStatusUpdate struct {
	phase        clrkv1alpha1.DaemonPhase
	upSince      *metav1.Time
	clearUpSince bool
	restartCount int32
}

// patchStatus merge-patches the worker-owned status fields (Phase / UpSince
// / RestartCount), leaving controller-written fields (Conditions,
// ObservedGeneration, Latest*RevisionName) untouched.
func (m *DaemonLifecycle) patchStatus(ctx context.Context, da *clrkv1alpha1.DaemonAgent, upd daemonStatusUpdate) {
	log := ctrl.LoggerFrom(ctx).WithName("daemon-lifecycle")

	statusObj := map[string]any{
		"phase":        string(upd.phase),
		"restartCount": upd.restartCount,
	}
	switch {
	case upd.clearUpSince:
		statusObj["upSince"] = nil
	case upd.upSince != nil:
		statusObj["upSince"] = upd.upSince.UTC().Format(time.RFC3339)
	}

	patch := map[string]any{"status": statusObj}
	body, err := json.Marshal(patch)
	if err != nil {
		log.Error(err, "Failed to marshal status patch")
		return
	}

	target := &clrkv1alpha1.DaemonAgent{}
	target.Namespace = da.Namespace
	target.Name = da.Name
	if err := m.client.Status().Patch(ctx, target, client.RawPatch(types.MergePatchType, body)); err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		log.Error(err, "Failed to patch DaemonAgent status")
	}
}

// waitForEgressReady polls until all of the DaemonAgent's EG
// dependencies are usable: the CA Secret exists, the EG status
// carries at least one listener BackendAddress, and at least one of
// those addresses is TCP-dialable. Returns (caPEM, backends, policy,
// nil) on success. Designed to be called BEFORE libcontainer Create so
// the cold-start window doesn't churn netstack state on every retry.
//
// CA load and backend resolution are kept as separate calls (vs. the
// dispatcher's single ResolveEgress) so the poll loop can distinguish
// "CA Secret not yet provisioned" from "EG status not yet populated"
// in the once-per-EG log line, rather than collapsing both into one
// composite error.
func (m *DaemonLifecycle) waitForEgressReady(
	ctx context.Context,
	da *clrkv1alpha1.DaemonAgent,
	log logr.Logger,
) ([]byte, []egress.BackendListener, *egress.SandboxPolicy, error) {
	const (
		warmupTimeout = 5 * time.Minute
		pollInterval  = 1 * time.Second
	)
	deadline := time.Now().Add(warmupTimeout)
	logged := false
	for {
		caPEM, caErr := LoadEgressCA(ctx, m.client, da.Namespace, da.Spec.EgressRefs)
		backends, policy, cfgErr := ResolveEgressNoCA(ctx, m.client, m.router, da.Namespace, da.Spec.EgressRefs)
		if caErr == nil && cfgErr == nil {
			if len(backends) == 0 {
				return caPEM, nil, nil, nil
			}
			// Probe the first backend; if it dials we assume the
			// EG-managed pod is fully up and the rest of the
			// listener ports are bound on the same Service. EG
			// brings them up as a unit, so per-listener gating
			// would just multiply latency in the cold-start path.
			probe := backends[0].Addr
			if err := waitTCPDialable(ctx, probe, 30*time.Second); err == nil {
				return caPEM, backends, policy, nil
			} else if !logged {
				log.Info("Egress backend not yet dialable; will keep polling", "backend", probe, "err", err)
				logged = true
			}
		} else if !logged {
			err := caErr
			if err == nil {
				err = cfgErr
			}
			log.Info("Egress prerequisites not ready; will keep polling", "err", err)
			logged = true
		}
		if time.Now().After(deadline) {
			err := caErr
			if err == nil {
				err = cfgErr
			}
			if err == nil {
				err = fmt.Errorf("EgressGateway warmup timed out after %s", warmupTimeout)
			}
			return nil, nil, nil, err
		}
		select {
		case <-ctx.Done():
			return nil, nil, nil, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// waitTCPDialable retries a TCP dial against addr until it succeeds or
// timeout elapses. ctx cancellation also exits early. Used to gate
// sandbox start on the EgressGateway's envoy-gateway pod actually
// serving its NodePort, not just the Service having an assigned port.
// APO-569.
func waitTCPDialable(ctx context.Context, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	const dialTimeout = 500 * time.Millisecond
	const probeInterval = 250 * time.Millisecond
	for {
		conn, err := (&net.Dialer{Timeout: dialTimeout}).DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("dialing %s: %w", addr, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(probeInterval):
		}
	}
}
