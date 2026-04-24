//go:build linux

package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	clrkcontroller "github.com/apoxy-dev/clrk/internal/controller"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
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

// daemonLifecycleManager owns one goroutine per DaemonAgent that this worker
// has been elected to run. The goroutine drives the sandbox lifecycle
// (Create → Start → Wait → restart per policy) and patches the parent
// DaemonAgent's status.
type daemonLifecycleManager struct {
	sandboxMgr *SandboxManager
	client     client.Client
	podName    string
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

func newDaemonLifecycleManager(baseCtx context.Context, sandboxMgr *SandboxManager, c client.Client, podName string) *daemonLifecycleManager {
	return &daemonLifecycleManager{
		sandboxMgr: sandboxMgr,
		client:     c,
		podName:    podName,
		baseCtx:    baseCtx,
		loops:      make(map[types.NamespacedName]*daemonLoop),
	}
}

// Ensure starts a daemon loop for da bound to rev. If a loop already exists
// for the same revision it's a no-op; if it exists for a different revision
// it's drained first.
func (m *daemonLifecycleManager) Ensure(da *clrkv1alpha1.DaemonAgent, rev *clrkv1alpha1.AgentSandboxRevision) {
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

		m.mu.Lock()
		// Only clear the map entry if we're still the registered loop —
		// a rollover could have already replaced us.
		if cur, ok := m.loops[key]; ok && cur == loop {
			delete(m.loops, key)
		}
		m.mu.Unlock()
	}()
}

// Stop cancels and drains the loop for key, if any.
func (m *daemonLifecycleManager) Stop(key types.NamespacedName) {
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

// Shutdown cancels every loop and waits for them all to exit.
func (m *daemonLifecycleManager) Shutdown() {
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
func (m *daemonLifecycleManager) run(ctx context.Context, da *clrkv1alpha1.DaemonAgent, rev *clrkv1alpha1.AgentSandboxRevision) {
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

		sandboxID := SandboxID(fmt.Sprintf("da-%s-%s-%d-%d", da.Namespace, da.Name, rev.Generation, attempt))
		log := log.WithValues("sandboxID", sandboxID, "attempt", attempt)

		identity := proxyproto.AgentIdentity{
			Kind:      proxyproto.AgentKindDaemon,
			Namespace: da.Namespace,
			Name:      da.Name,
			UID:       string(da.UID),
			Revision:  rev.Name,
		}

		caPEM, err := m.loadEgressCA(ctx, da)
		if err != nil {
			log.Error(err, "Failed to load EgressGateway CA")
			if !m.sleepBackoff(ctx, &backoffExp) {
				return
			}
			continue
		}

		log.Info("Starting daemon sandbox")
		if _, err := m.sandboxMgr.Create(ctx, sandboxID, da.Name, identity, caPEM, rev.Spec.AgentSandbox, da.Spec.Resources); err != nil {
			log.Error(err, "Failed to create sandbox")
			if !m.sleepBackoff(ctx, &backoffExp) {
				return
			}
			continue
		}
		if backend := egressBackend(da); backend != "" {
			if err := m.sandboxMgr.SetEgressBackend(sandboxID, backend); err != nil {
				log.Error(err, "Set egress backend failed")
			}
		}
		startedAt := time.Now()
		if err := m.sandboxMgr.Start(ctx, sandboxID); err != nil {
			log.Error(err, "Failed to start sandbox")
			_ = m.sandboxMgr.Delete(context.Background(), sandboxID)
			if !m.sleepBackoff(ctx, &backoffExp) {
				return
			}
			continue
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

		state, waitErr := m.waitOrCancel(ctx, sandboxID)
		if lifetimeTimer != nil {
			lifetimeTimer.Stop()
		}
		ranFor := time.Since(startedAt)

		// Best-effort delete; use Background so a cancelled ctx doesn't
		// abort cleanup of the libcontainer + netns.
		if err := m.sandboxMgr.Delete(context.Background(), sandboxID); err != nil && !errors.Is(err, ErrNotFound) {
			log.Error(err, "Failed to delete sandbox")
		}

		if ctx.Err() != nil {
			// Rollover or shutdown — leave Phase as-is for the next loop.
			return
		}
		if waitErr != nil {
			log.Error(waitErr, "Sandbox wait failed")
		}
		log.Info("Sandbox exited", "ranFor", ranFor, "state", processStateString(state))

		nextAttempt := attempt + 1
		decision := decideRestart(da.Spec.RestartPolicy, state, nextAttempt, da.Spec.MaxRestarts)
		switch decision {
		case daemonDecisionStop:
			m.patchStatus(ctx, da, daemonStatusUpdate{
				phase:        clrkv1alpha1.DaemonPhaseStopped,
				clearUpSince: true,
				restartCount: attempt,
			})
			return
		case daemonDecisionRestart:
			if ranFor >= healthyRunThreshold {
				backoffExp = 0
			} else {
				m.patchStatus(ctx, da, daemonStatusUpdate{
					phase:        clrkv1alpha1.DaemonPhaseCrashLoopBackOff,
					clearUpSince: true,
					restartCount: attempt,
				})
				if !m.sleepBackoff(ctx, &backoffExp) {
					return
				}
			}
		}
		attempt = nextAttempt
	}
}

// waitOrCancel blocks on the sandbox's exit, but races against ctx so a
// cancellation triggers a sandbox Stop and lets Wait return.
func (m *daemonLifecycleManager) waitOrCancel(ctx context.Context, id SandboxID) (*os.ProcessState, error) {
	type waitResult struct {
		state *os.ProcessState
		err   error
	}
	ch := make(chan waitResult, 1)
	go func() {
		s, e := m.sandboxMgr.Wait(context.Background(), id)
		ch <- waitResult{state: s, err: e}
	}()
	select {
	case r := <-ch:
		return r.state, r.err
	case <-ctx.Done():
		_ = m.sandboxMgr.Stop(context.Background(), id)
		r := <-ch
		return r.state, r.err
	}
}

// sleepBackoff sleeps with exponential back-off and returns false if ctx
// was cancelled mid-sleep.
func (m *daemonLifecycleManager) sleepBackoff(ctx context.Context, exp *int) bool {
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
func decideRestart(policy clrkv1alpha1.RestartPolicy, state *os.ProcessState, nextAttempt int32, maxRestarts *int32) daemonDecision {
	if maxRestarts != nil && *maxRestarts > 0 && nextAttempt > *maxRestarts {
		return daemonDecisionStop
	}
	success := state != nil && state.Success()
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

func processStateString(state *os.ProcessState) string {
	if state == nil {
		return "<nil>"
	}
	return state.String()
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
func (m *daemonLifecycleManager) patchStatus(ctx context.Context, da *clrkv1alpha1.DaemonAgent, upd daemonStatusUpdate) {
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

// egressBackend returns the DNS address workers dial for the DaemonAgent's
// first EgressGateway ref. The EgressGateway controller creates a Gateway
// (and thus a Service) per EG using the GatewayNamePrefix convention; we
// resolve it by DNS. Empty return means "no MITM, direct dial".
func egressBackend(da *clrkv1alpha1.DaemonAgent) string {
	if len(da.Spec.EgressRefs) == 0 {
		return ""
	}
	egName := da.Spec.EgressRefs[0].GatewayRef
	// Envoy Gateway names its Service "envoy-<gateway-namespace>-
	// <gateway-name>-<hash>"; the hash is non-deterministic so we rely on
	// a clrk-side headless Service created by the EgressGateway
	// controller under the same name as the Gateway. Follow-up: replace
	// with a lookup off EgressGateway.Status.BackendAddress when the
	// controller populates it.
	return fmt.Sprintf("clrk-eg-%s.%s.svc.cluster.local:%d",
		egName, da.Namespace, clrkcontroller.EgressListenerPort)
}

// loadEgressCA fetches the MITM CA cert PEM for the DaemonAgent's first
// EgressGateway ref. An agent with no EgressRefs has nothing to MITM, so we
// return nil (sandbox is then created without trust injection and egress
// either fails closed or is bypassed depending on route table policy). An
// error here means the Secret hasn't landed yet and the caller should back
// off.
func (m *daemonLifecycleManager) loadEgressCA(ctx context.Context, da *clrkv1alpha1.DaemonAgent) ([]byte, error) {
	if len(da.Spec.EgressRefs) == 0 {
		return nil, nil
	}
	egName := da.Spec.EgressRefs[0].GatewayRef
	var sec corev1.Secret
	key := types.NamespacedName{
		Name:      clrkcontroller.EgressGatewayCASecretName(egName),
		Namespace: da.Namespace,
	}
	if err := m.client.Get(ctx, key, &sec); err != nil {
		return nil, fmt.Errorf("fetching EgressGateway CA secret %s: %w", key, err)
	}
	caPEM := sec.Data[corev1.TLSCertKey]
	if len(caPEM) == 0 {
		return nil, fmt.Errorf("EgressGateway CA secret %s has empty %s", key, corev1.TLSCertKey)
	}
	return caPEM, nil
}
