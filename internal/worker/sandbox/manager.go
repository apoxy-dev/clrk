//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
	"github.com/apoxy-dev/clrk/internal/ports"
)

// Manager manages sandbox lifecycle via gVisor runsc
// subprocesses. The runsc dispatch lives in cmd/worker/cli_linux.go
// (gVisor maincli import).
//
// Implements the SandboxRuntime interface defined in package agents.
type Manager struct {
	stateDir   string // runsc --root.
	rootDir    string // Per-sandbox rootfs overlay + trust + net config.
	logsDir    string // Per-agent stdio log files.
	podName    string
	imageStore *ImageStore

	// imdsHostAddr is the worker-process IMDS dial target the Sentry
	// writes into the in-Sentry InitStr; the per-sandbox TCP forwarder
	// dials it (with a PROXY v2 SandboxID TLV) when it sees outbound
	// traffic to 169.254.169.254:80 / [fd00:ec2::254]:80.
	imdsHostAddr string

	// egressHostAddr is the worker-process egress dial target. The
	// in-Sentry TCP forwarder routes every non-IMDS, non-DNS
	// outbound stream here with a PROXY v2 SandboxID TLV so the
	// worker can look up identity/InvocationID/Backends/Policy and
	// take the central egress decision.
	egressHostAddr string

	// workerResolvers is the host-side DNS resolver list shipped to
	// each Sentry via InitStr.DNSResolvers. Captured once at worker
	// startup; never re-read.
	workerResolvers []netip.AddrPort

	// mu guards every map below. RWMutex because LookupEgressState is
	// on the per-egress-conn hot path (one acquire per accepted SYN
	// from any Sentry) and must not serialize against the lifecycle
	// writes (Create/Start/Destroy/Set{Backends,Policy,InvocationID}).
	mu           sync.RWMutex
	sandboxes    map[SandboxID]*Instance
	waiters      map[SandboxID]context.CancelFunc
	stdLogs      map[SandboxID]sandboxLogs
	egressStates map[SandboxID]*EgressState
}

type sandboxLogs struct {
	stdout *sandboxLineWriter
	stderr *sandboxLineWriter
	file   *os.File
}

// ManagerConfig bundles the construction-time inputs of NewManager.
// Field grouping mirrors concerns: filesystem roots, the per-pod
// identity, the image store, and the worker-bound dial targets used
// to render every sandbox's initStr.
type ManagerConfig struct {
	// StateDir is runsc's --root directory; one subdirectory per
	// sandbox keyed by container ID.
	StateDir string
	// RootDir holds per-sandbox rootfs overlays + trust + net config.
	RootDir string
	// LogsDir is the per-agent stdio log file root.
	LogsDir string
	// PodName tags every sandbox annotation so out-of-band tooling
	// can correlate libcontainer state with the owning worker pod.
	PodName string
	// ImageStore pulls and extracts OCI images. Shared across all
	// sandboxes on this worker.
	ImageStore *ImageStore
	// IMDSHostAddr is the worker-bound IMDS dial target (typically
	// "127.0.0.1:<WorkerIMDSPort>") shipped to each Sentry via
	// initStr. The in-Sentry TCP forwarder dials it when it matches
	// an outbound SYN to 169.254.169.254:80 / [fd00:ec2::254]:80.
	IMDSHostAddr string
	// EgressHostAddr is the worker-bound egress dial target shipped
	// via initStr. Every non-IMDS/DNS outbound TCP from the Sentry
	// lands here for central policy + MITM dispatch.
	EgressHostAddr string
	// Resolvers is the host-side DNS resolver list shipped to each
	// Sentry via InitStr.DNSResolvers.
	Resolvers []netip.AddrPort
}

// NewManager constructs the worker's sandbox lifecycle manager.
func NewManager(cfg ManagerConfig) *Manager {
	return &Manager{
		stateDir:        cfg.StateDir,
		rootDir:         cfg.RootDir,
		logsDir:         cfg.LogsDir,
		podName:         cfg.PodName,
		imageStore:      cfg.ImageStore,
		imdsHostAddr:    cfg.IMDSHostAddr,
		egressHostAddr:  cfg.EgressHostAddr,
		workerResolvers: cfg.Resolvers,
		sandboxes:       make(map[SandboxID]*Instance),
		waiters:         make(map[SandboxID]context.CancelFunc),
		stdLogs:         make(map[SandboxID]sandboxLogs),
		egressStates:    make(map[SandboxID]*EgressState),
	}
}

// EnsureImage pulls (or returns cached metadata for) ref via the
// manager's underlying ImageStore. Exposed so external callers
// (notably the agents-package Watcher, which used to reach into the
// private imageStore field) can drive image pulls without crossing
// the package boundary into private state.
func (m *Manager) EnsureImage(ctx context.Context, ref string) (*ImageInfo, error) {
	return m.imageStore.EnsureImage(ctx, ref)
}

// ImageStore returns the manager's image store. Exposed so callers
// that need the cached-refs query (the worker status service) can
// reach it without reaching into private fields.
func (m *Manager) ImageStore() *ImageStore { return m.imageStore }

// LookupEgressState returns the per-sandbox egress snapshot consulted
// by the worker's egress bridge on every accepted conn. Returns
// (zero, false) when no sandbox is registered — the bridge drops
// stray dials from unknown SandboxIDs rather than open-egress them.
//
// Exposed as a method (not a closure) so the bridge can hold a stable
// reference to the manager across sandbox lifecycle events.
func (m *Manager) LookupEgressState(sandboxID string) (EgressState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	st, ok := m.egressStates[SandboxID(sandboxID)]
	if !ok || st == nil {
		return EgressState{}, false
	}
	return *st, true
}

// egressStateLocked returns the per-sandbox state record, creating
// it lazily on first write. Must be called with m.mu held.
func (m *Manager) egressStateLocked(id SandboxID) *EgressState {
	st, ok := m.egressStates[id]
	if !ok {
		st = &EgressState{}
		m.egressStates[id] = st
	}
	return st
}

// CreateRequest bundles the per-call inputs of Manager.Create. The
// previous positional 10-arg signature was easy to mis-order and
// inflated every call site; CreateRequest keeps each field's role
// visible at the call site.
//
// AgentRef is the parent agent's K8s name (TaskAgent.Name or
// DaemonAgent.Name). It is distinct from Identity.Name on TaskAgents,
// where the latter holds the per-invocation name — AgentRef drives
// the per-(ns,agent) state dir path and the warm-pool ownership
// lookup, and must stay parent-scoped.
type CreateRequest struct {
	ID        SandboxID
	AgentRef  string
	Identity  proxyproto.AgentIdentity
	CAPEM     []byte
	Sandbox   clrkv1alpha1.AgentSandbox
	Resources clrkv1alpha1.ExecutionResources
	// State opts into a per-(ns,agent) persistent state bind-mount.
	// Nil for stateless callers (DaemonAgent, stateless TaskAgent).
	State *clrkv1alpha1.AgentState
	// Stdio requests dispatcher-facing stdio pipes on the resulting
	// Instance. Daemons set false; stdout/stderr still flow into the
	// per-agent log file via the line-splitter sink in either mode.
	Stdio bool
	// Attempt is the restart-attempt counter (daemons); 0 for
	// TaskAgent invocations.
	Attempt int32
}

// Create pulls the image, builds an OCI bundle, and runs `runsc
// create` to spawn the Sentry. The sandbox is left in the Ready phase
// for warm pool use — Start is a separate call.
func (m *Manager) Create(ctx context.Context, req CreateRequest) (*Instance, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("sandboxID", req.ID)

	m.mu.Lock()
	if _, exists := m.sandboxes[req.ID]; exists {
		m.mu.Unlock()
		return nil, ErrAlreadyExists
	}
	m.mu.Unlock()

	log.Info("Creating sandbox")

	imgInfo, err := m.imageStore.EnsureImage(ctx, req.Sandbox.Image)
	if err != nil {
		return nil, fmt.Errorf("ensuring image: %w", err)
	}
	sandboxRootFS := imgInfo.RootFS

	// Allocate the per-sandbox /30 (gw + container IP). No real netns
	// or TAP under sentrystack — the Sentry's PluginStack is the only
	// network the sandbox sees, the host-side OCI runtime inherits the
	// worker process's netns so the Sentry can dial 127.0.0.1 for the
	// IMDS bridge. The IPs only feed initStr + resolv.conf.
	gw, sandboxIP, err := allocateIPs()
	if err != nil {
		return nil, fmt.Errorf("allocating sandbox IPs: %w", err)
	}

	// Unwinds in LIFO order on error; cleared on success.
	var cleanup []func()
	defer func() {
		for i := len(cleanup) - 1; i >= 0; i-- {
			cleanup[i]()
		}
	}()
	pushCleanup := func(f func()) { cleanup = append(cleanup, f) }

	var extraMounts []specs.Mount
	if len(req.CAPEM) > 0 {
		caPath, err := m.writeAgentCA(req.ID, req.CAPEM)
		if err != nil {
			return nil, fmt.Errorf("staging agent CA: %w", err)
		}
		pushCleanup(func() { m.removeAgentCA(req.ID) })
		extraMounts = append(extraMounts, buildTrustMountsSpec(sandboxRootFS, caPath)...)
	}
	resolvPath, err := m.writeSandboxResolvConf(req.ID, gw)
	if err != nil {
		return nil, fmt.Errorf("staging sandbox resolv.conf: %w", err)
	}
	pushCleanup(func() { m.removeSandboxNetConfig(req.ID) })
	extraMounts = append(extraMounts, buildResolvMountSpec(resolvPath))
	if req.State != nil {
		hostPath, err := ensureStateDir(req.Identity.Namespace, req.AgentRef, req.State.SizeLimitMB)
		if err != nil {
			return nil, err
		}
		extraMounts = append(extraMounts, buildStateMountSpec(hostPath, req.State))
	}

	args := resolveProcessArgs(req.Sandbox, imgInfo.Entrypoint)
	env := buildProcessEnv(req.Sandbox.Env)
	annotations := buildSandboxAnnotations(req.Identity, m.podName, req.Attempt)
	spec := buildSpec(string(req.ID), sandboxRootFS, args, env, req.Resources, extraMounts, annotations)

	bundleDir, err := m.ensureRunscBundleDir(req.ID)
	if err != nil {
		return nil, err
	}
	pushCleanup(func() { m.removeRunscBundleDir(req.ID) })
	if err := writeConfigJSON(bundleDir, spec); err != nil {
		return nil, fmt.Errorf("writing OCI bundle: %w", err)
	}

	sb := &Instance{
		ID:        req.ID,
		AgentRef:  req.AgentRef,
		Phase:     SandboxReady,
		RootFS:    sandboxRootFS,
		SandboxIP: sandboxIP,
		GatewayIP: gw,
		Sandbox:   req.Sandbox,
		Resources: req.Resources,
		Identity:  req.Identity,
		CreatedAt: time.Now(),
	}

	if err := wireSandboxStdio(sb, req.Stdio); err != nil {
		return nil, err
	}
	pushCleanup(sb.closeStdio)

	initStr, err := buildSandboxInitStr(sb, m.imdsHostAddr, m.egressHostAddr, m.workerResolvers)
	if err != nil {
		return nil, fmt.Errorf("building InitStr: %w", err)
	}
	sb.initStr = initStr

	createErr := runscCreate(ctx, runscCreateOpts{
		id:        string(req.ID),
		rootDir:   m.stateDir,
		bundleDir: bundleDir,
		initStr:   initStr,
		stdin:     sb.stdinChild,
		stdout:    sb.stdoutChild,
		stderr:    sb.stderrChild,
	})
	if createErr != nil {
		return nil, fmt.Errorf("creating sandbox via runsc: %w", createErr)
	}

	cleanup = nil // success — keep all allocated resources.
	m.mu.Lock()
	m.sandboxes[req.ID] = sb
	// Seed the egress state record with the sandbox's identity so
	// the bridge has something useful to log before SetEgressBackends
	// / SetEgressPolicy / SetInvocationID fire (which they may not at
	// all, for DaemonAgents with no EgressRefs).
	st := m.egressStateLocked(req.ID)
	st.Identity = req.Identity
	m.mu.Unlock()

	log.Info("Sandbox created")
	return sb, nil
}

// Start fires the Sentry's spec.Process. The user binary is running
// inside the sandbox after this returns.
func (m *Manager) Start(ctx context.Context, id SandboxID) error {
	log := ctrl.LoggerFrom(ctx).WithValues("sandboxID", id)

	m.mu.Lock()
	sb, ok := m.sandboxes[id]
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}

	sbLogger := slog.With(append(
		[]any{slog.String("sandbox.id", string(id))},
		identityLogFields(sb.Identity)...,
	)...)
	logFile, err := openAgentLogFile(m.logsDir, sb.Identity.Namespace, sb.Identity.Name)
	if err != nil {
		log.Error(err, "Opening agent log file (continuing without file tee)")
	}
	stdoutLog := newSandboxLineWriter(sbLogger, slog.LevelInfo, "stdout", logFile)
	stderrLog := newSandboxLineWriter(sbLogger, slog.LevelWarn, "stderr", logFile)

	// Explicit nil-interface for the daemon case so drainSentryStdio's
	// nil-check actually fires (a typed-nil *os.File would slip through).
	var outSink, errSink io.WriteCloser
	if sb.stdoutToDispatcher != nil {
		outSink = sb.stdoutToDispatcher
	}
	if sb.stderrToDispatcher != nil {
		errSink = sb.stderrToDispatcher
	}
	if sb.stdoutInternalR != nil {
		go drainSentryStdio(sb.stdoutInternalR, outSink, stdoutLog)
	}
	if sb.stderrInternalR != nil {
		go drainSentryStdio(sb.stderrInternalR, errSink, stderrLog)
	}

	log.Info("Starting sandbox")
	if err := runscStart(ctx, m.stateDir, string(id), sb.initStr); err != nil {
		return err
	}

	m.mu.Lock()
	sb.Phase = SandboxRunning
	m.stdLogs[id] = sandboxLogs{stdout: stdoutLog, stderr: stderrLog, file: logFile}
	m.mu.Unlock()

	log.Info("Sandbox started")
	return nil
}

// drainSentryStdio fans the Sentry's stdio writes into the slog sink
// and (in stdio mode) the dispatcher-facing pipe. dispatcherSink is
// nil for daemons; closing it on EOF makes the dispatcher's sb.Stdout
// reader return cleanly.
func drainSentryStdio(src io.Reader, dispatcherSink io.WriteCloser, logSink io.Writer) {
	w := io.Writer(logSink)
	if dispatcherSink != nil {
		w = io.MultiWriter(dispatcherSink, logSink)
	}
	_, _ = io.Copy(w, src)
	if dispatcherSink != nil {
		_ = dispatcherSink.Close()
	}
}

// SetEgressBackends updates the per-listener EG egress backends on
// the worker-side egress state map consulted by the egress bridge on
// every dial.
func (m *Manager) SetEgressBackends(id SandboxID, backends []egress.BackendListener) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sandboxes[id]; !ok {
		return ErrNotFound
	}
	m.egressStateLocked(id).Backends = backends
	return nil
}

// SetEgressPolicy updates the per-sandbox policy handle. Nil disables
// enforcement; the bridge interprets a nil Policy as "allow all" so
// DaemonAgents without EgressRefs keep their direct-dial behaviour.
func (m *Manager) SetEgressPolicy(id SandboxID, policy *egress.SandboxPolicy) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sandboxes[id]; !ok {
		return ErrNotFound
	}
	m.egressStateLocked(id).Policy = policy
	return nil
}

// SetInvocationID stamps the per-dispatch InvocationID into both the
// sandbox's identity record (read by status / logs) and the egress
// state map (read by the egress bridge on every backend dial so the
// PROXY v2 TLV announces the current invocation).
func (m *Manager) SetInvocationID(id SandboxID, invocationID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb, ok := m.sandboxes[id]
	if !ok {
		return ErrNotFound
	}
	sb.Identity.InvocationID = invocationID
	m.egressStateLocked(id).Identity.InvocationID = invocationID
	return nil
}

// Wait blocks until the sandbox's init process exits and returns its
// exit code. ErrNotFound if the sandbox is unknown. The caller is
// responsible for calling Delete after Wait returns.
func (m *Manager) Wait(ctx context.Context, id SandboxID) (int, error) {
	m.mu.Lock()
	_, ok := m.sandboxes[id]
	m.mu.Unlock()
	if !ok {
		return -1, ErrNotFound
	}

	waitCtx, cancel := context.WithCancel(ctx)
	m.mu.Lock()
	m.waiters[id] = cancel
	m.mu.Unlock()

	exitCode, err := runscWait(waitCtx, m.stateDir, string(id))

	m.mu.Lock()
	sb, sbOK := m.sandboxes[id]
	if sbOK {
		sb.Phase = SandboxStopped
	}
	delete(m.waiters, id)
	cancel()
	logs, hasLogs := m.stdLogs[id]
	delete(m.stdLogs, id)
	m.mu.Unlock()

	if sbOK {
		// On Wait completion the Sentry exited; child-side FDs are
		// useless. Close everything so the dispatcher's reader on
		// sb.Stdout / sb.Stderr sees EOF promptly.
		sb.closeStdio()
	}

	if hasLogs {
		if logs.stdout != nil {
			logs.stdout.Flush()
		}
		if logs.stderr != nil {
			logs.stderr.Flush()
		}
		if logs.file != nil {
			_ = logs.file.Close()
		}
	}

	if err != nil {
		return -1, err
	}
	return exitCode, nil
}

// Stop sends SIGTERM to the sandbox's init via runsc.
func (m *Manager) Stop(ctx context.Context, id SandboxID) error {
	log := ctrl.LoggerFrom(ctx).WithValues("sandboxID", id)

	m.mu.Lock()
	sb, ok := m.sandboxes[id]
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}

	st, err := runscState(ctx, m.stateDir, string(id))
	if err != nil {
		if isRunscNotExist(err) {
			return ErrNotFound
		}
		return err
	}
	if st.Status == runscStatusStopped {
		m.mu.Lock()
		sb.Phase = SandboxStopped
		m.mu.Unlock()
		return nil
	}

	log.Info("Sending SIGTERM to sandbox", "pid", st.Pid)
	if err := runscKill(ctx, m.stateDir, string(id), "SIGTERM"); err != nil {
		return err
	}

	m.mu.Lock()
	sb.Phase = SandboxStopping
	m.mu.Unlock()
	return nil
}

// Kill sends SIGKILL to the sandbox's init via runsc.
func (m *Manager) Kill(ctx context.Context, id SandboxID) error {
	m.mu.Lock()
	_, ok := m.sandboxes[id]
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}

	if err := runscKill(ctx, m.stateDir, string(id), "SIGKILL"); err != nil {
		if isRunscNotExist(err) {
			return ErrNotFound
		}
		return fmt.Errorf("SIGKILL sandbox: %w", err)
	}
	return nil
}

// Delete destroys the sandbox and tears down the network namespace.
// `runsc delete --force` SIGKILLs anything still running.
func (m *Manager) Delete(ctx context.Context, id SandboxID) error {
	log := ctrl.LoggerFrom(ctx).WithValues("sandboxID", id)

	m.mu.Lock()
	sb, ok := m.sandboxes[id]
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}

	if err := runscDelete(ctx, m.stateDir, string(id)); err != nil && !isRunscNotExist(err) {
		log.Error(err, "Failed to destroy sandbox via runsc")
	}

	m.removeAgentCA(id)
	m.removeSandboxNetConfig(id)
	m.removeRunscBundleDir(id)
	// removeSandboxDebugLog intentionally NOT called — keep per-sandbox
	// Sentry logs for post-mortem inspection during the LLM-hang +
	// start-crash investigation. Re-enable after diagnosis.

	sb.closeStdio()

	// Collect the waiter cancel under the lock but invoke it after
	// release: cancelling a context can wake goroutines that need to
	// reacquire m.mu (e.g. the Wait goroutine itself, which grabs the
	// lock to flip Phase / delete the waiter), and holding the lock
	// across cancel() risks self-deadlock as soon as that wake-up
	// races with another method blocked on mu.
	m.mu.Lock()
	delete(m.sandboxes, id)
	cancelWaiter, hasWaiter := m.waiters[id]
	if hasWaiter {
		delete(m.waiters, id)
	}
	if logs, ok := m.stdLogs[id]; ok && logs.file != nil {
		_ = logs.file.Close()
	}
	delete(m.stdLogs, id)
	delete(m.egressStates, id)
	m.mu.Unlock()
	if hasWaiter {
		cancelWaiter()
	}

	log.Info("Sandbox deleted")
	return nil
}

// Status returns the current state of a sandbox.
func (m *Manager) Status(ctx context.Context, id SandboxID) (*Instance, error) {
	m.mu.Lock()
	sb, ok := m.sandboxes[id]
	m.mu.Unlock()
	if !ok {
		return nil, ErrNotFound
	}

	st, err := runscState(ctx, m.stateDir, string(id))
	if err != nil {
		if isRunscNotExist(err) {
			m.mu.Lock()
			sb.Phase = SandboxStopped
			m.mu.Unlock()
			return sb, nil
		}
		return nil, fmt.Errorf("getting runsc state: %w", err)
	}

	m.mu.Lock()
	sb.Phase = phaseFromRunscState(st.Status)
	m.mu.Unlock()
	return sb, nil
}

// List returns all tracked sandboxes.
func (m *Manager) List() []*Instance {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]*Instance, 0, len(m.sandboxes))
	for _, sb := range m.sandboxes {
		result = append(result, sb)
	}
	return result
}

// Cleanup removes orphaned sandboxes from a previous worker
// incarnation. Scans the runsc --root dir directly rather than forking
// `runsc list` — same information, no subprocess.
func (m *Manager) Cleanup(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx)
	log.Info("Cleaning up orphaned sandboxes")

	entries, err := os.ReadDir(m.stateDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("listing orphaned sandboxes: %w", err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		log.Info("Found orphaned sandbox, destroying", "id", e.Name())
		m.Purge(ctx, SandboxID(e.Name()))
	}
	return nil
}

// Purge tears down any runsc state left behind for id. Safe to call
// before Create against a stale ID; runsc delete is idempotent for
// not-found containers.
func (m *Manager) Purge(ctx context.Context, id SandboxID) {
	log := ctrl.LoggerFrom(ctx).WithValues("sandboxID", id)

	if err := runscDelete(ctx, m.stateDir, string(id)); err != nil && !isRunscNotExist(err) {
		log.Error(err, "Destroy of orphaned sandbox failed; falling back to RemoveAll")
	}
	if err := os.RemoveAll(filepath.Join(m.stateDir, string(id))); err != nil {
		log.Error(err, "RemoveAll of state dir failed")
	}
	m.removeRunscBundleDir(id)
}

// wireSandboxStdio allocates pipes for the Sentry's stdio. Two
// pipes per stream in stdio mode: an inner pipe (Sentry writes →
// drain goroutine reads) and an outer pipe (drain goroutine writes →
// dispatcher reads). Daemons skip the outer pipe; everything still
// flows into the slog sink.
func wireSandboxStdio(sb *Instance, stdio bool) error {
	var toClose []*os.File
	cleanup := func() {
		for _, f := range toClose {
			_ = f.Close()
		}
	}
	pipe := func() (r, w *os.File, err error) {
		r, w, err = os.Pipe()
		if err == nil {
			toClose = append(toClose, r, w)
		}
		return
	}

	outChildR, outChildW, err := pipe()
	if err != nil {
		return fmt.Errorf("creating stdout child pipe: %w", err)
	}
	sb.stdoutChild = outChildW
	sb.stdoutInternalR = outChildR

	errChildR, errChildW, err := pipe()
	if err != nil {
		cleanup()
		return fmt.Errorf("creating stderr child pipe: %w", err)
	}
	sb.stderrChild = errChildW
	sb.stderrInternalR = errChildR

	if !stdio {
		return nil
	}

	inR, inW, err := pipe()
	if err != nil {
		cleanup()
		return fmt.Errorf("creating stdin pipe: %w", err)
	}
	sb.Stdin = inW
	sb.stdinChild = inR

	outerOutR, outerOutW, err := pipe()
	if err != nil {
		cleanup()
		return fmt.Errorf("creating outer stdout pipe: %w", err)
	}
	sb.Stdout = outerOutR
	sb.stdoutToDispatcher = outerOutW

	outerErrR, outerErrW, err := pipe()
	if err != nil {
		cleanup()
		return fmt.Errorf("creating outer stderr pipe: %w", err)
	}
	sb.Stderr = outerErrR
	sb.stderrToDispatcher = outerErrW

	return nil
}

// closeStdio tears down every stdio FD on the instance. Replaces the
// prior trio of closeStdio{Children,Parents,Internals} helpers — they
// were always called together at every site (Create cleanup, Wait
// completion, Delete), and splitting them into three obscured that
// the lifecycle is monolithic.
func (sb *Instance) closeStdio() {
	if sb == nil {
		return
	}
	closers := []*struct {
		f **os.File
	}{
		{&sb.stdinChild},
		{&sb.stdoutChild},
		{&sb.stderrChild},
		{&sb.stdoutInternalR},
		{&sb.stderrInternalR},
		{&sb.stdoutToDispatcher},
		{&sb.stderrToDispatcher},
	}
	for _, c := range closers {
		if *c.f != nil {
			_ = (*c.f).Close()
			*c.f = nil
		}
	}
	if sb.Stdin != nil {
		_ = sb.Stdin.Close()
		sb.Stdin = nil
	}
	if sb.Stdout != nil {
		_ = sb.Stdout.Close()
		sb.Stdout = nil
	}
	if sb.Stderr != nil {
		_ = sb.Stderr.Close()
		sb.Stderr = nil
	}
}

// runsc emits these OCI status values; see runsc/container/status.go.
const (
	runscStatusCreating = "creating"
	runscStatusCreated  = "created"
	runscStatusRunning  = "running"
	runscStatusStopped  = "stopped"
)

func phaseFromRunscState(status string) SandboxPhase {
	switch status {
	case runscStatusCreating:
		return SandboxCreating
	case runscStatusCreated:
		return SandboxReady
	case runscStatusRunning:
		return SandboxRunning
	default:
		return SandboxStopped
	}
}

// isRunscNotExist reports whether err is runsc's "container not
// found" shape. runsc folds the phrase into stderr from CombinedOutput.
func isRunscNotExist(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "does not exist")
}

// buildProcessEnv assembles the env passed to the sandbox process via
// the OCI spec. Order: TLS trust overrides, IMDS URL constants, then
// user-supplied AgentSandbox.Env. PATH is appended only if not already
// set.
func buildProcessEnv(userEnv []corev1.EnvVar) []string {
	env := append([]string(nil), trustEnv("/etc/ssl/certs/ca-certificates.crt")...)
	env = append(env,
		fmt.Sprintf("CLRK_METADATA_URL=http://%s/v1", ports.MetadataAddrV4),
		fmt.Sprintf("CLRK_METADATA_URL_V6=http://[%s]/v1", ports.MetadataAddrV6),
	)
	env = append(env, envVarsToStrings(userEnv)...)
	hasPath := false
	for _, e := range env {
		if strings.HasPrefix(e, "PATH=") {
			hasPath = true
			break
		}
	}
	if !hasPath {
		env = append(env, "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	}
	return env
}

// envVarsToStrings converts corev1.EnvVar slice to "KEY=VALUE" strings.
func envVarsToStrings(envVars []corev1.EnvVar) []string {
	result := make([]string, 0, len(envVars))
	for _, ev := range envVars {
		result = append(result, fmt.Sprintf("%s=%s", ev.Name, ev.Value))
	}
	return result
}
