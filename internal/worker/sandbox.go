//go:build linux

package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
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

// Compile-time guard: *SandboxManager satisfies SandboxRuntime.
var _ SandboxRuntime = (*SandboxManager)(nil)

// SandboxManager manages sandbox lifecycle via gVisor runsc
// subprocesses. The runsc dispatch lives in cmd/worker/cli_linux.go
// (gVisor maincli import).
type SandboxManager struct {
	stateDir   string // runsc --root.
	rootDir    string // Per-sandbox rootfs overlay + trust + net config.
	logsDir    string // Per-agent stdio log files.
	podName    string
	imageStore *ImageStore

	// TODO(phase5): retire — sentrystack replaces the shared netstack.
	revStackMgr *RevisionStackManager

	mu        sync.Mutex
	sandboxes map[SandboxID]*SandboxInstance
	waiters   map[SandboxID]context.CancelFunc
	stdLogs   map[SandboxID]sandboxLogs
}

type sandboxLogs struct {
	stdout *sandboxLineWriter
	stderr *sandboxLineWriter
	file   *os.File
}

func NewSandboxManager(stateDir, rootDir, logsDir, podName string, imageStore *ImageStore, revStackMgr *RevisionStackManager) *SandboxManager {
	return &SandboxManager{
		stateDir:    stateDir,
		rootDir:     rootDir,
		logsDir:     logsDir,
		podName:     podName,
		imageStore:  imageStore,
		revStackMgr: revStackMgr,
		sandboxes:   make(map[SandboxID]*SandboxInstance),
		waiters:     make(map[SandboxID]context.CancelFunc),
		stdLogs:     make(map[SandboxID]sandboxLogs),
	}
}

// Create pulls the image, builds an OCI bundle, and runs `runsc
// create` to spawn the Sentry. The sandbox is left in the Ready phase
// for warm pool use — Start is a separate call.
func (m *SandboxManager) Create(
	ctx context.Context,
	id SandboxID,
	agentRef string,
	identity proxyproto.AgentIdentity,
	caPEM []byte,
	sandbox clrkv1alpha1.AgentSandbox,
	resources clrkv1alpha1.ExecutionResources,
	state *clrkv1alpha1.AgentState,
	stdio bool,
	attempt int32,
) (*SandboxInstance, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("sandboxID", id)

	m.mu.Lock()
	if _, exists := m.sandboxes[id]; exists {
		m.mu.Unlock()
		return nil, ErrAlreadyExists
	}
	m.mu.Unlock()

	log.Info("Creating sandbox")

	imgInfo, err := m.imageStore.EnsureImage(ctx, sandbox.Image)
	if err != nil {
		return nil, fmt.Errorf("ensuring image: %w", err)
	}
	sandboxRootFS := imgInfo.RootFS

	// The kernel netns is a placeholder under runsc + sentrystack — the
	// Sentry's PluginStack is the only network the sandbox sees — but
	// runsc still requires the path to exist (OCI namespace bind-mount).
	nsCfg, err := SetupNetNS(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("setting up netns: %w", err)
	}

	// Unwinds in LIFO order on error; cleared on success.
	var cleanup []func()
	defer func() {
		for i := len(cleanup) - 1; i >= 0; i-- {
			cleanup[i]()
		}
	}()
	pushCleanup := func(f func()) { cleanup = append(cleanup, f) }
	pushCleanup(func() { TeardownNetNS(nsCfg) })

	revAttach, err := m.revStackMgr.Attach(identity, nsCfg.TAPFD, nsCfg.GW, nsCfg.IP)
	if err != nil {
		return nil, fmt.Errorf("attaching sandbox to revision netstack: %w", err)
	}
	pushCleanup(revAttach.Detach)

	var extraMounts []specs.Mount
	if len(caPEM) > 0 {
		caPath, err := m.writeAgentCA(id, caPEM)
		if err != nil {
			return nil, fmt.Errorf("staging agent CA: %w", err)
		}
		pushCleanup(func() { m.removeAgentCA(id) })
		extraMounts = append(extraMounts, buildTrustMountsSpec(sandboxRootFS, caPath)...)
	}
	resolvPath, err := m.writeSandboxResolvConf(id, nsCfg.GW)
	if err != nil {
		return nil, fmt.Errorf("staging sandbox resolv.conf: %w", err)
	}
	pushCleanup(func() { m.removeSandboxNetConfig(id) })
	extraMounts = append(extraMounts, buildResolvMountSpec(resolvPath))
	if state != nil {
		hostPath, err := ensureStateDir(identity.Namespace, agentRef, state.SizeLimitMB)
		if err != nil {
			return nil, err
		}
		extraMounts = append(extraMounts, buildStateMountSpec(hostPath, state))
	}

	args := resolveProcessArgs(sandbox, imgInfo.Entrypoint)
	env := buildProcessEnv(sandbox.Env)
	annotations := buildSandboxAnnotations(identity, m.podName, attempt)
	spec := buildSpec(string(id), sandboxRootFS, args, env, resources, nsCfg.NSPath, extraMounts, annotations)

	bundleDir, err := m.ensureRunscBundleDir(id)
	if err != nil {
		return nil, err
	}
	pushCleanup(func() { m.removeRunscBundleDir(id) })
	if err := writeConfigJSON(bundleDir, spec); err != nil {
		return nil, fmt.Errorf("writing OCI bundle: %w", err)
	}

	sb := &SandboxInstance{
		ID:        id,
		AgentRef:  agentRef,
		Phase:     SandboxReady,
		RootFS:    sandboxRootFS,
		NetNS:     nsCfg.NSPath,
		TAPName:   nsCfg.TAPName,
		TAPFD:     nsCfg.TAPFD,
		SandboxIP: nsCfg.IP,
		GatewayIP: nsCfg.GW,
		stack:     revAttach,
		Sandbox:   sandbox,
		Resources: resources,
		Identity:  identity,
		CreatedAt: time.Now(),
	}

	if err := wireSandboxStdio(sb, stdio); err != nil {
		return nil, err
	}
	pushCleanup(func() {
		closeStdioChildren(sb)
		closeStdioParents(sb)
		closeStdioInternals(sb)
	})

	initStr, err := buildSandboxInitStr(sb)
	if err != nil {
		return nil, fmt.Errorf("building InitStr: %w", err)
	}

	createErr := runscCreate(ctx, runscCreateOpts{
		id:        string(id),
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
	m.sandboxes[id] = sb
	m.mu.Unlock()

	log.Info("Sandbox created")
	return sb, nil
}

// Start fires the Sentry's spec.Process. The user binary is running
// inside the sandbox after this returns.
func (m *SandboxManager) Start(ctx context.Context, id SandboxID) error {
	log := ctrl.LoggerFrom(ctx).WithValues("sandboxID", id)

	m.mu.Lock()
	sb, ok := m.sandboxes[id]
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}

	sbLogger := slog.With(
		slog.String("sandbox.id", string(id)),
		slog.String("agent.namespace", sb.Identity.Namespace),
		slog.String("agent.name", sb.Identity.Name),
		slog.String("agent.uid", sb.Identity.UID),
		slog.String("agent.revision", sb.Identity.Revision),
	)
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
	if err := runscStart(ctx, m.stateDir, string(id)); err != nil {
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

// SetEgressBackends configures the per-listener EG egress backends
// for a sandbox between Create and Start.
func (m *SandboxManager) SetEgressBackends(id SandboxID, backends []egress.BackendListener) error {
	m.mu.Lock()
	sb, ok := m.sandboxes[id]
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}
	sb.EgressBackends = backends
	if sb.stack != nil {
		sb.stack.SetEgressBackends(backends)
	}
	return nil
}

// SetEgressPolicy attaches the per-sandbox policy handle. Nil
// disables enforcement.
func (m *SandboxManager) SetEgressPolicy(id SandboxID, policy *egress.SandboxPolicy) error {
	m.mu.Lock()
	sb, ok := m.sandboxes[id]
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}
	sb.EgressPolicy = policy
	if sb.stack != nil {
		sb.stack.SetEgressPolicy(policy)
	}
	return nil
}

// SetInvocationID stamps the per-dispatch InvocationID into the
// sandbox's slot. Called once per dispatch (cold and warm path).
func (m *SandboxManager) SetInvocationID(id SandboxID, invocationID string) error {
	m.mu.Lock()
	sb, ok := m.sandboxes[id]
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}
	sb.Identity.InvocationID = invocationID
	if sb.stack != nil {
		sb.stack.SetInvocationID(invocationID)
	}
	return nil
}

// Wait blocks until the sandbox's init process exits and returns its
// exit code. ErrNotFound if the sandbox is unknown. The caller is
// responsible for calling Delete after Wait returns.
func (m *SandboxManager) Wait(ctx context.Context, id SandboxID) (int, error) {
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
		closeStdioChildren(sb)
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
func (m *SandboxManager) Stop(ctx context.Context, id SandboxID) error {
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
func (m *SandboxManager) Kill(ctx context.Context, id SandboxID) error {
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
func (m *SandboxManager) Delete(ctx context.Context, id SandboxID) error {
	log := ctrl.LoggerFrom(ctx).WithValues("sandboxID", id)

	m.mu.Lock()
	sb, ok := m.sandboxes[id]
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}

	if sb.stack != nil {
		sb.stack.Detach()
	}

	if err := runscDelete(ctx, m.stateDir, string(id)); err != nil && !isRunscNotExist(err) {
		log.Error(err, "Failed to destroy sandbox via runsc")
	}

	if err := TeardownNetNS(&NetNSConfig{
		NSName: netnsName(id),
		NSPath: sb.NetNS,
		TAPFD:  sb.TAPFD,
	}); err != nil {
		log.Error(err, "Failed to teardown netns")
	}

	m.removeAgentCA(id)
	m.removeSandboxNetConfig(id)
	m.removeRunscBundleDir(id)

	closeStdioChildren(sb)
	closeStdioParents(sb)
	closeStdioInternals(sb)

	m.mu.Lock()
	delete(m.sandboxes, id)
	if c, ok := m.waiters[id]; ok {
		c()
		delete(m.waiters, id)
	}
	if logs, ok := m.stdLogs[id]; ok && logs.file != nil {
		_ = logs.file.Close()
	}
	delete(m.stdLogs, id)
	m.mu.Unlock()

	log.Info("Sandbox deleted")
	return nil
}

// Status returns the current state of a sandbox.
func (m *SandboxManager) Status(ctx context.Context, id SandboxID) (*SandboxInstance, error) {
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
func (m *SandboxManager) List() []*SandboxInstance {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]*SandboxInstance, 0, len(m.sandboxes))
	for _, sb := range m.sandboxes {
		result = append(result, sb)
	}
	return result
}

// Cleanup removes orphaned sandboxes from a previous worker
// incarnation. Scans the runsc --root dir directly rather than forking
// `runsc list` — same information, no subprocess.
func (m *SandboxManager) Cleanup(ctx context.Context) error {
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

// Purge tears down any runsc + netns state left behind for id. Safe
// to call before Create against a stale ID; runsc delete is idempotent
// for not-found containers.
func (m *SandboxManager) Purge(ctx context.Context, id SandboxID) {
	log := ctrl.LoggerFrom(ctx).WithValues("sandboxID", id)

	if err := runscDelete(ctx, m.stateDir, string(id)); err != nil && !isRunscNotExist(err) {
		log.Error(err, "Destroy of orphaned sandbox failed; falling back to RemoveAll")
	}
	if err := os.RemoveAll(filepath.Join(m.stateDir, string(id))); err != nil {
		log.Error(err, "RemoveAll of state dir failed")
	}
	m.removeRunscBundleDir(id)
	TeardownNetNS(&NetNSConfig{NSName: netnsName(id)})
}

// wireSandboxStdio allocates pipes for the Sentry's stdio. Two
// pipes per stream in stdio mode: an inner pipe (Sentry writes →
// drain goroutine reads) and an outer pipe (drain goroutine writes →
// dispatcher reads). Daemons skip the outer pipe; everything still
// flows into the slog sink.
func wireSandboxStdio(sb *SandboxInstance, stdio bool) error {
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

func closeStdioChildren(sb *SandboxInstance) {
	if sb == nil {
		return
	}
	if sb.stdinChild != nil {
		_ = sb.stdinChild.Close()
		sb.stdinChild = nil
	}
	if sb.stdoutChild != nil {
		_ = sb.stdoutChild.Close()
		sb.stdoutChild = nil
	}
	if sb.stderrChild != nil {
		_ = sb.stderrChild.Close()
		sb.stderrChild = nil
	}
}

func closeStdioParents(sb *SandboxInstance) {
	if sb == nil {
		return
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

func closeStdioInternals(sb *SandboxInstance) {
	if sb == nil {
		return
	}
	if sb.stdoutInternalR != nil {
		_ = sb.stdoutInternalR.Close()
		sb.stdoutInternalR = nil
	}
	if sb.stderrInternalR != nil {
		_ = sb.stderrInternalR.Close()
		sb.stderrInternalR = nil
	}
	if sb.stdoutToDispatcher != nil {
		_ = sb.stdoutToDispatcher.Close()
		sb.stdoutToDispatcher = nil
	}
	if sb.stderrToDispatcher != nil {
		_ = sb.stderrToDispatcher.Close()
		sb.stderrToDispatcher = nil
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
