//go:build linux

package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/opencontainers/runc/libcontainer"
	"github.com/opencontainers/runc/libcontainer/configs"
	"github.com/opencontainers/runc/libcontainer/devices"
	"github.com/opencontainers/runc/libcontainer/specconv"
	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	// Enable cgroup manager to manage devices. runc v1.3 split this out of
	// the runc module into opencontainers/cgroups.
	_ "github.com/opencontainers/cgroups/devices"
	// nsenter is required for libcontainer container init.
	_ "github.com/opencontainers/runc/libcontainer/nsenter"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
	"github.com/apoxy-dev/clrk/internal/netstack"
	"github.com/apoxy-dev/clrk/internal/ports"
)

// Compile-time guard: *SandboxManager satisfies SandboxRuntime.
var _ SandboxRuntime = (*SandboxManager)(nil)

func init() {
	if len(os.Args) > 1 && os.Args[1] == "init" {
		// This is the golang entry point for runc init, executed
		// before main() but after libcontainer/nsenter's nsexec().
		libcontainer.Init()
	}
}

// SandboxManager manages the lifecycle of sandbox containers via libcontainer.
type SandboxManager struct {
	stateDir   string // libcontainer state dir (e.g. /run/clrk/state).
	rootDir    string // Per-sandbox rootfs overlay dir (e.g. /run/clrk/rootfs).
	logsDir    string // Per-agent stdio log files (e.g. /run/clrk/logs).
	imageStore *ImageStore
	dialer     netstack.Dialer // Egress dialer for sandbox netstacks.
	// workerResolvers is the worker container's own DNS server list.
	// Used to rewrite sandbox :53 dials in IdentityDialer — see dns.go.
	workerResolvers []netip.AddrPort

	mu        sync.Mutex
	sandboxes map[SandboxID]*SandboxInstance
	// containers retains the *libcontainer.Container returned by Create so
	// Start can call ctr.Start() directly. libcontainer only writes
	// state.json once Start runs, so a Create→Load handoff fails with
	// "container does not exist" — the in-memory handle is the source of
	// truth between those two calls.
	containers map[SandboxID]*libcontainer.Container
	// processes retains the libcontainer.Process for each running sandbox
	// so callers (Wait) can block on its exit. Kept off SandboxInstance to
	// avoid leaking a linux-only type into the cross-platform types.go.
	processes map[SandboxID]*libcontainer.Process
	// stdLogs retains the line-splitting writers and the per-agent log
	// file wrapping each running sandbox's stdout/stderr so Wait can
	// flush tail buffers and close the file when the init process exits.
	// Same linux-only-leak rationale as `processes`.
	stdLogs map[SandboxID]sandboxLogs
}

// sandboxLogs bundles the two line-splitting writers wrapping a running
// sandbox's stdio with the shared per-agent file they tee into. Both
// writers prefix their lines with `[stdout]` / `[stderr]` so a single
// log file is enough.
type sandboxLogs struct {
	stdout *sandboxLineWriter
	stderr *sandboxLineWriter
	file   *os.File
}

// NewSandboxManager creates a new SandboxManager.
func NewSandboxManager(stateDir, rootDir, logsDir string, imageStore *ImageStore, dialer netstack.Dialer) *SandboxManager {
	return &SandboxManager{
		stateDir:        stateDir,
		rootDir:         rootDir,
		logsDir:         logsDir,
		imageStore:      imageStore,
		dialer:          dialer,
		workerResolvers: readWorkerResolvers(),
		sandboxes:       make(map[SandboxID]*SandboxInstance),
		containers:      make(map[SandboxID]*libcontainer.Container),
		processes:       make(map[SandboxID]*libcontainer.Process),
		stdLogs:         make(map[SandboxID]sandboxLogs),
	}
}

// Create pulls the image, sets up the network namespace and TAP device,
// and creates a libcontainer container. The container is created but NOT
// started, leaving it in the Ready phase for warm pool use.
//
// agentRef, identity, and resources come from the parent agent (TaskAgent
// or DaemonAgent), since AgentSandboxRevision (the watched resource) only
// carries the immutable image+command snapshot.
//
// When stdio is true the returned instance carries os.Pipe-backed
// Stdin/Stdout/Stderr that Start will splice into the libcontainer
// Process. The dispatcher uses this to feed an HTTP request body in
// and stream the response body out; daemons leave stdio false and
// keep Process.Stdin = nil.
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
) (*SandboxInstance, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("sandboxID", id)

	m.mu.Lock()
	if _, exists := m.sandboxes[id]; exists {
		m.mu.Unlock()
		return nil, ErrAlreadyExists
	}
	m.mu.Unlock()

	log.Info("Creating sandbox")

	// 1. Pull image + extract rootfs.
	imgInfo, err := m.imageStore.EnsureImage(ctx, sandbox.Image)
	if err != nil {
		return nil, fmt.Errorf("ensuring image: %w", err)
	}

	// Use the shared image rootfs directly. Multiple sandboxes share the same
	// extracted image since the rootfs is mounted read-only by libcontainer.
	sandboxRootFS := imgInfo.RootFS

	// 3. Setup per-run netns + TAP.
	nsCfg, err := SetupNetNS(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("setting up netns: %w", err)
	}

	// 4. Create per-sandbox netstack.
	stack, err := netstack.NewSandboxStack(nsCfg.TAPFD, nsCfg.GW)
	if err != nil {
		TeardownNetNS(nsCfg)
		return nil, fmt.Errorf("creating sandbox netstack: %w", err)
	}

	// 5. Build libcontainer config.
	cfg := baseConfig(string(id), sandboxRootFS, resources)

	// 5a. Stage the agent's MITM CA and bind-mount it over every well-known
	// system trust path that exists in the sandbox rootfs. The rootfs is
	// read-only, so we can only overlay files that already exist — hence
	// the env-var fallback in Start.
	if len(caPEM) > 0 {
		caPath, err := m.writeAgentCA(id, caPEM)
		if err != nil {
			stack.Close()
			TeardownNetNS(nsCfg)
			return nil, fmt.Errorf("staging agent CA: %w", err)
		}
		cfg.Mounts = append(cfg.Mounts, buildTrustMounts(sandboxRootFS, caPath)...)
	}

	// 5b. Stage a per-sandbox /etc/resolv.conf pointing at the netns
	// gateway IP, so DNS queries route via the TAP device into the
	// gVisor netstack. The IdentityDialer rewrites :53 dials back to
	// the worker's actual resolver — see dns.go for the rationale.
	resolvPath, err := m.writeSandboxResolvConf(id, nsCfg.GW)
	if err != nil {
		m.removeAgentCA(id)
		stack.Close()
		TeardownNetNS(nsCfg)
		return nil, fmt.Errorf("staging sandbox resolv.conf: %w", err)
	}
	cfg.Mounts = append(cfg.Mounts, buildResolvMount(resolvPath))

	// 5c. Persistent agent state — bind-mount /var/lib/clrk/state/<ns>/<agent>/
	// into the sandbox at AgentState.MountPath. Pre-Create du-check
	// against sizeLimitMB; over-cap returns ErrStateOverLimit which
	// the dispatcher maps to 507. Per-(ns,agent) directory is shared
	// across all executions for the same TaskAgent on this worker.
	if state != nil {
		hostPath, err := ensureStateDir(identity.Namespace, agentRef, state.SizeLimitMB)
		if err != nil {
			m.removeSandboxNetConfig(id)
			m.removeAgentCA(id)
			stack.Close()
			TeardownNetNS(nsCfg)
			return nil, err
		}
		cfg.Mounts = append(cfg.Mounts, buildStateMount(hostPath, state))
	}

	// 6. Create container (does NOT start it).
	ctr, err := libcontainer.Create(m.stateDir, string(id), cfg)
	if err != nil {
		m.removeSandboxNetConfig(id)
		m.removeAgentCA(id)
		stack.Close()
		TeardownNetNS(nsCfg)
		return nil, fmt.Errorf("creating container: %w", err)
	}

	// 7. Track instance.
	sb := &SandboxInstance{
		ID:        id,
		AgentRef:  agentRef,
		Phase:     SandboxReady,
		RootFS:    sandboxRootFS,
		NetNS:     nsCfg.NSPath,
		TAPName:   nsCfg.TAPName,
		TAPFD:     nsCfg.TAPFD,
		Stack:     stack,
		Sandbox:   sandbox,
		Resources: resources,
		Identity:  identity,
		CreatedAt: time.Now(),
	}

	// 7a. Wire stdio pipes when requested. Caller writes to Stdin and
	// reads from Stdout/Stderr; the libcontainer Process gets the other
	// ends in Start. Failure here unwinds the same state Create owns.
	if stdio {
		inR, inW, err := os.Pipe()
		if err != nil {
			m.removeSandboxNetConfig(id)
			m.removeAgentCA(id)
			stack.Close()
			TeardownNetNS(nsCfg)
			return nil, fmt.Errorf("creating stdin pipe: %w", err)
		}
		outR, outW, err := os.Pipe()
		if err != nil {
			inR.Close()
			inW.Close()
			m.removeSandboxNetConfig(id)
			m.removeAgentCA(id)
			stack.Close()
			TeardownNetNS(nsCfg)
			return nil, fmt.Errorf("creating stdout pipe: %w", err)
		}
		errR, errW, err := os.Pipe()
		if err != nil {
			inR.Close()
			inW.Close()
			outR.Close()
			outW.Close()
			m.removeSandboxNetConfig(id)
			m.removeAgentCA(id)
			stack.Close()
			TeardownNetNS(nsCfg)
			return nil, fmt.Errorf("creating stderr pipe: %w", err)
		}
		sb.Stdin = inW
		sb.Stdout = outR
		sb.Stderr = errR
		sb.stdinChild = inR
		sb.stdoutChild = outW
		sb.stderrChild = errW
	}

	m.mu.Lock()
	m.sandboxes[id] = sb
	m.containers[id] = ctr
	m.mu.Unlock()

	log.Info("Sandbox created")
	return sb, nil
}

// Start starts the container's init process.
func (m *SandboxManager) Start(ctx context.Context, id SandboxID) error {
	log := ctrl.LoggerFrom(ctx).WithValues("sandboxID", id)

	m.mu.Lock()
	sb, ok := m.sandboxes[id]
	ctr := m.containers[id]
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}
	if ctr == nil {
		return fmt.Errorf("no container handle for %s (Create not called on this manager)", id)
	}

	// Build process args from spec, falling back to image entrypoint.
	// Copy the command slice to avoid aliasing the spec's backing array.
	var args []string
	if len(sb.Sandbox.Command) > 0 {
		args = append([]string(nil), sb.Sandbox.Command...)
	} else {
		imgInfo, err := m.imageStore.EnsureImage(ctx, sb.Sandbox.Image)
		if err != nil {
			return fmt.Errorf("getting image info: %w", err)
		}
		args = append([]string(nil), imgInfo.Entrypoint...)
	}
	args = append(args, sb.Sandbox.Args...)

	// Prepend MITM trust env vars so that user-supplied env can override
	// them if an agent explicitly wants a different trust store.
	env := append([]string(nil), trustEnv("/etc/ssl/certs/ca-certificates.crt")...)
	// IMDS metadata URLs are constants — same address for every
	// sandbox (link-local IMDS convention). Set unconditionally
	// regardless of delivery.mode: harmless for Stdin agents, and
	// agents that don't care can ignore it.
	env = append(env,
		fmt.Sprintf("CLRK_METADATA_URL=http://%s/v1", ports.MetadataAddrV4),
		fmt.Sprintf("CLRK_METADATA_URL_V6=http://[%s]/v1", ports.MetadataAddrV6),
	)
	env = append(env, envVarsToStrings(sb.Sandbox.Env)...)
	// Ensure PATH is set.
	hasPath := false
	for _, e := range env {
		if len(e) >= 5 && e[:5] == "PATH=" {
			hasPath = true
			break
		}
	}
	if !hasPath {
		env = append(env, "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	}

	// Wrap the sandbox's stdio in line-splitting slog writers so each
	// line of agent output becomes a structured record attributed to the
	// owning agent.
	sbLogger := slog.With(
		slog.String("sandbox.id", string(id)),
		slog.String("agent.namespace", sb.Identity.Namespace),
		slog.String("agent.name", sb.Identity.Name),
		slog.String("agent.uid", sb.Identity.UID),
		slog.String("agent.revision", sb.Identity.Revision),
	)
	logFile, err := openAgentLogFile(m.logsDir, sb.Identity.Namespace, sb.Identity.Name)
	if err != nil {
		// Tee-to-file is best-effort; agent stdio still flows to slog.
		log.Error(err, "Opening agent log file (continuing without file tee)")
	}
	stdoutLog := newSandboxLineWriter(sbLogger, slog.LevelInfo, "stdout", logFile)
	stderrLog := newSandboxLineWriter(sbLogger, slog.LevelWarn, "stderr", logFile)

	// When the sandbox was Created with stdio pipes, splice them into
	// libcontainer's Process: agent reads from the dispatcher via stdin,
	// and stdout/stderr fan out to both the dispatcher (for the response
	// body) and the per-agent log sink so structured logging keeps
	// working unchanged.
	var (
		procStdin  io.Reader = nil
		procStdout io.Writer = stdoutLog
		procStderr io.Writer = stderrLog
	)
	if sb.stdinChild != nil {
		procStdin = sb.stdinChild
	}
	if sb.stdoutChild != nil {
		procStdout = io.MultiWriter(sb.stdoutChild, stdoutLog)
	}
	if sb.stderrChild != nil {
		procStderr = io.MultiWriter(sb.stderrChild, stderrLog)
	}

	p := &libcontainer.Process{
		Args:            args,
		Env:             env,
		UID:             0,
		GID:             0,
		Cwd:             "/",
		NoNewPrivileges: ptr.To(true),
		Stdin:           procStdin,
		Stdout:          procStdout,
		Stderr:          procStderr,
		Init:            true,
	}

	log.Info("Starting sandbox", "args", args)

	// Start the per-sandbox netstack pump in a background goroutine.
	// It runs until the sandbox is deleted and its stack is closed.
	// Use a background context so the netstack outlives the caller's
	// request-scoped context (e.g. reconciler).
	if stack, ok := sb.Stack.(*netstack.SandboxStack); ok {
		dialer := netstack.Dialer(&egress.IdentityDialer{
			Base:         m.dialer,
			Identity:     sb.Identity,
			Backends:     sb.EgressBackends,
			DNSResolvers: m.workerResolvers,
			DNSCache:     stack.DNSCache(),
			Policy:       sb.EgressPolicy,
		})
		go func() {
			if err := stack.Start(context.Background(), dialer); err != nil {
				log.Error(err, "Netstack pump exited")
			}
		}()
	}

	if err := ctr.Run(p); err != nil {
		ctr.Destroy()
		return fmt.Errorf("running container: %w", err)
	}

	m.mu.Lock()
	sb.Phase = SandboxRunning
	m.processes[id] = p
	m.stdLogs[id] = sandboxLogs{stdout: stdoutLog, stderr: stderrLog, file: logFile}
	m.mu.Unlock()

	log.Info("Sandbox started")
	return nil
}

// SetEgressBackends configures the per-listener EG egress backends
// for a sandbox between Create and Start. Empty / nil disables PROXY
// v2 framing and direct-dials upstream.
func (m *SandboxManager) SetEgressBackends(id SandboxID, backends []egress.BackendListener) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb, ok := m.sandboxes[id]
	if !ok {
		return ErrNotFound
	}
	sb.EgressBackends = backends
	return nil
}

// SetEgressPolicy attaches the per-sandbox SandboxPolicy handle for a
// sandbox between Create and Start. The handle is consulted by the
// IdentityDialer before backend selection, so DefaultPolicy=DenyAll
// denies even traffic that would otherwise match a backend listener.
// Nil disables enforcement (used for agents with no EgressRefs).
func (m *SandboxManager) SetEgressPolicy(id SandboxID, policy *egress.SandboxPolicy) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb, ok := m.sandboxes[id]
	if !ok {
		return ErrNotFound
	}
	sb.EgressPolicy = policy
	return nil
}

// Wait blocks until the sandbox's init process exits. Returns ErrNotFound
// if the sandbox is unknown or was never Started. The caller is responsible
// for calling Delete after Wait returns.
func (m *SandboxManager) Wait(ctx context.Context, id SandboxID) (*os.ProcessState, error) {
	m.mu.Lock()
	p, ok := m.processes[id]
	m.mu.Unlock()
	if !ok {
		return nil, ErrNotFound
	}

	state, err := p.Wait()

	m.mu.Lock()
	sb, sbOK := m.sandboxes[id]
	if sbOK {
		sb.Phase = SandboxStopped
	}
	delete(m.processes, id)
	logs, hasLogs := m.stdLogs[id]
	delete(m.stdLogs, id)
	m.mu.Unlock()

	// Close the child ends of the stdio pipes now that the process has
	// exited. This makes the dispatcher's stdout reader see EOF and lets
	// it return cleanly without waiting for Delete.
	if sbOK {
		closeStdioChildren(sb)
	}

	// Emit any tail bytes the agent wrote without a trailing newline
	// before exit so the last line of output isn't dropped, then close
	// the per-agent tee file.
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

	return state, err
}

// Stop sends SIGTERM to all processes in the sandbox.
func (m *SandboxManager) Stop(ctx context.Context, id SandboxID) error {
	log := ctrl.LoggerFrom(ctx).WithValues("sandboxID", id)

	m.mu.Lock()
	sb, ok := m.sandboxes[id]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}
	m.mu.Unlock()

	ctr, err := libcontainer.Load(m.stateDir, string(id))
	if err == libcontainer.ErrNotExist {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("loading container: %w", err)
	}

	status, err := ctr.Status()
	if err != nil {
		return fmt.Errorf("getting container status: %w", err)
	}
	if status == libcontainer.Stopped {
		m.mu.Lock()
		sb.Phase = SandboxStopped
		m.mu.Unlock()
		return nil
	}

	ps, err := ctr.Processes()
	if err != nil {
		return fmt.Errorf("getting container processes: %w", err)
	}
	for _, pid := range ps {
		p, err := os.FindProcess(pid)
		if err != nil {
			return fmt.Errorf("finding process %d: %w", pid, err)
		}
		log.Info("Sending SIGTERM to process", "pid", pid)
		if err := p.Signal(syscall.SIGTERM); err != nil {
			log.Error(err, "Failed to send SIGTERM", "pid", pid)
		}
	}

	m.mu.Lock()
	sb.Phase = SandboxStopping
	m.mu.Unlock()

	return nil
}

// Delete destroys the container, tears down the network namespace, and
// removes the sandbox from tracking.
func (m *SandboxManager) Delete(ctx context.Context, id SandboxID) error {
	log := ctrl.LoggerFrom(ctx).WithValues("sandboxID", id)

	m.mu.Lock()
	sb, ok := m.sandboxes[id]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}
	m.mu.Unlock()

	// Close the netstack before destroying the container and netns.
	if sb.Stack != nil {
		if err := sb.Stack.Close(); err != nil {
			log.Error(err, "Failed to close netstack")
		}
	}

	ctr, err := libcontainer.Load(m.stateDir, string(id))
	if err != nil && err != libcontainer.ErrNotExist {
		return fmt.Errorf("loading container: %w", err)
	}
	if err == nil {
		if err := ctr.Destroy(); err != nil {
			log.Error(err, "Failed to destroy container")
		}
	}

	if err := TeardownNetNS(&NetNSConfig{
		NSName: fmt.Sprintf("run-%s", id),
		NSPath: sb.NetNS,
		TAPFD:  sb.TAPFD,
	}); err != nil {
		log.Error(err, "Failed to teardown netns")
	}

	m.removeAgentCA(id)
	m.removeSandboxNetConfig(id)

	// Close any stdio pipe ends still open. closeStdioChildren is safe
	// to call multiple times (each *os.File close is idempotent under
	// our usage); Wait may have closed the child ends already.
	closeStdioChildren(sb)
	closeStdioParents(sb)

	m.mu.Lock()
	delete(m.sandboxes, id)
	delete(m.containers, id)
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
	if !ok {
		m.mu.Unlock()
		return nil, ErrNotFound
	}
	m.mu.Unlock()

	// Refresh phase from libcontainer.
	ctr, err := libcontainer.Load(m.stateDir, string(id))
	if err == libcontainer.ErrNotExist {
		m.mu.Lock()
		sb.Phase = SandboxStopped
		m.mu.Unlock()
		return sb, nil
	}
	if err != nil {
		return nil, fmt.Errorf("loading container: %w", err)
	}

	cStatus, err := ctr.Status()
	if err != nil {
		return nil, fmt.Errorf("getting container status: %w", err)
	}

	m.mu.Lock()
	sb.Phase = phaseFromStatus(cStatus)
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

// Cleanup removes orphaned containers from a previous worker incarnation.
func (m *SandboxManager) Cleanup(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx)
	log.Info("Cleaning up orphaned containers")

	entries, err := os.ReadDir(m.stateDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("reading state dir: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		log.Info("Found orphaned container, destroying", "id", entry.Name())
		m.Purge(ctx, SandboxID(entry.Name()))
	}
	return nil
}

// Purge tears down any libcontainer + netns state left behind for id, even
// if the in-process SandboxManager has no record of it. Safe to call before
// Create as a guard against the "container with given ID already exists"
// error that surfaces when a previous Create attempt left partial state
// (libcontainer.Create writes the state directory before validating cgroup
// / namespace setup, so a failure midway through can leave a directory
// behind that Load won't touch but the next Create rejects). Fully best-
// effort: any errors here are logged, not returned, since we're about to
// try a fresh Create anyway.
func (m *SandboxManager) Purge(ctx context.Context, id SandboxID) {
	log := ctrl.LoggerFrom(ctx).WithValues("sandboxID", id)

	m.mu.Lock()
	delete(m.containers, id)
	m.mu.Unlock()

	stateEntry := filepath.Join(m.stateDir, string(id))
	if _, err := os.Stat(stateEntry); err != nil {
		// Nothing to clean up — common path on the first attempt.
		return
	}

	if ctr, err := libcontainer.Load(m.stateDir, string(id)); err == nil {
		if derr := ctr.Destroy(); derr != nil {
			log.Error(derr, "Destroy of orphaned container failed; falling back to RemoveAll")
		}
	} else if err != libcontainer.ErrNotExist {
		log.Error(err, "Load of orphaned container failed; falling back to RemoveAll")
	}

	// Even on a successful Destroy, libcontainer occasionally leaves the
	// dir behind on some kernels — RemoveAll unconditionally so the next
	// Create gets a clean slate.
	if err := os.RemoveAll(stateEntry); err != nil {
		log.Error(err, "RemoveAll of state dir failed")
	}

	TeardownNetNS(&NetNSConfig{NSName: fmt.Sprintf("run-%s", id)})
}

// closeStdioChildren closes the libcontainer-Process-facing pipe ends.
// Called after Wait returns (so the parent reader sees EOF) and again
// from Delete defensively. Each *os.File.Close is safe to call twice;
// the second returns a "file already closed" error we discard.
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

// closeStdioParents closes the dispatcher-facing pipe ends. Called
// from Delete; the dispatcher itself typically closes Stdin once it's
// done writing the request body, so this is a defensive sweep.
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

// phaseFromStatus maps libcontainer.Status to SandboxPhase.
func phaseFromStatus(status libcontainer.Status) SandboxPhase {
	switch status {
	case libcontainer.Running:
		return SandboxRunning
	case libcontainer.Stopped:
		return SandboxStopped
	case libcontainer.Paused:
		return SandboxReady
	default:
		return SandboxStopped
	}
}

// baseConfig creates the base container configuration, ported and adapted
// from apoxy-cli pkg/edgefunc/runc/runc.go:40-197.
func baseConfig(id, rootFS string, resources clrkv1alpha1.ExecutionResources) *configs.Config {
	devs := make([]*devices.Rule, len(specconv.AllowedDevices))
	for i, d := range specconv.AllowedDevices {
		devs[i] = &d.Rule
	}

	caps := []string{"CAP_NET_BIND_SERVICE"}

	c := &configs.Config{
		Rootfs:     rootFS,
		Readonlyfs: true,
		Capabilities: &configs.Capabilities{
			Bounding:  caps,
			Effective: caps,
			Permitted: caps,
			Ambient:   caps,
		},
		Namespaces: configs.Namespaces([]configs.Namespace{
			{Type: configs.NEWNS},
			{Type: configs.NEWUTS},
			{Type: configs.NEWIPC},
			{Type: configs.NEWPID},
			{Type: configs.NEWNET, Path: fmt.Sprintf("/run/netns/run-%s", id)},
			{Type: configs.NEWCGROUP},
		}),
		Devices:  specconv.AllowedDevices,
		Hostname: id,
		MaskPaths: []string{
			"/proc/kcore",
			"/sys/firmware",
		},
		ReadonlyPaths: []string{
			"/proc/sys", "/proc/sysrq-trigger", "/proc/irq", "/proc/bus",
		},
		NoNewKeyring: true,
		Networks: []*configs.Network{
			{
				Type:    "loopback",
				Address: "127.0.0.1/0",
				Gateway: "localhost",
			},
		},
		Cgroups: &configs.Cgroup{
			Name:   id,
			Parent: "system",
			Resources: &configs.Resources{
				MemorySwappiness: nil,
				Devices:          devs,
			},
		},
		Mounts: []*configs.Mount{
			{
				Source:      "proc",
				Destination: "/proc",
				Device:      "proc",
				Flags:       syscall.MS_NOEXEC | syscall.MS_NOSUID | syscall.MS_NODEV,
			},
			{
				Source:      "tmpfs",
				Destination: "/dev",
				Device:      "tmpfs",
				Flags:       syscall.MS_NOSUID | syscall.MS_STRICTATIME,
				Data:        "mode=755",
			},
			{
				Source:      "sysfs",
				Destination: "/sys",
				Device:      "sysfs",
				Flags:       syscall.MS_RDONLY | syscall.MS_NOEXEC | syscall.MS_NOSUID | syscall.MS_NODEV,
			},
			{
				Source:      "cgroup",
				Destination: "/sys/fs/cgroup",
				Device:      "cgroup",
				Flags:       syscall.MS_NOEXEC | syscall.MS_NOSUID | syscall.MS_NODEV | syscall.MS_RELATIME | syscall.MS_RDONLY,
			},
			{
				Source:      "devpts",
				Destination: "/dev/pts",
				Device:      "devpts",
				Flags:       syscall.MS_NOSUID | syscall.MS_NOEXEC,
				Data:        "newinstance,ptmxmode=0666,mode=0620,gid=5",
			},
			{
				Source:      "tmpfs",
				Destination: "/tmp",
				Device:      "tmpfs",
				Flags:       syscall.MS_NOSUID | syscall.MS_STRICTATIME,
				Data:        "mode=1777,size=100M",
			},
		},
		Rlimits: []configs.Rlimit{
			{
				Type: unix.RLIMIT_NOFILE,
				Hard: 1024,
				Soft: 1024,
			},
		},
	}

	// Apply resource limits from the parent agent's spec.
	if !resources.Memory.IsZero() {
		c.Cgroups.Resources.Memory = resources.Memory.Value()
	}
	if !resources.CPU.IsZero() {
		// Convert CPU quantity (e.g. "1" = 1 core, "500m" = 0.5 core) to CFS quota.
		// quota = millicores * period / 1000
		millis := resources.CPU.MilliValue()
		c.Cgroups.Resources.CpuQuota = millis * 100000 / 1000
		c.Cgroups.Resources.CpuPeriod = 100000
	}

	return c
}

// envVarsToStrings converts corev1.EnvVar slice to "KEY=VALUE" strings.
func envVarsToStrings(envVars []corev1.EnvVar) []string {
	result := make([]string, 0, len(envVars))
	for _, ev := range envVars {
		result = append(result, fmt.Sprintf("%s=%s", ev.Name, ev.Value))
	}
	return result
}
