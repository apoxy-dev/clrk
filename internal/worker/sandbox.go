//go:build linux

package worker

import (
	"context"
	"fmt"
	"os"
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

	// Enable cgroup manager to manage devices.
	_ "github.com/opencontainers/runc/libcontainer/cgroups/devices"
	// nsenter is required for libcontainer container init.
	_ "github.com/opencontainers/runc/libcontainer/nsenter"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

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
	imageStore *ImageStore

	mu        sync.Mutex
	sandboxes map[SandboxID]*SandboxInstance
}

// NewSandboxManager creates a new SandboxManager.
func NewSandboxManager(stateDir, rootDir string, imageStore *ImageStore) *SandboxManager {
	return &SandboxManager{
		stateDir:   stateDir,
		rootDir:    rootDir,
		imageStore: imageStore,
		sandboxes:  make(map[SandboxID]*SandboxInstance),
	}
}

// Create pulls the image, sets up the network namespace and TAP device,
// and creates a libcontainer container. The container is created but NOT
// started, leaving it in the Ready phase for warm pool use.
func (m *SandboxManager) Create(ctx context.Context, id SandboxID, spec clrkv1alpha1.SandboxStateSpec) (*SandboxInstance, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("sandboxID", id)

	m.mu.Lock()
	if _, exists := m.sandboxes[id]; exists {
		m.mu.Unlock()
		return nil, ErrAlreadyExists
	}
	m.mu.Unlock()

	log.Info("Creating sandbox")

	// 1. Pull image + extract rootfs.
	imgInfo, err := m.imageStore.EnsureImage(ctx, spec.Sandbox.Image)
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

	// 4. Build libcontainer config.
	cfg := baseConfig(string(id), sandboxRootFS, spec)

	// 5. Create container (does NOT start it).
	ctr, err := libcontainer.Create(m.stateDir, string(id), cfg)
	if err != nil {
		TeardownNetNS(nsCfg)
		return nil, fmt.Errorf("creating container: %w", err)
	}
	_ = ctr // Container is persisted in stateDir; we Load() it later for Start.

	// 6. Track instance.
	sb := &SandboxInstance{
		ID:        id,
		AgentRef:  spec.AgentRef,
		Phase:     SandboxReady,
		RootFS:    sandboxRootFS,
		NetNS:     nsCfg.NSPath,
		TAPName:   nsCfg.TAPName,
		TAPFD:     nsCfg.TAPFD,
		Sandbox:   spec.Sandbox,
		Resources: spec.Resources,
		CreatedAt: time.Now(),
	}

	m.mu.Lock()
	m.sandboxes[id] = sb
	m.mu.Unlock()

	log.Info("Sandbox created")
	return sb, nil
}

// Start starts the container's init process.
func (m *SandboxManager) Start(ctx context.Context, id SandboxID) error {
	log := ctrl.LoggerFrom(ctx).WithValues("sandboxID", id)

	m.mu.Lock()
	sb, ok := m.sandboxes[id]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}
	m.mu.Unlock()

	ctr, err := libcontainer.Load(m.stateDir, string(id))
	if err != nil {
		return fmt.Errorf("loading container: %w", err)
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

	env := envVarsToStrings(sb.Sandbox.Env)
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

	p := &libcontainer.Process{
		Args:            args,
		Env:             env,
		User:            "0:0",
		Cwd:             "/",
		NoNewPrivileges: ptr.To(true),
		Stdin:           nil,
		Stdout:          os.Stdout,
		Stderr:          os.Stderr,
		Init:            true,
	}

	log.Info("Starting sandbox", "args", args)

	if err := ctr.Run(p); err != nil {
		ctr.Destroy()
		return fmt.Errorf("running container: %w", err)
	}

	m.mu.Lock()
	sb.Phase = SandboxRunning
	m.mu.Unlock()

	log.Info("Sandbox started")
	return nil
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

	m.mu.Lock()
	delete(m.sandboxes, id)
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
		id := entry.Name()
		log.Info("Found orphaned container, destroying", "id", id)

		ctr, err := libcontainer.Load(m.stateDir, id)
		if err != nil {
			log.Error(err, "Failed to load orphaned container", "id", id)
			continue
		}
		if err := ctr.Destroy(); err != nil {
			log.Error(err, "Failed to destroy orphaned container", "id", id)
		}

		// Best-effort netns cleanup.
		nsName := fmt.Sprintf("run-%s", id)
		TeardownNetNS(&NetNSConfig{NSName: nsName})
	}

	return nil
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
func baseConfig(id, rootFS string, spec clrkv1alpha1.SandboxStateSpec) *configs.Config {
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

	// Apply resource limits from the CRD spec.
	if !spec.Resources.Memory.IsZero() {
		c.Cgroups.Resources.Memory = spec.Resources.Memory.Value()
	}
	if !spec.Resources.CPU.IsZero() {
		// Convert CPU quantity (e.g. "1" = 1 core, "500m" = 0.5 core) to CFS quota.
		// quota = millicores * period / 1000
		millis := spec.Resources.CPU.MilliValue()
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
