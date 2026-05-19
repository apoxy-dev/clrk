//go:build linux

package worker

import (
	"fmt"
	"os"
	"path/filepath"

	specs "github.com/opencontainers/runtime-spec/specs-go"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
)

const ociSpecVersion = "1.0.0"

// buildSpec returns the OCI runtime spec for a sandbox. Args and Env
// are baked in here because runsc's Start has no per-call override —
// the spec on disk is the final word.
func buildSpec(
	id, rootfs string,
	args, env []string,
	resources clrkv1alpha1.ExecutionResources,
	netnsPath string,
	mounts []specs.Mount,
	annotations map[string]string,
) *specs.Spec {
	caps := []string{"CAP_NET_BIND_SERVICE"}

	spec := &specs.Spec{
		Version: ociSpecVersion,
		Process: &specs.Process{
			User: specs.User{UID: 0, GID: 0},
			Args: args,
			Env:  env,
			Cwd:  "/",
			Capabilities: &specs.LinuxCapabilities{
				Bounding:  caps,
				Effective: caps,
				Permitted: caps,
				Ambient:   caps,
			},
			NoNewPrivileges: true,
			Rlimits: []specs.POSIXRlimit{
				{Type: "RLIMIT_NOFILE", Hard: 1024, Soft: 1024},
			},
		},
		Root: &specs.Root{
			Path:     rootfs,
			Readonly: true,
		},
		Hostname:    id,
		Mounts:      append(defaultSpecMounts(), mounts...),
		Annotations: annotations,
		Linux: &specs.Linux{
			Namespaces: []specs.LinuxNamespace{
				{Type: specs.MountNamespace},
				{Type: specs.UTSNamespace},
				{Type: specs.IPCNamespace},
				{Type: specs.PIDNamespace},
				{Type: specs.NetworkNamespace, Path: netnsPath},
				{Type: specs.CgroupNamespace},
			},
			MaskedPaths: []string{
				"/proc/kcore",
				"/sys/firmware",
			},
			ReadonlyPaths: []string{
				"/proc/sys", "/proc/sysrq-trigger", "/proc/irq", "/proc/bus",
			},
			Resources:   &specs.LinuxResources{},
			CgroupsPath: filepath.Join("/system", id),
		},
	}

	if !resources.Memory.IsZero() {
		v := resources.Memory.Value()
		spec.Linux.Resources.Memory = &specs.LinuxMemory{Limit: &v}
	}
	if !resources.CPU.IsZero() {
		// CFS quota: quota = millicores * period / 1000.
		millis := resources.CPU.MilliValue()
		quota := millis * 100000 / 1000
		var period uint64 = 100000
		spec.Linux.Resources.CPU = &specs.LinuxCPU{Quota: &quota, Period: &period}
	}

	return spec
}

func defaultSpecMounts() []specs.Mount {
	return []specs.Mount{
		{Destination: "/proc", Type: "proc", Source: "proc", Options: []string{"noexec", "nosuid", "nodev"}},
		{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755"}},
		{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"ro", "noexec", "nosuid", "nodev"}},
		{Destination: "/sys/fs/cgroup", Type: "cgroup", Source: "cgroup", Options: []string{"ro", "noexec", "nosuid", "nodev", "relatime"}},
		{Destination: "/dev/pts", Type: "devpts", Source: "devpts", Options: []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"}},
		{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=1777", "size=100M"}},
	}
}

// buildTrustMountsSpec returns bind mounts for every well-known trust
// path that already exists in the read-only rootfs. Missing targets
// are skipped — runsc rejects a mount whose destination doesn't exist,
// and the env-var fallback in buildProcessEnv covers programs that
// honor SSL_CERT_FILE.
func buildTrustMountsSpec(rootfs, caPath string) []specs.Mount {
	out := make([]specs.Mount, 0, len(wellKnownTrustPaths))
	for _, target := range wellKnownTrustPaths {
		if _, err := os.Stat(filepath.Join(rootfs, target)); err != nil {
			continue
		}
		out = append(out, specs.Mount{
			Destination: target,
			Source:      caPath,
			Type:        "bind",
			Options:     []string{"bind", "ro", "rec"},
		})
	}
	return out
}

func buildResolvMountSpec(source string) specs.Mount {
	return specs.Mount{
		Destination: "/etc/resolv.conf",
		Source:      source,
		Type:        "bind",
		Options:     []string{"bind", "ro"},
	}
}

func buildStateMountSpec(hostPath string, state *clrkv1alpha1.AgentState) specs.Mount {
	dest := defaultStateGuestMount
	if state != nil && state.MountPath != "" {
		dest = state.MountPath
	}
	return specs.Mount{
		Destination: dest,
		Source:      hostPath,
		Type:        "bind",
		Options:     []string{"bind", "rec"},
	}
}

func buildSandboxAnnotations(identity proxyproto.AgentIdentity, podName string, attempt int32) map[string]string {
	return buildSandboxLabelMap(identity, podName, attempt)
}

// resolveProcessArgs returns sb.Command (else image Entrypoint), with
// sb.Args appended. Baked into the OCI spec at Create time — runsc's
// Start has no per-call override.
func resolveProcessArgs(sb clrkv1alpha1.AgentSandbox, entrypoint []string) []string {
	var args []string
	if len(sb.Command) > 0 {
		args = append(args, sb.Command...)
	} else {
		args = append(args, entrypoint...)
	}
	return append(args, sb.Args...)
}

func (m *SandboxManager) runscBundleDir(id SandboxID) string {
	return filepath.Join(m.stateDir, string(id)+"-bundle")
}

func (m *SandboxManager) ensureRunscBundleDir(id SandboxID) (string, error) {
	dir := m.runscBundleDir(id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating runsc bundle dir: %w", err)
	}
	return dir, nil
}

func (m *SandboxManager) removeRunscBundleDir(id SandboxID) {
	_ = os.RemoveAll(m.runscBundleDir(id))
}
