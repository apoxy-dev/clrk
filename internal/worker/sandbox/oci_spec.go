//go:build linux

package sandbox

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	specs "github.com/opencontainers/runtime-spec/specs-go"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
)

const ociSpecVersion = "1.0.0"

// buildSpec returns the OCI runtime spec for a sandbox. Args and Env
// are baked in here because runsc's Start has no per-call override —
// the spec on disk is the final word.
//
// NetworkNamespace is explicitly pinned to the worker's netns. Under
// --network=plugin, runsc otherwise creates a fresh empty netns for the
// Sentry — see
// https://github.com/apoxy-dev/gvisor/blob/5d6cfb0c0960/runsc/sandbox/sandbox.go#L1045-L1053
// — which would strand the Sentry's forwarder dials (DNS upstream
// resolvers, IMDS bridge on 127.0.0.1, egress MITM bridge), all
// reached via Linux net.Dial from inside the Sentry process. Pinning
// to /proc/self/ns/net resolves (in runsc) to runsc's own netns,
// which is inherited from the worker; the Sentry then setns()es into
// that same netns and gains reachability to 127.0.0.1:<port>. The
// sandboxed application never touches the worker's netns — it only
// sees the in-Sentry PluginStack — so this doesn't widen the security
// perimeter.
func buildSpec(
	id, rootfs string,
	args, env []string,
	resources clrkv1alpha1.ExecutionResources,
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
				{Type: specs.CgroupNamespace},
				{Type: specs.NetworkNamespace, Path: "/proc/self/ns/net"},
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

	// Linux.Resources is cosmetic under --ignore-cgroups — runsc never
	// reads it. Real per-sandbox enforcement happens in the worker-
	// owned cgroup at <worker>/system/<id> (see createSandboxCgroup);
	// we still populate the OCI block so the on-disk config.json
	// matches what the kernel is actually enforcing, which keeps
	// `runsc state` / config dumps useful during post-mortem.
	if !resources.Memory.IsZero() {
		v := resources.Memory.Value()
		spec.Linux.Resources.Memory = &specs.LinuxMemory{Limit: &v}
	}
	if !resources.CPU.IsZero() {
		quota, period := cpuMaxFor(resources.CPU.MilliValue())
		p := uint64(period)
		spec.Linux.Resources.CPU = &specs.LinuxCPU{Quota: &quota, Period: &p}
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
// path that already exists as a regular file in the read-only rootfs.
// Missing targets are skipped — runsc rejects a mount whose destination
// doesn't exist, and the env-var fallback in buildProcessEnv covers
// programs that honor SSL_CERT_FILE.
//
// We Lstat instead of Stat so symlink targets like Wolfi's
// /etc/ssl/cert.pem → certs/ca-certificates.crt don't get bind-mounted
// twice over the same underlying file. Two bind mounts whose
// destinations resolve to the same inode confuse gVisor's gofer:
// the gofer's chroot-side opens both at the canonical path and reports
// them as duplicates, the Sentry's StartRoot pairs Sentry-side bind
// FDs to gofer-side serving FDs by spec order, and the mismatch causes
// `runsc start` to error out with urpc EOF on containerManager.StartRoot
// after a partially-mounted root. Mounting over the canonical target
// alone already covers any symlinked path that resolves to it.
func buildTrustMountsSpec(rootfs, caPath string) []specs.Mount {
	out := make([]specs.Mount, 0, len(wellKnownTrustPaths))
	for _, target := range wellKnownTrustPaths {
		info, err := os.Lstat(filepath.Join(rootfs, target))
		if err != nil {
			continue
		}
		if info.Mode()&os.ModeSymlink != 0 {
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
	// Admission (api/clrk/v1alpha1.validateAgentState) is the normative
	// gate. This re-check defends in-process callers (tests, future
	// programmatic construction) and any TaskAgent objects that landed
	// in etcd before admission validation existed: rather than fail the
	// sandbox, we fall back to the default mount and log a warning.
	if state != nil && state.MountPath != "" {
		if isSafeStateMountPath(state.MountPath) {
			dest = state.MountPath
		} else {
			slog.Warn("Rejecting unsafe AgentState.MountPath; falling back to default",
				slog.String("mountPath", state.MountPath),
				slog.String("fallback", defaultStateGuestMount))
		}
	}
	return specs.Mount{
		Destination: dest,
		Source:      hostPath,
		Type:        "bind",
		Options:     []string{"bind", "rec"},
	}
}

// isSafeStateMountPath mirrors api/clrk/v1alpha1.validateAgentState so
// the worker rejects the same shapes admission rejects, without
// importing the api package's reserved list (it's intentionally
// duplicated there because that package is cross-platform). The
// reserved set here is derived from this package's own sources of
// truth — defaultSpecMounts, buildResolvMountSpec, wellKnownTrustPaths
// — plus the rootfs binary/system-config dirs.
func isSafeStateMountPath(p string) bool {
	if !filepath.IsAbs(p) || p != filepath.Clean(p) || p == "/" {
		return false
	}
	for _, r := range reservedGuestMountPaths() {
		if mountPathConflicts(p, r) {
			return false
		}
	}
	return true
}

func reservedGuestMountPaths() []string {
	out := make([]string, 0, 16)
	for _, m := range defaultSpecMounts() {
		out = append(out, m.Destination)
	}
	out = append(out, "/etc/resolv.conf")
	out = append(out, wellKnownTrustPaths...)
	out = append(out,
		"/etc", "/usr", "/bin", "/sbin", "/lib", "/lib64", "/boot", "/root",
	)
	return out
}

func mountPathConflicts(candidate, reserved string) bool {
	if candidate == reserved {
		return true
	}
	sep := string(filepath.Separator)
	return strings.HasPrefix(candidate, reserved+sep) || strings.HasPrefix(reserved, candidate+sep)
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

func (m *Manager) runscBundleDir(id SandboxID) string {
	return filepath.Join(m.stateDir, string(id)+"-bundle")
}

func (m *Manager) ensureRunscBundleDir(id SandboxID) (string, error) {
	dir := m.runscBundleDir(id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating runsc bundle dir: %w", err)
	}
	return dir, nil
}

func (m *Manager) removeRunscBundleDir(id SandboxID) {
	_ = os.RemoveAll(m.runscBundleDir(id))
}
