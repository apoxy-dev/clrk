//go:build linux

package sandbox

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
	sandboxcore "github.com/apoxy-dev/clrk/pkg/sandbox"
)

// This file holds the clrk-specific mount + annotation builders the
// wrapper computes from a CreateRequest and hands to the core as neutral
// sandboxcore.Mount / annotation values (the four de-contamination seams:
// trust + persistent-state mounts, identity annotations). The OCI runtime
// spec, default mount set, and resolv.conf mount moved to the core.

// buildTrustMounts returns bind mounts for every well-known trust path
// that already exists as a regular file in the read-only rootfs. Missing
// targets are skipped — runsc rejects a mount whose destination doesn't
// exist, and the env-var fallback in buildProcessEnv covers programs that
// honor SSL_CERT_FILE.
//
// We Lstat instead of Stat so symlink targets like Wolfi's
// /etc/ssl/cert.pem -> certs/ca-certificates.crt don't get bind-mounted
// twice over the same underlying file. Two bind mounts whose destinations
// resolve to the same inode confuse gVisor's gofer: it reports them as
// duplicates and `runsc start` errors out with urpc EOF on
// containerManager.StartRoot after a partially-mounted root. Mounting over
// the canonical target alone already covers any symlinked path resolving to it.
func buildTrustMounts(rootfs, caPath string) []sandboxcore.Mount {
	out := make([]sandboxcore.Mount, 0, len(wellKnownTrustPaths))
	for _, target := range wellKnownTrustPaths {
		info, err := os.Lstat(filepath.Join(rootfs, target))
		if err != nil {
			continue
		}
		if info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		out = append(out, sandboxcore.Mount{
			Destination: target,
			Source:      caPath,
			Type:        "bind",
			Options:     []string{"bind", "ro", "rec"},
		})
	}
	return out
}

// buildStateMount returns the persistent-state bind mount, honoring a
// safe AgentState.MountPath override and falling back to the default guest
// mount otherwise.
func buildStateMount(hostPath string, state *clrkv1alpha1.AgentState) sandboxcore.Mount {
	dest := defaultStateGuestMount
	// Admission (api/clrk/v1alpha1.validateAgentState) is the normative
	// gate. This re-check defends in-process callers and any objects that
	// landed in etcd before admission validation existed: rather than fail
	// the sandbox, we fall back to the default mount and log a warning.
	if state != nil && state.MountPath != "" {
		if isSafeStateMountPath(state.MountPath) {
			dest = state.MountPath
		} else {
			slog.Warn("Rejecting unsafe AgentState.MountPath; falling back to default",
				slog.String("mountPath", state.MountPath),
				slog.String("fallback", defaultStateGuestMount))
		}
	}
	return sandboxcore.Mount{
		Destination: dest,
		Source:      hostPath,
		Type:        "bind",
		Options:     []string{"bind", "rec"},
	}
}

// isSafeStateMountPath mirrors api/clrk/v1alpha1.validateAgentState so the
// worker rejects the same shapes admission rejects, without importing the
// api package's reserved list (it's intentionally duplicated there because
// that package is cross-platform).
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

// reservedGuestMountPaths enumerates in-sandbox paths the runtime owns and
// that a persistent-state bind mount must not shadow. The default OCI
// mount destinations are hardcoded here (the core owns defaultSpecMounts;
// these six values are stable and also duplicated in
// api/clrk/v1alpha1.reservedMountPaths) alongside the resolv.conf mount,
// the trust CA bundle locations, and the rootfs binary/system-config dirs.
func reservedGuestMountPaths() []string {
	out := []string{
		// Core defaultSpecMounts destinations.
		"/proc", "/dev", "/sys", "/sys/fs/cgroup", "/dev/pts", "/tmp",
		// Core resolv.conf mount destination.
		"/etc/resolv.conf",
	}
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

// buildSandboxAnnotations projects the lineage labels into the OCI
// annotation map stamped onto the container config.
func buildSandboxAnnotations(identity proxyproto.AgentIdentity, podName string, attempt int32) map[string]string {
	return buildSandboxLabelMap(identity, podName, attempt)
}
