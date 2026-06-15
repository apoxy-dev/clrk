//go:build linux

package sandbox

import (
	"os"
	"path/filepath"
	"testing"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// isSafeStateMountPath mirrors admission's validateAgentState: an absolute,
// clean, non-"/" path that doesn't shadow any runtime-reserved mount.
func TestIsSafeStateMountPath(t *testing.T) {
	cases := []struct {
		p    string
		want bool
	}{
		{"/var/lib/agent", true},
		{"/data", true},
		{"/srv/state", true},
		{"/var", true}, // /var itself is not reserved
		{"/", false},
		{"relative/path", false},
		{"/a/../b", false}, // not Clean
		{"/etc", false},    // reserved
		{"/etc/foo", false},
		{"/proc", false},
		{"/tmp", false},
		{"/usr/local", false}, // under reserved /usr
		{"/etc/ssl/certs/ca-certificates.crt", false}, // trust path
	}
	for _, tc := range cases {
		t.Run(tc.p, func(t *testing.T) {
			if got := isSafeStateMountPath(tc.p); got != tc.want {
				t.Fatalf("isSafeStateMountPath(%q) = %v; want %v", tc.p, got, tc.want)
			}
		})
	}
}

// mountPathConflicts treats a path as conflicting with a reserved path when
// they're equal or one contains the other at a separator boundary (no false
// positive on a shared string prefix like /etcfoo vs /etc).
func TestMountPathConflicts(t *testing.T) {
	cases := []struct {
		candidate, reserved string
		want                bool
	}{
		{"/etc", "/etc", true},
		{"/etc/ssl", "/etc", true},  // candidate under reserved
		{"/etc", "/etc/ssl", true},  // reserved under candidate
		{"/foo/bar", "/foo", true},  // nested
		{"/etcfoo", "/etc", false},  // shared prefix, not a path boundary
		{"/var", "/etc", false},     // disjoint
	}
	for _, tc := range cases {
		if got := mountPathConflicts(tc.candidate, tc.reserved); got != tc.want {
			t.Errorf("mountPathConflicts(%q, %q) = %v; want %v", tc.candidate, tc.reserved, got, tc.want)
		}
	}
}

// buildStateMount returns the default guest mount for a nil/unset state, honors
// a safe MountPath override, and falls back to the default for an unsafe one.
func TestBuildStateMount(t *testing.T) {
	const hostPath = "/host/state"

	t.Run("nil_state_uses_default", func(t *testing.T) {
		m := buildStateMount(hostPath, nil)
		if m.Destination != defaultStateGuestMount {
			t.Errorf("Destination = %q; want %q", m.Destination, defaultStateGuestMount)
		}
		if m.Source != hostPath || m.Type != "bind" {
			t.Errorf("mount = %+v; want src=%q type=bind", m, hostPath)
		}
	})
	t.Run("safe_override_honored", func(t *testing.T) {
		m := buildStateMount(hostPath, &clrkv1alpha1.AgentState{MountPath: "/data/agent"})
		if m.Destination != "/data/agent" {
			t.Errorf("safe MountPath ignored: Destination = %q", m.Destination)
		}
	})
	t.Run("unsafe_override_falls_back", func(t *testing.T) {
		m := buildStateMount(hostPath, &clrkv1alpha1.AgentState{MountPath: "/etc"})
		if m.Destination != defaultStateGuestMount {
			t.Errorf("unsafe MountPath must fall back to default; Destination = %q", m.Destination)
		}
	})
}

// buildTrustMounts binds the agent CA over every well-known trust path that
// exists as a *regular* file in the rootfs; symlinked and missing paths are
// skipped (a double bind over the same inode breaks gVisor's gofer).
func TestBuildTrustMounts(t *testing.T) {
	rootfs := t.TempDir()

	// A regular trust file at the canonical Debian path.
	regular := "/etc/ssl/certs/ca-certificates.crt"
	mkRootfsFile(t, rootfs, regular)

	// A symlink at the Alpine path -> the canonical file (must be skipped).
	symlinkPath := "/etc/ssl/cert.pem"
	if err := os.Symlink("certs/ca-certificates.crt", filepath.Join(rootfs, symlinkPath)); err != nil {
		t.Fatalf("creating symlink: %v", err)
	}

	got := buildTrustMounts(rootfs, "/host/ca.crt")

	dests := map[string]bool{}
	for _, m := range got {
		dests[m.Destination] = true
		if m.Source != "/host/ca.crt" {
			t.Errorf("trust mount %q Source = %q; want /host/ca.crt", m.Destination, m.Source)
		}
	}
	if !dests[regular] {
		t.Errorf("regular trust file %q should be bind-mounted; got %v", regular, dests)
	}
	if dests[symlinkPath] {
		t.Errorf("symlinked trust path %q must be skipped", symlinkPath)
	}
	if dests["/etc/pki/tls/certs/ca-bundle.crt"] {
		t.Errorf("missing trust path must not be mounted")
	}
}

func mkRootfsFile(t *testing.T, rootfs, guestPath string) {
	t.Helper()
	full := filepath.Join(rootfs, guestPath)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("mkdir for %q: %v", guestPath, err)
	}
	if err := os.WriteFile(full, []byte("x"), 0o644); err != nil {
		t.Fatalf("write %q: %v", guestPath, err)
	}
}
