//go:build linux

package sandbox

import (
	"net/netip"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// envVarsToStrings is the corev1.EnvVar -> "KEY=VALUE" de-contamination seam:
// the core carries process env as plain strings, never Kubernetes types.
func TestEnvVarsToStrings(t *testing.T) {
	in := []corev1.EnvVar{
		{Name: "A", Value: "1"},
		{Name: "B", Value: "x=y"}, // a value containing '=' must survive
		{Name: "EMPTY", Value: ""},
	}
	want := []string{"A=1", "B=x=y", "EMPTY="}
	if got := envVarsToStrings(in); !reflect.DeepEqual(got, want) {
		t.Fatalf("envVarsToStrings = %v; want %v", got, want)
	}
	if n := len(envVarsToStrings(nil)); n != 0 {
		t.Errorf("envVarsToStrings(nil) = %d entries; want 0", n)
	}
}

// buildProcessEnv merges trust + metadata defaults with the user env and
// guarantees exactly one PATH (a default only when the user supplies none).
func TestBuildProcessEnv(t *testing.T) {
	t.Run("user_env_and_defaults", func(t *testing.T) {
		env := buildProcessEnv([]corev1.EnvVar{{Name: "CARVE", Value: "ok"}})
		if !contains(env, "CARVE=ok") {
			t.Errorf("user env var missing from %v", env)
		}
		if !hasAnyPrefix(env, "CLRK_METADATA_URL=") {
			t.Errorf("CLRK_METADATA_URL not injected: %v", env)
		}
		if c := countPrefix(env, "PATH="); c != 1 {
			t.Errorf("want exactly one PATH default when user supplies none; got %d", c)
		}
	})

	t.Run("user_path_not_duplicated", func(t *testing.T) {
		env := buildProcessEnv([]corev1.EnvVar{{Name: "PATH", Value: "/custom/bin"}})
		if c := countPrefix(env, "PATH="); c != 1 {
			t.Errorf("user PATH must not be duplicated by the default; got %d PATH entries in %v", c, env)
		}
		if !contains(env, "PATH=/custom/bin") {
			t.Errorf("user PATH value lost: %v", env)
		}
	})
}

// resolverStrings stringifies the worker resolver list for the init payload,
// returning nil (not an empty slice) when there are none.
func TestResolverStrings(t *testing.T) {
	if got := resolverStrings(nil); got != nil {
		t.Errorf("resolverStrings(nil) = %v; want nil", got)
	}
	got := resolverStrings([]netip.AddrPort{
		netip.MustParseAddrPort("1.1.1.1:53"),
		netip.MustParseAddrPort("[2606:4700:4700::1111]:53"),
	})
	want := []string{"1.1.1.1:53", "[2606:4700:4700::1111]:53"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("resolverStrings = %v; want %v", got, want)
	}
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func hasAnyPrefix(ss []string, prefix string) bool {
	for _, x := range ss {
		if strings.HasPrefix(x, prefix) {
			return true
		}
	}
	return false
}

func countPrefix(ss []string, prefix string) int {
	n := 0
	for _, x := range ss {
		if strings.HasPrefix(x, prefix) {
			n++
		}
	}
	return n
}
