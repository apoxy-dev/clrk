package cmd

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"runtime"
	"strings"

	"github.com/apoxy-dev/clrk/internal/drivers"
)

// preflight runs a fast battery of host-readiness checks before
// `clrk dev` starts pulling images or creating containers. Each check
// returns an error with a one-line actionable hint; preflight collects
// all of them, joins with newlines, and returns to runDev for a clean
// fail-fast.
//
// The checks are intentionally narrow — they cover the failure modes
// that produce confusing docker / runtime errors deep in bringUp on a
// fresh box (no docker, no /dev/net/tun, IPv6 disabled, can't reach
// the published OCI tag). They're deliberately not exhaustive: the
// real failure surface is wider, but these four catch the bulk of
// "ran clrk dev on a new VM, nothing happens" reports.
func preflight(ctx context.Context, o *devOpts) error {
	var errs []string
	for _, check := range []func(context.Context, *devOpts) error{
		checkDocker,
		checkLinuxKernel,
		checkImagePullable,
	} {
		if err := check(ctx, o); err != nil {
			errs = append(errs, "  • "+err.Error())
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.New("preflight failed:\n" + strings.Join(errs, "\n") +
		"\n\nFix the issues above (or pass --skip-preflight to bypass).")
}

// checkDocker fails fast when the daemon isn't reachable or the user
// can't talk to its socket. Distinguishing the two would require
// parsing dockerd error strings, so we report both possibilities in
// one hint.
func checkDocker(ctx context.Context, _ *devOpts) error {
	out, err := exec.CommandContext(ctx, "docker", "version", "--format", "{{.Server.Version}}").CombinedOutput()
	if err != nil {
		hint := "install docker and start the daemon"
		if runtime.GOOS == "linux" {
			hint += " (and add yourself to the `docker` group: `sudo usermod -aG docker $USER && newgrp docker`)"
		}
		return fmt.Errorf("docker daemon unreachable: %s — %s", strings.TrimSpace(string(out)), hint)
	}
	return nil
}

// checkLinuxKernel covers the two host-kernel features that bite
// hardest on minimal cloud images: a usable /dev/net/tun (the worker
// creates per-sandbox TAP devices for its netstack) and an
// IPv6-capable docker bridge (EnsureNetwork creates the `clrk`
// network with --ipv6 and refuses to silently fall back to v4-only).
//
// Skipped on darwin (clrk dev runs in linux containers anyway, and
// /dev/net/tun lives inside the docker VM there, not on the host).
func checkLinuxKernel(ctx context.Context, _ *devOpts) error {
	if runtime.GOOS != "linux" {
		return nil
	}
	if out, err := exec.CommandContext(ctx, "test", "-c", "/dev/net/tun").CombinedOutput(); err != nil {
		return fmt.Errorf("/dev/net/tun missing on host: %s — load the tun module (`sudo modprobe tun`) "+
			"and ensure it's in /etc/modules-load.d so it survives reboot", strings.TrimSpace(string(out)))
	}
	if out, err := exec.CommandContext(ctx, "test", "-d", "/proc/sys/net/ipv6").CombinedOutput(); err != nil {
		return fmt.Errorf("IPv6 disabled in kernel: %s — `clrk dev` needs an IPv6-capable docker bridge "+
			"for Envoy DFP to reach AAAA-only upstreams. Re-enable IPv6 (drop `ipv6.disable=1` from the kernel cmdline)",
			strings.TrimSpace(string(out)))
	}
	return nil
}

// checkImagePullable runs a `docker manifest inspect` against the
// resolved controller-manager + worker images. This is a metadata-
// only call (no layer download) that surfaces "manifest unknown" and
// "unauthorized" before bringUp tries the same pull as part of
// `docker run`. Catches the most common case after my last commit:
// the published image tag for this clrk SHA hasn't landed because
// the apoxy-cloud publish workflow failed or hasn't run yet.
//
// `--registry-image=<component>=<ref>` sets the image to a
// clrk-registry:5000/... ref that's unreachable from the host
// (the registry is on the docker bridge), so the manifest-inspect
// path skips those.
func checkImagePullable(ctx context.Context, o *devOpts) error {
	var errs []string
	for _, ref := range []string{o.controllerImage, o.workerImage} {
		if isLocalOnly(ref) {
			continue
		}
		out, err := exec.CommandContext(ctx, "docker", "manifest", "inspect", ref).CombinedOutput()
		if err != nil {
			errs = append(errs, fmt.Sprintf("cannot pull %s: %s", ref, strings.TrimSpace(string(out))))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	hint := fmt.Sprintf("either pin to a published tag with --controller-image / --worker-image, "+
		"or push a locally-built image to the dev registry and pass "+
		"`--registry-image=<component>=clrk-registry:5000/...`. "+
		"Image tags follow the clrk SHA; check the apoxy-cloud clrk.yml workflow run for SHA %s.",
		drivers.ImageTag())
	return errors.New(strings.Join(errs, "; ") + " — " + hint)
}

// isLocalOnly reports whether ref is a dev-local image tag (in-network
// hostname, no public registry path) that won't have a remote manifest.
// Refs of the form `clrk-registry:5000/...` resolve to the registry
// container ClusterDriver brings up on the shared docker bridge, so
// neither `docker manifest inspect` from the host nor a `docker pull`
// before the dev session is up will reach them.
func isLocalOnly(ref string) bool {
	return strings.HasPrefix(ref, "clrk-registry:5000/")
}
