//go:build linux

// Package reexec centralizes the gVisor runsc subcommand dispatch that the
// worker binary performs on startup. The sandbox Manager drives runsc by
// re-exec'ing /proc/self/exe with a runsc argv (create/start/boot/gofer/...);
// every binary that can be that re-exec target — the worker itself AND any
// test binary that boots a real sandbox — must hand those invocations to
// gVisor instead of running its normal main. Keeping the dispatch (and its
// subcommand table) in one place avoids a second copy drifting out of sync
// with the pinned gVisor commit.
package reexec

import (
	"fmt"
	"os"

	"gvisor.dev/gvisor/runsc/cli/maincli"

	// Side-effect import: registers the clrk PluginStack and AF_INET /
	// AF_INET6 socket providers in this process. Required in BOTH the
	// worker process (for PreInit) and any re-exec'd Sentry boot child
	// (for Init), so the blank-import lives next to the runsc dispatch.
	"github.com/apoxy-dev/clrk/internal/sentrystack"
)

// runscSubcommands is the set of argv[1] tokens that mean "this process
// invocation is a runsc subcommand re-exec, not the controller-runtime
// worker." Mirror of runsc/cli/maincli/maincli.go's commands() map plus
// the built-in `help` / `flags` registered by cli.Run. Keep in sync when
// bumping the pinned gvisor commit.
var runscSubcommands = map[string]struct{}{
	"checkpoint":      {},
	"create":          {},
	"delete":          {},
	"events":          {},
	"exec":            {},
	"kill":            {},
	"list":            {},
	"ps":              {},
	"pause":           {},
	"restore":         {},
	"resume":          {},
	"run":             {},
	"spec":            {},
	"start":           {},
	"state":           {},
	"update":          {},
	"wait":            {},
	"do":              {},
	"fscheckpoint":    {},
	"port-forward":    {},
	"tar":             {},
	"install":         {},
	"mitigate":        {},
	"uninstall":       {},
	"nvproxy":         {},
	"trace":           {},
	"cpu-features":    {},
	"debug":           {},
	"statefile":       {},
	"symbolize":       {},
	"usage":           {},
	"read-control":    {},
	"write-control":   {},
	"metric-metadata": {},
	"metric-export":   {},
	"metric-server":   {},
	"boot":            {},
	"gofer":           {},
	"umount":          {},
	"help":            {},
	"flags":           {},
}

// TryDispatchRunsc hands off to gVisor's runsc entrypoint when the process
// was re-exec'd as a runsc subcommand. The worker-side helpers in
// internal/worker/sandbox/runsc_exec.go put runsc-level flags (e.g. --root,
// --network) BEFORE the subcommand, mirroring the runsc CLI; so scan past
// leading flag-shaped argv tokens before checking for a known subcommand.
// maincli.Main never returns (cli.Run terminates via os.Exit), so this
// function only returns when argv contained no recognized subcommand at all.
//
// Call this FIRST in main()/TestMain(), before any other setup: the re-exec'd
// invocation must reach gVisor, not the controller-runtime manager or the Go
// test runner.
func TryDispatchRunsc() {
	for _, a := range os.Args[1:] {
		if len(a) > 0 && a[0] == '-' {
			continue
		}
		if _, ok := runscSubcommands[a]; !ok {
			return
		}
		if sentrystack.Singleton() == nil {
			fmt.Fprintf(os.Stderr,
				"reexec: runsc subcommand %q invoked but sentrystack PluginStack not registered\n", a)
			os.Exit(1)
		}
		maincli.Main()
		return
	}
}
