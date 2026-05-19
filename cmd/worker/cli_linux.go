//go:build linux

package main

import (
	"fmt"
	"os"

	// Side-effect import: registers the clrk PluginStack and AF_INET /
	// AF_INET6 socket providers in this process. Required in BOTH the
	// worker process (for PreInit) and any re-exec'd Sentry boot child
	// (for Init), so the blank-import lives next to the runsc dispatch.
	_ "github.com/apoxy-dev/clrk/internal/sentrystack"
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

// tryDispatchRunsc fails loud if a runsc subcommand is invoked and
// returns silently otherwise so the worker boots its controller-runtime
// manager. Runsc dispatch isn't wired yet — the worker still spawns
// sandboxes via libcontainer — so nothing should be re-execing us as
// runsc; if it does, we want a clear error rather than a confusing
// fall-through into the controller-runtime path.
func tryDispatchRunsc() {
	if len(os.Args) < 2 {
		return
	}
	if _, ok := runscSubcommands[os.Args[1]]; !ok {
		return
	}
	fmt.Fprintf(os.Stderr, "worker: runsc subcommand %q invoked but runsc dispatch is not enabled\n", os.Args[1])
	os.Exit(1)
}
