package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/apoxy-dev/clrk/internal/cmd/devtui"
	"github.com/apoxy-dev/clrk/internal/crds"
	"github.com/apoxy-dev/clrk/internal/drivers"
	"github.com/apoxy-dev/clrk/internal/install"
)

type installOpts struct {
	kubeconfig      string
	context         string
	namespace       string
	workerNamespace string
	controllerImage string
	workerImage     string
	version         string
	storageClass    string
	workers         int
	pull            string
	crdMode         string
	imagePullSecret string
	yes             bool
	skipPreflight   bool
	dryRun          bool
	tui             bool
	tuiSet          bool
	timeout         time.Duration
	readyInterval   time.Duration
}

func newInstallCmd() *cobra.Command {
	o := &installOpts{}
	cmd := &cobra.Command{
		Use:   "install",
		Short: "Install the clrk control plane into a Kubernetes cluster",
		Long: "Installs the clrk controller-manager (aggregated apiserver + embedded " +
			"kine/ClickHouse/NATS + Envoy Gateway), the Gateway-API + Envoy-Gateway " +
			"CRDs, and a default WorkerPool into an operator-supplied cluster. " +
			"Runs preflight checks and confirms risky actions before applying.",
		RunE: func(cmd *cobra.Command, args []string) error {
			o.tuiSet = cmd.Flags().Changed("tui")
			return runInstall(cmd.Context(), o)
		},
	}
	f := cmd.Flags()
	f.StringVar(&o.kubeconfig, "kubeconfig", "", "Path to the kubeconfig (defaults to $KUBECONFIG, then ~/.kube/config).")
	f.StringVar(&o.context, "context", "", "Kubeconfig context to target (defaults to the current-context).")
	f.StringVar(&o.namespace, "namespace", install.DefaultNamespace, "Namespace for the control plane.")
	f.StringVar(&o.workerNamespace, "worker-namespace", "", "Namespace for worker pods (defaults to --namespace).")
	f.StringVar(&o.controllerImage, "controller-image", drivers.DefaultControllerManagerImage, "Controller-manager image ref.")
	f.StringVar(&o.workerImage, "worker-image", drivers.DefaultWorkerImage, "Worker image ref.")
	f.StringVar(&o.version, "version", "", "clrk version to stamp on the install (informational in v1).")
	f.StringVar(&o.storageClass, "storage-class", "", "StorageClass for the cm PVCs (empty uses the cluster default).")
	f.IntVar(&o.workers, "workers", 1, "Worker replica count.")
	f.StringVar(&o.pull, "pull", "", "Image pull policy: always | missing | never (default IfNotPresent).")
	f.StringVar(&o.crdMode, "crd-mode", "if-missing", "Gateway-API/Envoy-Gateway CRD handling: always | if-missing | skip.")
	f.StringVar(&o.imagePullSecret, "image-pull-secret", "", "Name of an imagePullSecret to attach to the cm + worker pods.")
	f.BoolVar(&o.yes, "yes", false, "Skip interactive confirmation (required for non-interactive use).")
	f.BoolVar(&o.skipPreflight, "skip-preflight", false, "Skip the cluster readiness checks.")
	f.BoolVar(&o.dryRun, "dry-run", false, "Print the plan (objects + diffs) without changing the cluster.")
	f.BoolVar(&o.tui, "tui", true, "Render the bring-up in an interactive TUI (auto-disabled when stdout isn't a TTY).")
	f.DurationVar(&o.timeout, "timeout", 5*time.Minute, "Per-wait timeout (cm/workers Available, API discoverable).")
	f.DurationVar(&o.readyInterval, "ready-interval", 2*time.Second, "Polling interval for the readiness gate.")
	return cmd
}

// preflight level styles. Adaptive so the output reads on light + dark
// terminals.
var (
	passStyle = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#15803D", Dark: "#34D399"}).Bold(true)
	warnStyle = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#B45309", Dark: "#FBBF24"}).Bold(true)
	failStyle = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#B91C1C", Dark: "#F87171"}).Bold(true)
	mutedSt   = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#6B7280", Dark: "#9CA3AF"})
)

func levelStyle(l install.Level) lipgloss.Style {
	switch l {
	case install.LevelFail:
		return failStyle
	case install.LevelWarn:
		return warnStyle
	default:
		return passStyle
	}
}

func runInstall(ctx context.Context, o *installOpts) error {
	if o.workerNamespace == "" {
		o.workerNamespace = o.namespace
	}
	crdMode, err := parseCRDModeFlag(o.crdMode)
	if err != nil {
		return err
	}

	rc, err := install.NewRemoteCluster(o.kubeconfig, o.context)
	if err != nil {
		return err
	}
	p := install.Profile{
		Namespace:       o.namespace,
		WorkerNamespace: o.workerNamespace,
		ControllerImage: o.controllerImage,
		WorkerImage:     o.workerImage,
		PullPolicy:      o.pull,
		ImagePullSecret: o.imagePullSecret,
		Replicas:        1,
		Workers:         int32(o.workers),
		StorageClass:    o.storageClass,
		// TLS hardening (cert-manager / self-signed) + scoped RBAC land in a
		// follow-up; v1 install mirrors the dev TLS/RBAC posture and is guarded
		// by ClusterIP reachability.
		TLS:     install.TLSInsecureSkipVerify,
		Version: o.version,
	}

	fmt.Fprintf(os.Stderr, "Target cluster: context %s, namespace %s\n",
		lipgloss.NewStyle().Bold(true).Render(rc.Context()), p.Namespace)
	fmt.Fprintf(os.Stderr, "%s\n\n", mutedSt.Render(
		"note: v1 installs with the dev-equivalent security posture (insecure-skip-verify APIService, "+
			"cluster-admin RBAC); serving-cert + scoped-RBAC hardening is the next milestone."))

	// Preflight.
	if !o.skipPreflight {
		if err := runPreflight(ctx, rc, p, o.yes); err != nil {
			return err
		}
	}

	// Plan: SSA dry-run each object and show create/update/unchanged + diffs.
	cl, err := rc.KubeClient(ctx)
	if err != nil {
		return err
	}
	objs := append(install.BuildControllerManager(p), install.BuildWorkerPool(p)...)
	plans := install.BuildPlan(ctx, cl, "clrk-install", objs)
	renderPlan(plans, rc.Context(), o.crdMode)

	if o.dryRun {
		fmt.Fprintln(os.Stderr, "\n--dry-run: no changes applied. (YAML export via -o yaml lands with the render milestone.)")
		return nil
	}

	// Confirm the risky, mutating install before touching the cluster.
	if !o.yes {
		ok, cerr := confirm(ctx, fmt.Sprintf("\nApply the clrk control plane to context %q? [y/N]: ", rc.Context()))
		if cerr != nil {
			if errors.Is(cerr, errNoTTY) {
				return errors.New("non-interactive terminal: pass --yes to proceed without confirmation")
			}
			return cerr
		}
		if !ok {
			return errors.New("install aborted")
		}
	}

	// Auto-disable the TUI on a non-TTY stdout (CI, piped) unless the user
	// explicitly set --tui.
	useTUI := o.tui
	if !o.tuiSet && !term.IsTerminal(int(os.Stdout.Fd())) {
		useTUI = false
	}

	// Execute the bring-up.
	fmt.Fprintln(os.Stderr)
	if useTUI {
		if err := runInstallTUI(ctx, rc, p, crdMode, o); err != nil {
			return err
		}
	} else {
		steps := install.InstallSteps(rc, rc.RESTConfig(), p, crdMode, o.timeout, o.readyInterval, func(line string) {
			fmt.Fprintf(os.Stderr, "      %s\n", mutedSt.Render(line))
		})
		if err := install.RunSteps(ctx, steps, plainStepLogger); err != nil {
			return err
		}
	}

	fmt.Fprintf(os.Stderr, "\n%s clrk installed into context %s (namespace %s).\n",
		passStyle.Render("OK"), rc.Context(), p.Namespace)
	return nil
}

// installStepNames are the sidebar component names for the install TUI, in
// bring-up order. Must match the Step.Name values InstallSteps produces.
var installStepNames = []string{
	"namespaces", "crds", "controller-manager", "aggregated-api", "worker-pool", "readiness",
}

// runInstallTUI drives the bring-up inside the devtui system-only progress view:
// a step-list sidebar with status glyphs and a per-step streaming-log pane. The
// orchestration runs in a goroutine; the TUI renders on the main goroutine and
// stays up after completion so the operator can review, then quits on `q`.
// stderr is redirected to a file for the TUI's lifetime so stray library writes
// don't corrupt the alt-screen.
func runInstallTUI(ctx context.Context, rc *install.RemoteCluster, p install.Profile, crdMode crds.Mode, o *installOpts) error {
	if restore, err := redirectStderrToFile(filepath.Join(clrkDir, "install.stderr.log")); err == nil {
		defer restore()
	}

	prog := devtui.NewSystem(installStepNames, "install · "+rc.Context())

	// Route slog (e.g. crds.Install's progress) into the cli pane while the TUI
	// owns the screen, then restore.
	prev := slog.Default()
	slog.SetDefault(slog.New(devtui.NewSlogHandler(prog, slog.LevelInfo)))
	defer slog.SetDefault(prev)

	// current is set by the step logger on "start" and read by the log sink;
	// both run on the orchestration goroutine, so no synchronization is needed.
	var current string
	sink := func(line string) {
		if current != "" {
			prog.SendLog(current, line, devtui.StreamStdout)
		}
	}
	logger := func(step install.Step, status string, err error) {
		switch status {
		case "start":
			current = step.Name
			prog.SetStatus(step.Name, devtui.StatusStarting)
			prog.SendLog(step.Name, "why: "+step.Why, devtui.StreamClrk)
		case "done":
			prog.SetStatus(step.Name, devtui.StatusReady)
		case "error":
			prog.SetStatus(step.Name, devtui.StatusError)
			prog.SendLog(step.Name, "error: "+err.Error(), devtui.StreamStderr)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		err := install.RunSteps(runCtx, install.InstallSteps(rc, rc.RESTConfig(), p, crdMode, o.timeout, o.readyInterval, sink), logger)
		if err != nil {
			prog.SendLog(devtui.ClrkSource, "install failed: "+err.Error()+" — press q to exit", devtui.StreamStderr)
		} else {
			prog.SendLog(devtui.ClrkSource, "install complete — press q to exit", devtui.StreamClrk)
		}
		errCh <- err
	}()

	runErr := prog.Run(ctx)
	cancel()
	stepErr := <-errCh
	if stepErr != nil {
		return stepErr
	}
	return runErr
}

// renderPlan prints the SSA dry-run plan: one line per object with its action,
// the risky flag, and an indented diff for updates.
func renderPlan(plans []install.ResourcePlan, context, crdMode string) {
	fmt.Fprintf(os.Stderr, "\nPlan for context %s:\n", context)
	for _, pl := range plans {
		marker := actionStyle(pl.Action).Render(fmt.Sprintf("%-9s", pl.Action))
		line := fmt.Sprintf("  %s %s %s", marker, pl.Kind, pl.Name)
		if pl.Risky {
			line += "  " + warnStyle.Render("[risky: "+pl.Why+"]")
		}
		fmt.Fprintln(os.Stderr, line)
		if pl.Note != "" {
			fmt.Fprintf(os.Stderr, "        %s\n", mutedSt.Render(pl.Note))
		}
		if pl.Diff != "" {
			for _, dl := range splitNonEmpty(pl.Diff) {
				fmt.Fprintf(os.Stderr, "        %s\n", diffLineStyle(dl).Render(dl))
			}
		}
	}
	fmt.Fprintf(os.Stderr, "  %s Gateway-API + Envoy-Gateway CRDs (mode=%s)\n", mutedSt.Render("apply   "), crdMode)
}

func splitNonEmpty(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

func actionStyle(a install.Action) lipgloss.Style {
	switch a {
	case install.ActionCreate:
		return passStyle
	case install.ActionUpdate:
		return warnStyle
	case install.ActionUnknown:
		return failStyle
	default:
		return mutedSt
	}
}

func diffLineStyle(line string) lipgloss.Style {
	switch {
	case strings.HasPrefix(line, "+"):
		return passStyle
	case strings.HasPrefix(line, "-"):
		return failStyle
	default:
		return mutedSt
	}
}

// runPreflight runs the cluster checks, renders them, aborts on FAIL, and
// confirms on WARN unless yes is set.
func runPreflight(ctx context.Context, rc *install.RemoteCluster, p install.Profile, yes bool) error {
	cl, err := rc.KubeClient(ctx)
	if err != nil {
		return err
	}
	disco, err := rc.Discovery()
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "Preflight:")
	results := install.Preflight(ctx, cl, disco, p)
	var fails, warns int
	for _, r := range results {
		label := levelStyle(r.Level).Render(fmt.Sprintf("%-4s", r.Level))
		fmt.Fprintf(os.Stderr, "  %s %s — %s\n", label, r.Name, r.Detail)
		if r.Hint != "" && r.Level != install.LevelPass {
			fmt.Fprintf(os.Stderr, "        %s\n", mutedSt.Render("hint: "+r.Hint))
		}
		switch r.Level {
		case install.LevelFail:
			fails++
		case install.LevelWarn:
			warns++
		}
	}
	if fails > 0 {
		return fmt.Errorf("preflight failed: %d check(s) must be resolved (or pass --skip-preflight to bypass)", fails)
	}
	if warns > 0 && !yes {
		ok, cerr := confirm(ctx, fmt.Sprintf("\n%d warning(s) above. Proceed anyway? [y/N]: ", warns))
		if cerr != nil {
			if errors.Is(cerr, errNoTTY) {
				return errors.New("non-interactive terminal with preflight warnings: pass --yes to proceed")
			}
			return cerr
		}
		if !ok {
			return errors.New("install aborted at preflight")
		}
	}
	return nil
}

// plainStepLogger renders step lifecycle to stderr with a status glyph.
func plainStepLogger(step install.Step, status string, err error) {
	switch status {
	case "start":
		fmt.Fprintf(os.Stderr, "%s %s\n", warnStyle.Render("◐"), step.Name)
		fmt.Fprintf(os.Stderr, "      %s\n", mutedSt.Render("why: "+step.Why))
	case "done":
		fmt.Fprintf(os.Stderr, "%s %s\n", passStyle.Render("●"), step.Name)
	case "error":
		fmt.Fprintf(os.Stderr, "%s %s: %v\n", failStyle.Render("✕"), step.Name, err)
	}
}

// parseCRDModeFlag maps the --crd-mode flag to a crds.Mode.
func parseCRDModeFlag(s string) (crds.Mode, error) {
	switch s {
	case "always":
		return crds.ModeAlways, nil
	case "if-missing", "":
		return crds.ModeIfMissing, nil
	case "skip":
		return crds.ModeSkip, nil
	default:
		return 0, fmt.Errorf("invalid --crd-mode %q (want: always | if-missing | skip)", s)
	}
}
