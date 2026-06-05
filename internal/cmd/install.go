package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
	"golang.org/x/term"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"

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
	rbac            string
	tlsMode         string
	apiserverCIDR   string
	networkPolicy   bool
	yes             bool
	skipPreflight   bool
	dryRun          bool
	output          string
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
	registerInstallFlags(cmd, o)
	return cmd
}

// registerInstallFlags registers the flags shared by `clrk install` and `clrk
// upgrade` onto cmd.
func registerInstallFlags(cmd *cobra.Command, o *installOpts) {
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
	f.StringVar(&o.rbac, "rbac", "scoped", "Controller-manager + worker RBAC: scoped | cluster-admin.")
	f.StringVar(&o.tlsMode, "tls", "auto", "APIService serving TLS: auto (cert-manager if present, else self-signed) | cert-manager | self-signed | insecure.")
	f.StringVar(&o.apiserverCIDR, "apiserver-cidr", "", "CIDR(s) allowed to reach the unauthenticated aggregated API, comma-separated (auto-derived from apiserver Endpoints + node IPs).")
	f.BoolVar(&o.networkPolicy, "network-policy", true, "Emit a NetworkPolicy restricting the aggregated API (cm:8443) to the apiserver/node CIDRs.")
	f.BoolVar(&o.yes, "yes", false, "Skip interactive confirmation (required for non-interactive use).")
	f.BoolVar(&o.skipPreflight, "skip-preflight", false, "Skip the cluster readiness checks.")
	f.BoolVar(&o.dryRun, "dry-run", false, "Print the plan (objects + diffs) without changing the cluster.")
	f.StringVarP(&o.output, "output", "o", "", "Output format: empty prints the human plan; 'yaml' emits the full manifest set to stdout (implies no apply) for GitOps/audit.")
	f.BoolVar(&o.tui, "tui", true, "Render the bring-up as a live status view with toggleable per-step logs (auto-disabled when stdout isn't a TTY).")
	f.DurationVar(&o.timeout, "timeout", 5*time.Minute, "Per-wait timeout (cm/workers Available, API discoverable).")
	f.DurationVar(&o.readyInterval, "ready-interval", 2*time.Second, "Polling interval for the readiness gate.")
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
	rbacScoped, err := parseRBACFlag(o.rbac)
	if err != nil {
		return err
	}
	if err := validatePullFlag(o.pull); err != nil {
		return err
	}
	if err := validateOutputFlag(o.output); err != nil {
		return err
	}

	rc, cl, tlsMode, err := resolveTarget(ctx, o)
	if err != nil {
		return err
	}
	p := o.buildProfile(tlsMode, rbacScoped, o.version)

	return applyControlPlane(ctx, controlPlaneApply{
		o:            o,
		rc:           rc,
		cl:           cl,
		profile:      p,
		tlsMode:      tlsMode,
		rbacScoped:   rbacScoped,
		crdMode:      crdMode,
		crdModeLabel: o.crdMode,
		confirmPrompt: fmt.Sprintf(
			"\nApply the clrk control plane to context %q? [y/N]: ", rc.Context()),
		okMessage: fmt.Sprintf("clrk installed into context %s (namespace %s).",
			rc.Context(), p.Namespace),
	})
}

// resolveTarget resolves the kubeconfig+context into a RemoteCluster, its
// controller-runtime client, and the resolved serving-TLS mode. Shared by
// install and upgrade.
func resolveTarget(ctx context.Context, o *installOpts) (*install.RemoteCluster, client.Client, install.TLSMode, error) {
	rc, err := install.NewRemoteCluster(o.kubeconfig, o.context)
	if err != nil {
		return nil, nil, 0, err
	}
	cl, err := rc.KubeClient(ctx)
	if err != nil {
		return nil, nil, 0, err
	}
	disco, err := rc.Discovery()
	if err != nil {
		return nil, nil, 0, err
	}
	// auto picks cert-manager when its CRDs are present, else self-signed;
	// insecure mirrors the dev posture.
	tlsMode, err := resolveTLSMode(o.tlsMode, disco)
	if err != nil {
		return nil, nil, 0, err
	}
	return rc, cl, tlsMode, nil
}

// buildProfile assembles the install.Profile from the resolved flags. version is
// the value to stamp: --version for install, the gated target for upgrade.
func (o *installOpts) buildProfile(tlsMode install.TLSMode, rbacScoped bool, version string) install.Profile {
	return install.Profile{
		Namespace:       o.namespace,
		WorkerNamespace: o.workerNamespace,
		ControllerImage: o.controllerImage,
		WorkerImage:     o.workerImage,
		PullPolicy:      o.pull,
		ImagePullSecret: o.imagePullSecret,
		Replicas:        1,
		Workers:         int32(o.workers),
		StorageClass:    o.storageClass,
		RBACScoped:      rbacScoped,
		TLS:             tlsMode,
		Version:         version,
	}
}

// controlPlaneApply carries the resolved inputs for the shared install/upgrade
// bring-up pipeline.
type controlPlaneApply struct {
	o             *installOpts
	rc            *install.RemoteCluster
	cl            client.Client
	profile       install.Profile
	tlsMode       install.TLSMode
	rbacScoped    bool
	crdMode       crds.Mode
	crdModeLabel  string
	upgrade       bool
	confirmPrompt string
	okMessage     string
}

// applyControlPlane runs the pipeline both `clrk install` and `clrk upgrade`
// share: print the posture, preflight, prepare serving TLS, derive the
// NetworkPolicy CIDRs, render the SSA plan, gate on confirmation, and run the
// ordered bring-up. The upgrade switch (set by the caller) flips the worker-roll
// behaviour and the prompt/OK text; the caller also forces ModeAlways for the
// CRD step on upgrade.
func applyControlPlane(ctx context.Context, a controlPlaneApply) error {
	o := a.o
	p := a.profile

	// -o yaml renders the manifest set to stdout and never mutates the cluster, so
	// it skips the apply-readiness preflight and the confirm gate below.
	renderOnly := o.output == outputYAML

	fmt.Fprintf(os.Stderr, "Target cluster: context %s, namespace %s\n",
		lipgloss.NewStyle().Bold(true).Render(a.rc.Context()), p.Namespace)
	fmt.Fprintf(os.Stderr, "%s\n", mutedSt.Render(fmt.Sprintf(
		"posture: TLS=%s, RBAC=%s, NetworkPolicy=%s", tlsModeLabel(a.tlsMode), rbacLabel(a.rbacScoped), onOff(o.networkPolicy))))
	if a.tlsMode == install.TLSInsecureSkipVerify {
		fmt.Fprintf(os.Stderr, "%s\n", warnStyle.Render(
			"warning: --tls=insecure serves the aggregated API without a verified cert (dev posture)."))
	}
	fmt.Fprintln(os.Stderr)

	// Preflight. Skipped for a YAML render — it probes apply readiness (RBAC
	// can-i, PVC provisioner, PSA), none of which gate emitting manifests, and a
	// FAIL must not abort before the artifact is written.
	if !renderOnly && !o.skipPreflight {
		if err := runPreflight(ctx, a.rc, p, o.yes, a.upgrade); err != nil {
			return err
		}
	}

	// Serving cert: build (and, for self-signed, mint/reuse the CA) the cert
	// objects, which also stamps the APIService caBundle / inject-ca-from
	// annotation onto p so the built objects reflect the real posture.
	certObjs, err := install.PrepareTLS(ctx, a.cl, &p)
	if err != nil {
		return fmt.Errorf("preparing serving TLS: %w", err)
	}

	// NetworkPolicy CIDRs guarding the unauthenticated aggregated API. An
	// explicit --apiserver-cidr wins; otherwise derive from apiserver Endpoints +
	// node IPs. A derivation failure is a warning, not fatal — we skip the policy
	// rather than risk walling off the cm with a wrong CIDR.
	if o.networkPolicy {
		p.APIServerCIDRs, err = resolveAPIServerCIDRs(ctx, a.cl, o.apiserverCIDR)
		if err != nil {
			if o.apiserverCIDR != "" {
				// An explicit override that doesn't parse is an operator error,
				// not a soft auto-derive miss — fail loudly rather than silently
				// dropping the policy they asked for (a bad IPBlock would also
				// abort the apply mid-flight).
				return fmt.Errorf("invalid --apiserver-cidr: %w", err)
			}
			fmt.Fprintf(os.Stderr, "%s\n", warnStyle.Render(
				"warning: could not derive an apiserver CIDR ("+err.Error()+"); skipping the NetworkPolicy. Pass --apiserver-cidr to enable it."))
		}
	}

	// -o yaml: emit the full ordered manifest set to stdout (all human chatter
	// went to stderr, so a redirect captures clean YAML) and return without
	// touching the cluster.
	if renderOnly {
		return renderManifestsYAML(a, p, certObjs)
	}

	// Plan: SSA dry-run each object and show create/update/unchanged + diffs.
	// Cert objects apply first (the cm mounts/serves them), then the cm set, then
	// the worker set.
	objs := append(append(append([]client.Object{}, certObjs...),
		install.BuildControllerManager(p)...), install.BuildWorkerPool(p)...)
	var plans []install.ResourcePlan
	withSpinner("Computing changes (server-side dry-run)", 1200*time.Millisecond, func() {
		plans = install.BuildPlan(ctx, a.cl, "clrk-install", objs)
	})
	renderPlan(plans, a.rc.Context(), a.crdModeLabel)

	if o.dryRun {
		fmt.Fprintln(os.Stderr, "\n--dry-run: no changes applied. Re-run with -o yaml to emit the manifest set.")
		return nil
	}

	// Confirm the risky, mutating apply before touching the cluster.
	if !o.yes {
		ok, cerr := confirm(ctx, a.confirmPrompt)
		if cerr != nil {
			if errors.Is(cerr, errNoTTY) {
				return errors.New("non-interactive terminal: pass --yes to proceed without confirmation")
			}
			return cerr
		}
		if !ok {
			return errors.New("aborted")
		}
	}

	// Bundle the bring-up. cert-manager mints the serving Secret asynchronously,
	// so wait for it before the cm rolls; the self-signed Secret is applied
	// inline and needs no wait.
	orch := install.Orchestration{
		Applier:        a.rc,
		Config:         a.rc.RESTConfig(),
		Profile:        p,
		CRDMode:        a.crdMode,
		CertObjects:    certObjs,
		WaitCertSecret: a.tlsMode == install.TLSCertManager,
		Upgrade:        a.upgrade,
		WaitTimeout:    o.timeout,
		ReadyInterval:  o.readyInterval,
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
		if err := runInstallTUI(ctx, a.rc.Context(), orch); err != nil {
			return err
		}
	} else {
		orch.Log = func(line string) {
			fmt.Fprintf(os.Stderr, "      %s\n", mutedSt.Render(line))
		}
		if err := install.RunSteps(ctx, orch.Steps(), plainStepLogger); err != nil {
			return err
		}
	}

	fmt.Fprintf(os.Stderr, "\n%s %s\n", passStyle.Render("OK"), a.okMessage)
	return nil
}

// runInstallTUI drives the bring-up inside the compact devtui status view: an
// inline step checklist with a spinner on the in-flight step, its latest log
// line (including pod-failure reasons) shown beneath it, and a per-step log tail
// the operator can toggle with `l`. The orchestration runs in a goroutine; the
// view renders on the main goroutine, updates in place, and stays up after
// completion so the operator can review, then quits on `q`. stderr is
// redirected to a file for the view's lifetime so stray library writes don't
// corrupt the in-place redraw.
func runInstallTUI(ctx context.Context, contextName string, orch install.Orchestration) error {
	if restore, err := redirectStderrToFile(filepath.Join(clrkDir, "install.stderr.log")); err == nil {
		defer restore()
	}

	// current is the step the log sink attributes lines to. The step logger sets
	// it on "start" (orchestration goroutine); orch.Log reads it — and is now
	// also called from runWithPodDiagnostics' background diagnoser — so guard it
	// with a mutex rather than rely on the diagnoser happening to be joined
	// within its step.
	var (
		mu      sync.Mutex
		current string
	)
	var prog *devtui.Program
	setCurrent := func(name string) {
		mu.Lock()
		current = name
		mu.Unlock()
	}
	orch.Log = func(line string) {
		mu.Lock()
		c := current
		mu.Unlock()
		if c != "" {
			prog.SendLog(c, line, devtui.StreamStdout)
		}
	}

	// Build the steps (with Log wired) and seed the checklist from them so the
	// step list matches what actually runs (e.g. the conditional serving-cert
	// step).
	steps := orch.Steps()
	verb := "install"
	if orch.Upgrade {
		verb = "upgrade"
	}
	prog = devtui.NewStatus(install.StepNames(steps), verb+" · "+contextName)

	// Route slog (e.g. crds.Install's progress) into the cli pane while the TUI
	// owns the screen, then restore.
	prev := slog.Default()
	slog.SetDefault(slog.New(devtui.NewSlogHandler(prog, slog.LevelInfo)))
	defer slog.SetDefault(prev)

	logger := func(step install.Step, status string, err error) {
		switch status {
		case "start":
			setCurrent(step.Name)
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
		err := install.RunSteps(runCtx, steps, logger)
		if err != nil {
			prog.SendLog(devtui.ClrkSource, verb+" failed: "+err.Error(), devtui.StreamStderr)
		} else {
			prog.SendLog(devtui.ClrkSource, verb+" complete", devtui.StreamClrk)
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

// renderManifestsYAML renders the resolved control-plane manifest set and writes
// it to stdout, prefixed with a provenance comment (context/namespace/posture)
// for audit. All status/progress output is on stderr, so
// `clrk install ... -o yaml > clrk.yaml` captures clean, apply-able YAML. The
// self-signed sensitive-material warning is also echoed to stderr so it is seen
// even when stdout is redirected to a file.
func renderManifestsYAML(a controlPlaneApply, p install.Profile, certObjs []client.Object) error {
	out, err := install.RenderManifests(install.RenderInput{
		Profile:     p,
		CertObjects: certObjs,
		CRDMode:     a.crdMode,
	})
	if err != nil {
		return fmt.Errorf("rendering manifests: %w", err)
	}

	verb := "install"
	if a.upgrade {
		verb = "upgrade"
	}
	// Report whether a NetworkPolicy is actually in the rendered set, not the raw
	// --network-policy flag: when the flag is on but CIDR derivation soft-failed
	// (no --apiserver-cidr), p.APIServerCIDRs is empty and the builder emits no
	// policy, so "on" would misrepresent the artifact.
	netpol := a.o.networkPolicy && len(p.APIServerCIDRs) > 0
	header := fmt.Sprintf("# Generated by clrk %s --dry-run -o yaml\n", verb) +
		fmt.Sprintf("# context: %s  namespace: %s  worker-namespace: %s\n", a.rc.Context(), p.Namespace, p.WorkerNamespace) +
		fmt.Sprintf("# version: %s  tls: %s  rbac: %s  crd-mode: %s  network-policy: %s\n",
			orUnstamped(p.Version), tlsModeLabel(a.tlsMode), rbacLabel(a.rbacScoped), a.crdModeLabel, onOff(netpol)) +
		"# Apply with: kubectl apply -f -\n"

	if _, err := io.WriteString(os.Stdout, header); err != nil {
		return err
	}
	if _, err := os.Stdout.Write(out); err != nil {
		return err
	}
	if a.tlsMode == install.TLSSelfSigned {
		fmt.Fprintf(os.Stderr, "\n%s\n", warnStyle.Render(
			"warning: the rendered set embeds private key material (self-signed CA + serving-cert Secrets); treat it as sensitive."))
	}
	return nil
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
// confirms on WARN unless yes is set. upgrade flips the existing-install check
// from a WARN to an informational PASS.
func runPreflight(ctx context.Context, rc *install.RemoteCluster, p install.Profile, yes, upgrade bool) error {
	cl, err := rc.KubeClient(ctx)
	if err != nil {
		return err
	}
	disco, err := rc.Discovery()
	if err != nil {
		return err
	}
	var results []install.PreflightResult
	withSpinner("Running preflight checks", 1500*time.Millisecond, func() {
		results = install.Preflight(ctx, cl, disco, p, upgrade)
	})
	fmt.Fprintln(os.Stderr, "Preflight:")
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
			return errors.New("aborted at preflight")
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

// parseRBACFlag maps the --rbac flag to Profile.RBACScoped.
func parseRBACFlag(s string) (bool, error) {
	switch s {
	case "scoped", "":
		return true, nil
	case "cluster-admin":
		return false, nil
	default:
		return false, fmt.Errorf("invalid --rbac %q (want: scoped | cluster-admin)", s)
	}
}

// resolveTLSMode maps the --tls flag to an install.TLSMode, auto-detecting
// cert-manager when "auto". An explicit "cert-manager" errors if its CRDs are
// absent rather than silently downgrading.
func resolveTLSMode(flag string, disco discovery.DiscoveryInterface) (install.TLSMode, error) {
	switch flag {
	case "auto", "":
		if install.DetectCertManager(disco) {
			return install.TLSCertManager, nil
		}
		return install.TLSSelfSigned, nil
	case "cert-manager":
		if !install.DetectCertManager(disco) {
			return 0, errors.New("--tls=cert-manager but the cert-manager.io API group is not served on this cluster")
		}
		return install.TLSCertManager, nil
	case "self-signed":
		return install.TLSSelfSigned, nil
	case "insecure":
		return install.TLSInsecureSkipVerify, nil
	default:
		return 0, fmt.Errorf("invalid --tls %q (want: auto | cert-manager | self-signed | insecure)", flag)
	}
}

// resolveAPIServerCIDRs uses an explicit --apiserver-cidr override (comma-
// separated) when set, else derives the host-network CIDRs from the cluster.
func resolveAPIServerCIDRs(ctx context.Context, cl client.Client, override string) ([]string, error) {
	if override != "" {
		var cidrs []string
		for _, c := range strings.Split(override, ",") {
			c = strings.TrimSpace(c)
			if c == "" {
				continue
			}
			norm, err := normalizeCIDR(c)
			if err != nil {
				return nil, err
			}
			cidrs = append(cidrs, norm)
		}
		if len(cidrs) == 0 {
			return nil, errors.New("--apiserver-cidr is set but has no usable entries")
		}
		return cidrs, nil
	}
	return install.DeriveAPIServerCIDRs(ctx, cl)
}

// normalizeCIDR validates an --apiserver-cidr entry and returns it in CIDR
// notation, accepting a bare IP (converted to a single-host /32 or /128) so the
// NetworkPolicy always gets an IPBlock the apiserver accepts. The auto-derive
// path already normalizes via hostCIDR; this brings the override to parity.
func normalizeCIDR(s string) (string, error) {
	if _, _, err := net.ParseCIDR(s); err == nil {
		return s, nil
	}
	if ip := net.ParseIP(s); ip != nil {
		if ip.To4() != nil {
			return s + "/32", nil
		}
		return s + "/128", nil
	}
	return "", fmt.Errorf("invalid --apiserver-cidr entry %q (want a CIDR like 10.0.0.0/24 or a bare IP)", s)
}

// validatePullFlag rejects an unknown --pull value up front (mirroring --rbac/
// --tls/--crd-mode) so a typo like "alway" fails loudly instead of silently
// falling back to IfNotPresent.
func validatePullFlag(s string) error {
	switch s {
	case "", "always", "missing", "never":
		return nil
	default:
		return fmt.Errorf("invalid --pull %q (want: always | missing | never)", s)
	}
}

// outputYAML is the -o value that emits the rendered manifest set instead of the
// human-readable plan.
const outputYAML = "yaml"

// validateOutputFlag rejects an unknown -o/--output value. Empty is the default
// (human plan); "yaml" emits the manifest set.
func validateOutputFlag(s string) error {
	switch s {
	case "", outputYAML:
		return nil
	default:
		return fmt.Errorf("invalid --output %q (want: yaml, or omit for the human plan)", s)
	}
}

func tlsModeLabel(m install.TLSMode) string {
	switch m {
	case install.TLSCertManager:
		return "cert-manager"
	case install.TLSSelfSigned:
		return "self-signed"
	default:
		return "insecure"
	}
}

func rbacLabel(scoped bool) string {
	if scoped {
		return "scoped"
	}
	return "cluster-admin"
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
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
