package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/apoxy-dev/clrk/internal/install"
	"github.com/apoxy-dev/clrk/internal/version"
)

// upgradeOpts is installOpts plus the two upgrade-only gate flags.
type upgradeOpts struct {
	installOpts
	allowDowngrade bool
	force          bool
	// workersSet records whether the operator passed --workers explicitly, so an
	// upgrade that omits it carries the live fleet's replica count forward instead
	// of resetting it to the flag default.
	workersSet bool
}

func newUpgradeCmd() *cobra.Command {
	o := &upgradeOpts{}
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade an existing clrk control plane in a Kubernetes cluster",
		Long: "Re-applies the clrk control plane at a new version. Gates the version " +
			"transition (refuses a downgrade or an unorderable jump without a flag), " +
			"force-applies the Gateway-API + Envoy-Gateway CRD bundle, rolls the " +
			"controller-manager and the workers, and waits for the cluster to converge. " +
			"The controller-manager uses a Recreate strategy, so the aggregated API is " +
			"briefly unavailable while it rolls. No data migration is performed — the " +
			"controller-manager migrates its embedded stores at boot.",
		RunE: func(cmd *cobra.Command, args []string) error {
			o.tuiSet = cmd.Flags().Changed("tui")
			o.workersSet = cmd.Flags().Changed("workers")
			return runUpgrade(cmd.Context(), o)
		},
	}
	registerInstallFlags(cmd, &o.installOpts)
	f := cmd.Flags()
	f.BoolVar(&o.allowDowngrade, "allow-downgrade", false, "Allow installing an older version than is currently installed.")
	f.BoolVar(&o.force, "force", false, "Override the version gate (forces downgrades and unorderable version changes).")

	// --version is the upgrade TARGET here (gated), not an informational stamp;
	// it defaults to this binary's own version. And upgrades force-apply the CRD
	// bundle by default (the new schema is authoritative). Adjust the inherited
	// flags' defaults/usage accordingly.
	if vf := f.Lookup("version"); vf != nil {
		vf.Usage = "Target version to gate + stamp (defaults to this binary's version); " +
			"the deployed image is governed by --controller-image/--worker-image, not this flag."
	}
	o.crdMode = "always"
	if cf := f.Lookup("crd-mode"); cf != nil {
		cf.DefValue = "always"
		cf.Usage = "Gateway-API/Envoy-Gateway CRD handling on upgrade: always (default) | if-missing | skip."
	}
	return cmd
}

func runUpgrade(ctx context.Context, o *upgradeOpts) error {
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

	rc, cl, tlsMode, err := resolveTarget(ctx, &o.installOpts)
	if err != nil {
		return err
	}

	// Require an existing install, then gate the version transition.
	exists, installed, err := install.DetectInstall(ctx, cl, o.namespace)
	if err != nil {
		return fmt.Errorf("checking for an existing install: %w", err)
	}
	if !exists {
		return fmt.Errorf("no clrk control plane found in namespace %q on context %q; use `clrk install`",
			o.namespace, rc.Context())
	}

	target := o.version
	if target == "" {
		target = version.Current()
	}
	if target == "" {
		return errors.New("no target version: build clrk with a version ldflag or pass --version")
	}

	decision := install.GateUpgrade(installed, target, o.allowDowngrade, o.force)
	fmt.Fprintf(os.Stderr, "Version gate: %s\n", decision.Reason)
	switch decision.Verdict {
	case install.VerdictRefuse:
		return errors.New(decision.Reason)
	case install.VerdictConfirm:
		if !o.yes && !o.force {
			ok, cerr := confirm(ctx, "\nProceed with this upgrade? [y/N]: ")
			if cerr != nil {
				if errors.Is(cerr, errNoTTY) {
					// --yes is the non-interactive escape: --force only overrides the
					// gate verdict, the shared plan confirm still needs --yes.
					return errors.New("non-interactive terminal: pass --yes to proceed")
				}
				return cerr
			}
			if !ok {
				return errors.New("upgrade aborted at the version gate")
			}
		}
	}

	// Carry the existing fleet's worker count forward unless the operator
	// re-specified --workers: upgrade bumps the version, it must not silently
	// reset spec the operator set at install (or scaled later) to the flag default
	// (ForceOwnership SSA would overwrite spec.replicas otherwise).
	if !o.workersSet {
		if n, ok, werr := install.CurrentWorkerCount(ctx, cl, o.workerNamespace, install.DefaultWorkerPoolName); werr != nil {
			return fmt.Errorf("reading the existing WorkerPool replica count: %w", werr)
		} else if ok {
			o.workers = n
		}
	}

	p := o.buildProfile(tlsMode, rbacScoped, target)

	return applyControlPlane(ctx, controlPlaneApply{
		o:            &o.installOpts,
		rc:           rc,
		cl:           cl,
		profile:      p,
		tlsMode:      tlsMode,
		rbacScoped:   rbacScoped,
		crdMode:      crdMode,
		crdModeLabel: o.crdMode,
		upgrade:      true,
		confirmPrompt: fmt.Sprintf(
			"\nUpgrade clrk in context %q (%s -> %s)? The controller-manager will Recreate (brief API downtime). [y/N]: ",
			rc.Context(), orUnstamped(installed), target),
		okMessage: fmt.Sprintf("clrk upgraded in context %s to %s (namespace %s).",
			rc.Context(), target, p.Namespace),
	})
}

// orUnstamped renders an installed-version string for prompts, marking an
// install that recorded no version.
func orUnstamped(v string) string {
	if v == "" {
		return "(unstamped)"
	}
	return v
}
