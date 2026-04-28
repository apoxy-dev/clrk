package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

// newApplyCmd is `clrk apply -f <path>`. A thin wrapper over the same
// applyManifests engine `clrk dev --apply` uses, with `--local` resolving
// the kubeconfig of the running `clrk dev` session automatically so users
// don't have to remember `KUBECONFIG=~/.clrk/kubeconfig.host`.
func newApplyCmd() *cobra.Command {
	var (
		files      []string
		recursive  bool
		local      bool
		kubeconfig string
	)

	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Server-side-apply CRDs against a clrk apiserver",
		Long: "Reads YAML/JSON manifests and server-side-applies each document " +
			"against the cluster. Use --local to target the kubeconfig of the " +
			"running `clrk dev` session under --clrk-dir.",
		RunE: func(cmd *cobra.Command, args []string) error {
			kc, err := resolveKubeconfig(kubeconfig, local)
			if err != nil {
				return err
			}
			paths := append([]string(nil), files...)
			paths = append(paths, args...)
			if len(paths) == 0 {
				return fmt.Errorf("no manifests: pass paths positionally or with -f")
			}
			return applyManifests(cmd.Context(), kc, paths, recursive)
		},
	}

	cmd.Flags().StringArrayVarP(&files, "filename", "f", nil, "YAML/JSON file or directory to apply (repeatable; positional args also accepted).")
	cmd.Flags().BoolVarP(&recursive, "recursive", "R", false, "Recurse into subdirectories.")
	cmd.Flags().BoolVar(&local, "local", false, "Target the kubeconfig of the running `clrk dev` session (~/.clrk/kubeconfig.host by default).")
	cmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "Explicit kubeconfig path (takes precedence over --local and $KUBECONFIG).")
	return cmd
}

// resolveKubeconfig picks a kubeconfig in this order: explicit --kubeconfig,
// --local (clrk dev's host kubeconfig under --clrk-dir), then $KUBECONFIG.
func resolveKubeconfig(explicit string, local bool) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if local {
		path := filepath.Join(clrkDir, "kubeconfig.host")
		if _, err := os.Stat(path); err != nil {
			return "", fmt.Errorf("--local kubeconfig %s not found (is `clrk dev` running?): %w", path, err)
		}
		return path, nil
	}
	if env := os.Getenv("KUBECONFIG"); env != "" {
		return env, nil
	}
	return "", fmt.Errorf("no kubeconfig: pass --kubeconfig, set KUBECONFIG, or use --local")
}
