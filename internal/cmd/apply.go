package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// newApplyCmd is `clrk apply -f <path>`. A thin wrapper over the same
// applyManifests engine `clrk dev --apply` uses. Cluster targeting comes from
// the global --kubeconfig/--context/--local flags (see kube.go).
func newApplyCmd() *cobra.Command {
	var (
		files     []string
		recursive bool
	)

	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Server-side-apply CRDs against a clrk apiserver",
		Long: "Reads YAML/JSON manifests and server-side-applies each document " +
			"against the cluster. Targets your standard kubeconfig ($KUBECONFIG, " +
			"then ~/.kube/config); use --context to pick a context, --kubeconfig for " +
			"an explicit file, or --local to target the running `clrk dev` session.",
		RunE: func(cmd *cobra.Command, args []string) error {
			cc, err := kube.clientConfig()
			if err != nil {
				return err
			}
			paths := append([]string(nil), files...)
			paths = append(paths, args...)
			if len(paths) == 0 {
				return fmt.Errorf("no manifests: pass paths positionally or with -f")
			}
			return applyManifests(cmd.Context(), cc, paths, recursive)
		},
	}

	cmd.Flags().StringArrayVarP(&files, "filename", "f", nil, "YAML/JSON file or directory to apply (repeatable; positional args also accepted).")
	cmd.Flags().BoolVarP(&recursive, "recursive", "R", false, "Recurse into subdirectories.")
	return cmd
}
