package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/spf13/cobra"

	"github.com/apoxy-dev/clrk/internal/drivers"
)

// newDevPushImageCmd is `clrk dev push-image <component> --tar <path>`.
// Loads a docker-archive OCI tarball (the format rules_oci's oci_tarball
// emits) and pushes it straight to the live dev session's local registry
// at localhost:<RegistryHostPort()>, avoiding the
// `docker load → docker tag → docker push` shuffle. With --reload, also
// triggers the matching Deployment rollout so the next pod re-pulls.
func newDevPushImageCmd() *cobra.Command {
	var (
		dataDir string
		tarPath string
		reload  bool
	)
	cmd := &cobra.Command{
		Use:   "push-image <component>",
		Short: "Push an OCI tarball into the live clrk dev local registry",
		Long: "Loads <tar> (a docker-archive produced by rules_oci's oci_tarball, " +
			"e.g. bazel-bin/clrk/worker_oci_tarball/tarball.tar or the per-arch " +
			"tarball exported from `dagger call build-clrk`) and pushes it to " +
			"localhost:<registry-port>/clrk/<component>:dev. The registry port " +
			"is read from the running session's dev.json. <component> is " +
			"`worker` or `controller-manager`. Pair with --reload to also " +
			"roll the matching Deployment.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if dataDir == "" {
				dataDir = clrkDir
			}
			sess, err := readDevSession(dataDir)
			if err != nil {
				return err
			}
			comp := args[0]
			switch comp {
			case "controller-manager", "worker":
			default:
				return fmt.Errorf("component must be worker or controller-manager, got %q", comp)
			}
			if tarPath == "" {
				return errors.New("--tar is required")
			}
			return pushImageToDevRegistry(cmd.Context(), sess, comp, tarPath, reload)
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "Host path of the running session's data dir (defaults to --clrk-dir).")
	cmd.Flags().StringVar(&tarPath, "tar", "", "Path to a docker-archive OCI tarball.")
	cmd.Flags().BoolVar(&reload, "reload", false, "After the push, roll the matching Deployment and block until the new pod is running the just-pushed image (fails loudly if the node served a cached :dev tag instead of re-pulling).")
	return cmd
}

// pushImageToDevRegistry loads the docker-archive at tarPath, validates
// it's a linux image (the most common footgun is a darwin-host build
// where bazel produces Mach-O binaries inside the OCI layers), then
// streams it to the dev session's local registry as
// clrk/<comp>:dev. The in-cluster pods reference the same content via
// `clrk-registry:5000/clrk/<comp>:dev` (docker DNS on the shared
// network) so a successful push + rollout swaps the running image.
func pushImageToDevRegistry(ctx context.Context, sess *devSession, comp, tarPath string, reload bool) error {
	if sess.RegistryHostPort == 0 {
		return fmt.Errorf("dev session has no local registry; relaunch `clrk dev` with at least one --registry-image=COMPONENT=clrk-registry:5000/... to enable it")
	}
	refStr := drivers.LocalRegistryHostRef(comp, sess.RegistryHostPort)
	ref, err := name.NewTag(refStr)
	if err != nil {
		return fmt.Errorf("parsing target ref %q: %w", refStr, err)
	}

	img, err := tarball.ImageFromPath(tarPath, nil)
	if err != nil {
		return fmt.Errorf("loading %s as docker-archive: %w", tarPath, err)
	}

	cfg, err := img.ConfigFile()
	if err != nil {
		return fmt.Errorf("reading image config: %w", err)
	}
	if cfg.OS != "linux" {
		return fmt.Errorf("image OS is %q (want linux); rebuild via dagger or bazel with the right --config so the entrypoint is a linux ELF, not a %s binary", cfg.OS, cfg.OS)
	}

	slog.Info("Pushing image to dev registry",
		"target", refStr,
		"tar", tarPath,
		"os", cfg.OS,
		"arch", cfg.Architecture)
	// No auth: ctlptl's local registry runs anonymously on the shared
	// clrk network.
	if err := remote.Write(ref, img, remote.WithContext(ctx)); err != nil {
		return fmt.Errorf("pushing %s: %w", refStr, err)
	}
	// Capture the pushed image's digests so a subsequent --reload can
	// assert the live pod actually came up on THIS image rather than a
	// cached `:dev` tag. Container runtimes surface either the manifest
	// digest or the image-config digest as the pod's imageID, so collect
	// both and let the wait match on either.
	var wantDigests []string
	if d, derr := img.Digest(); derr == nil {
		slog.Info("Push complete", "digest", d.String())
		wantDigests = append(wantDigests, d.String())
	} else {
		slog.Info("Push complete")
	}
	if d, derr := img.ConfigName(); derr == nil {
		wantDigests = append(wantDigests, d.String())
	}

	if !reload {
		return nil
	}
	return reloadAndWait(ctx, sess, comp, wantDigests...)
}
