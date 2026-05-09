package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	apiregv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	apiregclient "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset/typed/apiregistration/v1"
	apiextclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"

	"github.com/apoxy-dev/clrk/internal/drivers"
)

// newDevWaitReadyCmd is `clrk dev wait-ready`. Polls a running clrk
// dev session against a host-side kubeconfig until every signal a
// downstream consumer (e.g. an integration test) needs has flipped
// green, or until --timeout fires. Replaces ad-hoc loops over docker
// logs / docker ps / kubectl by callers.
//
// Signals (all required):
//   - k3s apiserver answers discovery
//   - clrk.apoxy.dev/v1alpha1 APIService is Available
//   - Gateway API CRDs are installed (the in-process EG control plane
//     would crash-loop without them)
//   - <clrk-ns>/envoy-gateway Secret exists (certgen ran; EG xDS
//     server has the cert it needs to serve data-plane Envoys)
func newDevWaitReadyCmd() *cobra.Command {
	var (
		timeout    time.Duration
		interval   time.Duration
		kubeconfig string
	)

	cmd := &cobra.Command{
		Use:   "wait-ready",
		Short: "Block until a running clrk dev session is fully ready",
		RunE: func(cmd *cobra.Command, args []string) error {
			if kubeconfig == "" {
				kubeconfig = filepath.Join(clrkDir, drivers.KubeconfigFileName+".host")
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()
			return waitDevReady(ctx, kubeconfig, interval)
		},
	}

	cmd.Flags().DurationVar(&timeout, "timeout", 2*time.Minute, "Give up after this long.")
	cmd.Flags().DurationVar(&interval, "interval", 1*time.Second, "Time between probe rounds.")
	cmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "Path to the host-side kubeconfig (defaults to ~/.clrk/kubeconfig.host).")
	return cmd
}

// perCheckTimeout caps a single probe round. Combined with rest.Config's
// own Timeout this makes every check fail-fast: if the apiserver TCP is
// up but HTTPS hangs (orbstack pause/resume, controller-manager mid-
// crash, certgen mid-flight), the whole poll loop still spins instead
// of blocking on a single hung Dial.
const perCheckTimeout = 3 * time.Second

func waitDevReady(ctx context.Context, kubeconfigPath string, interval time.Duration) error {
	// Wait for the kubeconfig file itself — clrk dev writes it once
	// k3s has booted, so the file's absence is "still booting" not
	// "missing dev session".
	for {
		if _, err := os.Stat(kubeconfigPath); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for kubeconfig at %s", kubeconfigPath)
		case <-time.After(interval):
		}
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return fmt.Errorf("loading kubeconfig %s: %w", kubeconfigPath, err)
	}
	// rest.Config.Timeout caps every request the derived clients make,
	// regardless of whether the API method takes a context. Belt-and-
	// suspenders for client-go calls like Discovery().ServerVersion()
	// that historically didn't accept a ctx.
	cfg.Timeout = perCheckTimeout
	core, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("kubernetes client: %w", err)
	}
	apireg, err := apiregclient.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("apiregistration client: %w", err)
	}
	apiext, err := apiextclient.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("apiextensions client: %w", err)
	}

	checks := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"k3s apiserver", func(c context.Context) error {
			// /livez is a context-aware probe that goes through the
			// rest client (so cfg.Timeout applies). Avoids the legacy
			// Discovery().ServerVersion() shape, which ignores ctx and
			// can hang past the per-check deadline.
			_, err := core.Discovery().RESTClient().Get().AbsPath("/livez").DoRaw(c)
			return err
		}},
		{"clrk.apoxy.dev APIService Available", func(c context.Context) error {
			as, err := apireg.APIServices().Get(c, "v1alpha1.clrk.apoxy.dev", metav1.GetOptions{})
			if err != nil {
				return err
			}
			for _, cond := range as.Status.Conditions {
				if cond.Type == apiregv1.Available {
					if cond.Status == apiregv1.ConditionTrue {
						return nil
					}
					return fmt.Errorf("APIService not Available: %s", cond.Message)
				}
			}
			return errors.New("APIService Available condition not yet set")
		}},
		{"Gateway API CRDs installed", func(c context.Context) error {
			_, err := apiext.CustomResourceDefinitions().Get(c, "gateways.gateway.networking.k8s.io", metav1.GetOptions{})
			return err
		}},
		{"EG xDS Secret materialized", func(c context.Context) error {
			_, err := core.CoreV1().Secrets(devClrkNamespace).Get(c, "envoy-gateway", metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("%s/envoy-gateway not found yet", devClrkNamespace)
			}
			return err
		}},
	}

	// Once a check passes we never re-run it: every signal here is
	// monotonic (Available stays Available, Secret keeps existing).
	// This also makes progress visible — each green tick is a one-shot
	// announcement instead of being lost in the polling churn.
	passed := make(map[string]bool, len(checks))
	round := 0
	fmt.Fprintf(os.Stderr, "Waiting for clrk dev ready (kubeconfig=%s)...\n", kubeconfigPath)
	for {
		round++
		var pending []string
		for _, ch := range checks {
			if passed[ch.name] {
				continue
			}
			rctx, cancel := context.WithTimeout(ctx, perCheckTimeout)
			err := ch.fn(rctx)
			cancel()
			if err == nil {
				passed[ch.name] = true
				fmt.Fprintf(os.Stderr, "  ✓ %s\n", ch.name)
				continue
			}
			pending = append(pending, fmt.Sprintf("%s: %v", ch.name, err))
		}
		if len(pending) == 0 {
			fmt.Fprintln(os.Stdout, "clrk dev ready")
			return nil
		}
		// Progress output once per second-ish (every 4 rounds at the
		// default 250ms interval; once per round at 1s+). Flushes the
		// "what is it stuck on" question without spamming.
		if round == 1 || round%max(1, int(time.Second/interval)) == 0 {
			for _, p := range pending {
				fmt.Fprintf(os.Stderr, "  … %s\n", p)
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for clrk dev ready; pending: %s", strings.Join(pending, "; "))
		case <-time.After(interval):
		}
	}
}
