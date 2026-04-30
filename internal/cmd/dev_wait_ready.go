package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
//   - envoy-gateway-system/envoy-gateway Secret exists (certgen ran;
//     EG xDS server has the cert it needs to serve data-plane Envoys)
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
			_, err := core.Discovery().ServerVersion()
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
			_, err := core.CoreV1().Secrets("envoy-gateway-system").Get(c, "envoy-gateway", metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return errors.New("envoy-gateway-system/envoy-gateway not found yet")
			}
			return err
		}},
	}

	var lastErr error
	for {
		lastErr = nil
		for _, ch := range checks {
			rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := ch.fn(rctx)
			cancel()
			if err != nil {
				lastErr = fmt.Errorf("%s: %w", ch.name, err)
				break
			}
		}
		if lastErr == nil {
			fmt.Fprintln(os.Stdout, "clrk dev ready")
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for clrk dev ready: %w", lastErr)
		case <-time.After(interval):
		}
	}
}
