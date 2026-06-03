package install

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiextclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	apiregv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	apiregclient "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset/typed/apiregistration/v1"
)

// readyPerCheckTimeout caps a single probe round so a hung HTTPS dial (apiserver
// mid-restart, cm crash-looping) spins the loop instead of blocking.
const readyPerCheckTimeout = 5 * time.Second

// WorkerDeploymentName is the Deployment WorkerPoolDeploymentReconciler creates
// for the `default` WorkerPool.
const WorkerDeploymentName = "default-workers"

// WaitReady blocks until every post-install signal on the target cluster has
// flipped green, or ctx is cancelled. Adapted from `clrk dev wait-ready`:
// drops the k3s-specific /livez probe and adds the controller-manager + worker
// Deployment Available checks the dev path did inline. log receives one line
// per state transition (green tick or pending) so the caller can route it to
// stdout or a TUI pane.
//
// Signals (all required, each monotonic once green):
//   - clrk.apoxy.dev/v1alpha1 APIService is Available
//   - the ClickHouse-backed Invocation store answers a List
//   - Gateway API CRDs are installed
//   - <namespace>/envoy-gateway Secret exists (EG certgen ran)
//   - controller-manager Deployment is Available
//   - worker Deployment is Available
func WaitReady(ctx context.Context, cfg *rest.Config, p Profile, interval time.Duration, log func(string)) error {
	probe := *cfg
	probe.Timeout = readyPerCheckTimeout
	core, err := kubernetes.NewForConfig(&probe)
	if err != nil {
		return fmt.Errorf("kubernetes client: %w", err)
	}
	apireg, err := apiregclient.NewForConfig(&probe)
	if err != nil {
		return fmt.Errorf("apiregistration client: %w", err)
	}
	apiext, err := apiextclient.NewForConfig(&probe)
	if err != nil {
		return fmt.Errorf("apiextensions client: %w", err)
	}

	checks := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"clrk.apoxy.dev APIService Available", func(c context.Context) error {
			as, err := apireg.APIServices().Get(c, APIServiceName, metav1.GetOptions{})
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
		{"ClickHouse-backed Invocation store responsive", func(c context.Context) error {
			_, err := core.Discovery().RESTClient().Get().
				AbsPath("/apis/clrk.apoxy.dev/v1alpha1/invocations").
				Param("limit", "1").
				DoRaw(c)
			return err
		}},
		{"Gateway API CRDs installed", func(c context.Context) error {
			_, err := apiext.CustomResourceDefinitions().Get(c, "gateways.gateway.networking.k8s.io", metav1.GetOptions{})
			return err
		}},
		{"Envoy Gateway xDS Secret materialized", func(c context.Context) error {
			_, err := core.CoreV1().Secrets(p.Namespace).Get(c, EnvoyGatewayServiceName, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("%s/%s not found yet", p.Namespace, EnvoyGatewayServiceName)
			}
			return err
		}},
		{"controller-manager Deployment Available", func(c context.Context) error {
			return deploymentAvailable(c, core, p.Namespace, ControllerManagerName)
		}},
		{"worker Deployment Available", func(c context.Context) error {
			return deploymentAvailable(c, core, p.WorkerNamespace, WorkerDeploymentName)
		}},
	}

	passed := make(map[string]bool, len(checks))
	round := 0
	for {
		round++
		var pending []string
		for _, ch := range checks {
			if passed[ch.name] {
				continue
			}
			rctx, cancel := context.WithTimeout(ctx, readyPerCheckTimeout)
			err := ch.fn(rctx)
			cancel()
			if err == nil {
				passed[ch.name] = true
				if log != nil {
					log("ready: " + ch.name)
				}
				continue
			}
			pending = append(pending, fmt.Sprintf("%s: %v", ch.name, err))
		}
		if len(pending) == 0 {
			return nil
		}
		if log != nil && (round == 1 || round%max(1, int(time.Second/interval)) == 0) {
			for _, pnd := range pending {
				log("waiting: " + pnd)
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out; pending: %s", strings.Join(pending, "; "))
		case <-time.After(interval):
		}
	}
}

func deploymentAvailable(ctx context.Context, core kubernetes.Interface, ns, name string) error {
	dep, err := core.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	for _, cond := range dep.Status.Conditions {
		if cond.Type == appsv1.DeploymentAvailable && cond.Status == "True" {
			return nil
		}
	}
	return fmt.Errorf("%s/%s not Available yet", ns, name)
}
