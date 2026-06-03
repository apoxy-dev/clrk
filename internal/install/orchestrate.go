package install

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"

	"github.com/apoxy-dev/clrk/internal/crds"
)

// Step is one unit of the install/upgrade bring-up. Why is a one-line rationale
// rendered before Run executes, so the operator always understands what is
// about to happen and why. Risky marks steps that mutate cluster-wide or
// existing state (gated behind confirmation by the caller).
type Step struct {
	Name  string
	Why   string
	Risky bool
	Run   func(ctx context.Context) error
}

// StepLogger receives lifecycle events for a step. status is one of "start",
// "done", "error". Callers route this to stdout or a TUI pane.
type StepLogger func(step Step, status string, err error)

// RunSteps executes steps in order, emitting lifecycle events. It stops on the
// first error (the control plane is a dependency chain — a failed cm makes the
// WorkerPool apply meaningless).
func RunSteps(ctx context.Context, steps []Step, log StepLogger) error {
	for _, s := range steps {
		if log != nil {
			log(s, "start", nil)
		}
		if err := s.Run(ctx); err != nil {
			if log != nil {
				log(s, "error", err)
			}
			return fmt.Errorf("%s: %w", s.Name, err)
		}
		if log != nil {
			log(s, "done", nil)
		}
	}
	return nil
}

// InstallSteps builds the ordered bring-up for a customer-cluster install:
// namespaces -> CRDs -> controller-manager -> wait cm Available -> wait clrk API
// discoverable -> WorkerPool -> readiness gate. The cm must be Available and its
// aggregated API discoverable before the WorkerPool apply, which goes through
// that API. crdMode controls Gateway-API/EG CRD handling (if-missing on install,
// always on upgrade). waitTimeout caps each wait.
func InstallSteps(applier Applier, cfg *rest.Config, p Profile, crdMode crds.Mode, waitTimeout, readyInterval time.Duration, log func(string)) []Step {
	return []Step{
		{
			Name:  "namespaces",
			Why:   fmt.Sprintf("create the control-plane namespace %q and worker namespace %q", p.Namespace, p.WorkerNamespace),
			Risky: false,
			Run: func(ctx context.Context) error {
				if err := applier.EnsureNamespace(ctx, p.Namespace); err != nil {
					return err
				}
				if p.WorkerNamespace != p.Namespace {
					if err := applier.EnsureNamespace(ctx, p.WorkerNamespace); err != nil {
						return err
					}
				}
				return nil
			},
		},
		{
			Name:  "crds",
			Why:   "install the Gateway-API + Envoy-Gateway CRDs the controller-manager reconciles",
			Risky: crdMode == crds.ModeAlways,
			Run: func(ctx context.Context) error {
				return crds.Install(ctx, cfg, crds.InstallOptions{Mode: crdMode})
			},
		},
		{
			Name:  "controller-manager",
			Why:   "apply the controller-manager (apiserver + embedded kine/ClickHouse/NATS + Envoy-Gateway) and its aggregated APIService",
			Risky: true,
			Run: func(ctx context.Context) error {
				if err := ApplyControllerManager(ctx, applier, p); err != nil {
					return err
				}
				if log != nil {
					log("controller-manager applied; waiting for it to become Available")
				}
				return applier.WaitDeploymentAvailable(ctx, p.Namespace, ControllerManagerName, waitTimeout)
			},
		},
		{
			Name:  "aggregated-api",
			Why:   "wait for clrk.apoxy.dev/v1alpha1 to be served through the aggregation layer (the WorkerPool apply needs it)",
			Risky: false,
			Run: func(ctx context.Context) error {
				return WaitAPIDiscoverable(ctx, cfg, waitTimeout)
			},
		},
		{
			Name:  "worker-pool",
			Why:   "apply the default WorkerPool; the controller-manager reconciles it into the worker Deployment",
			Risky: true,
			Run: func(ctx context.Context) error {
				return ApplyWorkerPool(ctx, applier, p)
			},
		},
		{
			Name:  "readiness",
			Why:   "verify the API, Invocation store, Gateway CRDs, EG cert, and both Deployments are green",
			Risky: false,
			Run: func(ctx context.Context) error {
				return WaitReady(ctx, cfg, p, readyInterval, log)
			},
		},
	}
}

// WaitAPIDiscoverable polls discovery until clrk.apoxy.dev/v1alpha1 appears. The
// aggregated APIService is registered as soon as the cm is up, but
// kube-aggregator still has to probe the backend's TLS and mark the service
// Available before REST mappings for clrk kinds resolve.
func WaitAPIDiscoverable(ctx context.Context, cfg *rest.Config, timeout time.Duration) error {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return fmt.Errorf("discovery client: %w", err)
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		groups, err := dc.ServerGroups()
		if err == nil {
			for _, g := range groups.Groups {
				if g.Name == "clrk.apoxy.dev" {
					for _, v := range g.Versions {
						if v.Version == "v1alpha1" {
							return nil
						}
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("timed out waiting for clrk.apoxy.dev/v1alpha1 in discovery")
		case <-time.After(500 * time.Millisecond):
		}
	}
}
