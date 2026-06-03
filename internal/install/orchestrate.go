package install

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

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

// Orchestration carries everything the install/upgrade bring-up needs. Bundled
// in a struct (rather than a long parameter list) because the serving-cert step
// is conditional and the cluster path threads cert objects + an async wait
// through it. The TUI derives its sidebar from Steps(), so a conditional step is
// reflected in the rendered component list without a parallel hardcoded slice.
type Orchestration struct {
	Applier Applier
	Config  *rest.Config
	Profile Profile
	CRDMode crds.Mode

	// CertObjects are the serving-cert objects (cert-manager Issuer+Certificate,
	// or self-signed CA+serving Secret) applied before the cm so its --cert-dir
	// mount and the APIService caBundle are satisfied. Empty for the insecure
	// (dev-equivalent) posture, which skips the serving-cert step.
	CertObjects []client.Object
	// WaitCertSecret waits for the serving-cert Secret to be populated after
	// applying CertObjects. Needed on the cert-manager path (the Secret is minted
	// asynchronously); the self-signed path applies the Secret directly.
	WaitCertSecret bool

	WaitTimeout   time.Duration
	ReadyInterval time.Duration
	Log           func(string)
}

// Steps builds the ordered bring-up: namespaces -> [serving-cert] -> CRDs ->
// controller-manager (apply + wait Available) -> aggregated-api (wait
// discoverable) -> WorkerPool -> readiness. The cm must be Available and its
// aggregated API discoverable before the WorkerPool apply, which goes through
// that API. The serving-cert step is present only when CertObjects is non-empty.
func (o Orchestration) Steps() []Step {
	p := o.Profile
	steps := []Step{{
		Name:  "namespaces",
		Why:   fmt.Sprintf("create the control-plane namespace %q and worker namespace %q", p.Namespace, p.WorkerNamespace),
		Risky: false,
		Run: func(ctx context.Context) error {
			// The worker namespace hosts privileged gVisor/runsc pods, so it must
			// allow them: stamp the privileged Pod Security labels rather than let
			// a freshly created namespace inherit a restrictive cluster-wide PSA
			// default and silently reject the workers at admission. When the worker
			// and control-plane namespaces are the same, that one namespace carries
			// the labels (it hosts the workers too).
			if p.WorkerNamespace == p.Namespace {
				return o.Applier.ApplyObjects(ctx, workerNamespace(p.Namespace))
			}
			if err := o.Applier.EnsureNamespace(ctx, p.Namespace); err != nil {
				return err
			}
			return o.Applier.ApplyObjects(ctx, workerNamespace(p.WorkerNamespace))
		},
	}}

	if len(o.CertObjects) > 0 {
		steps = append(steps, Step{
			Name:  "serving-cert",
			Why:   certStepWhy(p.TLS),
			Risky: false,
			Run: func(ctx context.Context) error {
				if err := o.Applier.ApplyObjects(ctx, o.CertObjects...); err != nil {
					return err
				}
				if o.WaitCertSecret {
					if o.Log != nil {
						o.Log("waiting for cert-manager to issue the serving certificate")
					}
					return WaitServingSecret(ctx, o.Applier, p.Namespace, o.WaitTimeout)
				}
				return nil
			},
		})
	}

	steps = append(steps,
		Step{
			Name:  "crds",
			Why:   "install the Gateway-API + Envoy-Gateway CRDs the controller-manager reconciles",
			Risky: o.CRDMode == crds.ModeAlways,
			Run: func(ctx context.Context) error {
				return crds.Install(ctx, o.Config, crds.InstallOptions{Mode: o.CRDMode})
			},
		},
		Step{
			Name:  "controller-manager",
			Why:   "apply the controller-manager (apiserver + embedded kine/ClickHouse/NATS + Envoy-Gateway) and its aggregated APIService",
			Risky: true,
			Run: func(ctx context.Context) error {
				if err := ApplyControllerManager(ctx, o.Applier, p); err != nil {
					return err
				}
				if o.Log != nil {
					o.Log("controller-manager applied; waiting for it to become Available")
				}
				return o.Applier.WaitDeploymentAvailable(ctx, p.Namespace, ControllerManagerName, o.WaitTimeout)
			},
		},
		Step{
			Name:  "aggregated-api",
			Why:   "wait for clrk.apoxy.dev/v1alpha1 to be served through the aggregation layer (the WorkerPool apply needs it)",
			Risky: false,
			Run: func(ctx context.Context) error {
				return WaitAPIDiscoverable(ctx, o.Config, o.WaitTimeout)
			},
		},
		Step{
			Name:  "worker-pool",
			Why:   "apply the default WorkerPool; the controller-manager reconciles it into the worker Deployment",
			Risky: true,
			Run: func(ctx context.Context) error {
				return ApplyWorkerPool(ctx, o.Applier, p)
			},
		},
		Step{
			Name:  "readiness",
			Why:   "verify the API, Invocation store, Gateway CRDs, EG cert, and both Deployments are green",
			Risky: false,
			Run: func(ctx context.Context) error {
				// Bound the readiness gate by the same per-wait timeout the other
				// wait steps use; WaitReady itself only honors ctx, so without this
				// a signal that never goes green would hang the install forever.
				ctx, cancel := context.WithTimeout(ctx, o.WaitTimeout)
				defer cancel()
				return WaitReady(ctx, o.Config, p, o.ReadyInterval, o.Log)
			},
		},
	)
	return steps
}

// StepNames returns the ordered Step.Name values, for seeding the TUI sidebar so
// it matches the steps actually run (including the conditional serving-cert).
func StepNames(steps []Step) []string {
	names := make([]string, len(steps))
	for i, s := range steps {
		names[i] = s.Name
	}
	return names
}

// workerNamespace builds the worker Namespace with privileged Pod Security
// labels. Applied via SSA so it both creates the namespace and idempotently
// keeps the labels on re-run; the labels opt the namespace into running the
// privileged worker pods regardless of the cluster's default PSA level.
func workerNamespace(ns string) *corev1.Namespace {
	return &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
			Labels: map[string]string{
				"pod-security.kubernetes.io/enforce": "privileged",
				"pod-security.kubernetes.io/audit":   "privileged",
				"pod-security.kubernetes.io/warn":    "privileged",
			},
		},
	}
}

// certStepWhy explains the serving-cert step for the chosen TLS mode.
func certStepWhy(mode TLSMode) string {
	switch mode {
	case TLSCertManager:
		return "create the cert-manager Issuer + Certificate; cert-manager issues the serving cert and injects the CA into the APIService"
	case TLSSelfSigned:
		return "apply the installer-minted CA + serving certificate the apiserver presents (APIService caBundle pinned to the CA)"
	default:
		return "apply the apiserver serving certificate"
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
