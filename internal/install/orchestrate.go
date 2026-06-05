package install

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
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

	// Upgrade marks an in-place upgrade (vs a fresh install). It makes the
	// worker-pool step force a WorkerPool rollout and wait for convergence, so
	// the workers reliably pick up a new image even when only the image digest
	// (not the tag) moved. `clrk install` leaves it false.
	Upgrade bool

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
			// Apply the same namespace set the YAML render emits (see
			// namespaceObjects): the worker namespace carries privileged Pod
			// Security labels so the gVisor/runsc pods aren't rejected by a
			// restrictive cluster-wide PSA default, and a distinct control-plane
			// namespace is created plain.
			return o.Applier.ApplyObjects(ctx, namespaceObjects(p)...)
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
			Why:   controllerManagerStepWhy(o.Upgrade),
			Risky: true,
			Run: func(ctx context.Context) error {
				if err := ApplyControllerManager(ctx, o.Applier, p); err != nil {
					return err
				}
				// A typed client for pod-level diagnostics: both waits below run
				// under runWithPodDiagnostics so a stuck container (bad image,
				// config error, crash loop) is surfaced live AND fails fast,
				// instead of hanging the cm wait out to the full --timeout in
				// silence. An unschedulable pod is surfaced live but left to the
				// timeout (it can clear as the cluster scales).
				core, err := kubernetes.NewForConfig(o.Config)
				if err != nil {
					return fmt.Errorf("building kubernetes client: %w", err)
				}
				if !o.Upgrade {
					if o.Log != nil {
						o.Log("controller-manager applied; waiting for it to become Available")
					}
					return runWithPodDiagnostics(ctx, core, p.Namespace, ControllerManagerName, o.Log,
						func(c context.Context) error {
							return o.Applier.WaitDeploymentAvailable(c, p.Namespace, ControllerManagerName, o.WaitTimeout)
						})
				}
				// On upgrade, force a roll and wait for the Recreate to complete.
				// The version stamp lives on the cm Deployment's OWN metadata
				// (annotation + version label), deliberately NOT on the pod template,
				// so a same-tag/moved-digest image (or a stamp-only bump) is no
				// pod-template change and the Deployment controller wouldn't roll the
				// cm — leaving the OLD binary running while the recorded version says
				// otherwise. The same reasoning drives the worker-pool roll below.
				cl, err := o.Applier.KubeClient(ctx)
				if err != nil {
					return err
				}
				gen, err := Rollout(ctx, cl, p.Namespace, ControllerManagerName)
				if err != nil {
					return err
				}
				if o.Log != nil {
					o.Log("rolled the controller-manager (Recreate); waiting for the new pod to become Available")
				}
				return runWithPodDiagnostics(ctx, core, p.Namespace, ControllerManagerName, o.Log,
					func(c context.Context) error {
						return WaitDeploymentRolledOut(c, cl, p.Namespace, ControllerManagerName, gen, o.WaitTimeout)
					})
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
			Why:   workerPoolStepWhy(o.Upgrade),
			Risky: true,
			Run: func(ctx context.Context) error {
				if err := ApplyWorkerPool(ctx, o.Applier, p); err != nil {
					return err
				}
				if !o.Upgrade {
					return nil
				}
				// On upgrade, force a roll and wait for the WorkerPool to converge
				// at the new generation: a same-tag/moved-digest image is no spec
				// change to SSA, so without an explicit restartedAt bump the workers
				// would keep running the old image.
				cl, err := o.Applier.KubeClient(ctx)
				if err != nil {
					return err
				}
				gen, err := RolloutWorkerPool(ctx, cl, p.WorkerNamespace, DefaultWorkerPoolName)
				if err != nil {
					return err
				}
				if o.Log != nil {
					o.Log("rolled the default WorkerPool; waiting for the workers to converge")
				}
				return WaitWorkerPoolConverged(ctx, cl, p.WorkerNamespace, DefaultWorkerPoolName, gen, o.WaitTimeout)
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

// namespaceObjects returns the namespace objects the bring-up applies, the
// single source of truth shared by the orchestration's namespaces step and the
// YAML render so the two never drift. The worker namespace carries the
// privileged Pod Security labels (the gVisor/runsc pods need them); when the
// control-plane namespace is distinct it is created plain. When both are the
// same namespace, the one labeled namespace covers both roles.
func namespaceObjects(p Profile) []client.Object {
	if p.WorkerNamespace == p.Namespace {
		return []client.Object{workerNamespace(p.Namespace)}
	}
	return []client.Object{
		controlPlaneNamespace(p.Namespace),
		workerNamespace(p.WorkerNamespace),
	}
}

// controlPlaneNamespace builds the plain control-plane Namespace (no PSA labels;
// it holds the cm, not the privileged workers). Matches the object
// Applier.EnsureNamespace applies on the live path.
func controlPlaneNamespace(ns string) *corev1.Namespace {
	return &corev1.Namespace{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	}
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

// controllerManagerStepWhy explains the controller-manager step, noting the
// forced Recreate roll on upgrade (the version stamp doesn't change the pod
// template, so a same-tag image needs an explicit roll to pick up new code).
func controllerManagerStepWhy(upgrade bool) string {
	if upgrade {
		return "apply the controller-manager, roll it (Recreate — brief aggregated-API downtime), and wait for the new pod to become Available"
	}
	return "apply the controller-manager (apiserver + embedded kine/ClickHouse/NATS + Envoy-Gateway) and its aggregated APIService"
}

// workerPoolStepWhy explains the worker-pool step, noting the forced roll on
// upgrade.
func workerPoolStepWhy(upgrade bool) string {
	if upgrade {
		return "re-apply the default WorkerPool, roll it, and wait for the workers to converge on the new image"
	}
	return "apply the default WorkerPool; the controller-manager reconciles it into the worker Deployment"
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
