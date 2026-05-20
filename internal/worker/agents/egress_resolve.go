package agents

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	clrkcontroller "github.com/apoxy-dev/clrk/internal/controller"
	"github.com/apoxy-dev/clrk/internal/egress"
)

// EgressBundle is the full set of egress prerequisites for one
// sandbox: MITM CA PEM (for trust injection into the rootfs), the
// per-listener backend addresses to dial, and the SandboxPolicy
// handle compiled from EgressGateway.DefaultPolicy + matching
// EgressL4Routes.
type EgressBundle struct {
	CAPEM    []byte
	Backends []egress.BackendListener
	Policy   *egress.SandboxPolicy
}

// ResolveEgress loads the EG MITM CA, listener backends, and per-
// sandbox policy for refs. Returns a zero EgressBundle (nil error)
// when refs is empty — the agent has no EgressRefs, no MITM, direct
// dial. Returns an error if the EG exists but isn't ready yet; the
// caller maps that to a retryable status (the dispatcher's 503).
//
// Used by the dispatcher (cold path, one-shot resolve) and the
// warm pool fill loop. The daemon lifecycle's wait-for-ready loop
// uses ResolveEgressNoCA + LoadEgressCA separately so it can
// distinguish CA-missing from backends-not-yet-up across the poll.
func ResolveEgress(ctx context.Context, c client.Client, router *egress.Router, namespace string, refs []clrkv1alpha1.AgentEgressRef) (EgressBundle, error) {
	if len(refs) == 0 {
		return EgressBundle{}, nil
	}
	egName := refs[0].GatewayRef

	caPEM, err := fetchEgressCA(ctx, c, namespace, egName)
	if err != nil {
		return EgressBundle{}, err
	}
	backends, policy, err := resolveEgressBackendsAndPolicy(ctx, c, router, namespace, egName)
	if err != nil {
		return EgressBundle{}, err
	}
	return EgressBundle{CAPEM: caPEM, Backends: backends, Policy: policy}, nil
}

// ResolveEgressNoCA is ResolveEgress minus the CA fetch. Used by
// the daemon lifecycle so its polling loop can run a separate
// LoadEgressCA + ResolveEgressNoCA pair, distinguishing the two
// readiness gates rather than collapsing them into one composite
// error.
//
// Empty refs returns (nil, nil, nil) — agent has no MITM gateway.
func ResolveEgressNoCA(ctx context.Context, c client.Client, router *egress.Router, namespace string, refs []clrkv1alpha1.AgentEgressRef) ([]egress.BackendListener, *egress.SandboxPolicy, error) {
	if len(refs) == 0 {
		return nil, nil, nil
	}
	return resolveEgressBackendsAndPolicy(ctx, c, router, namespace, refs[0].GatewayRef)
}

// LoadEgressCA returns the MITM CA PEM for the first EgressRef.
// Empty refs returns (nil, nil) — agent has nothing to MITM, so
// the sandbox is created without trust injection.
func LoadEgressCA(ctx context.Context, c client.Client, namespace string, refs []clrkv1alpha1.AgentEgressRef) ([]byte, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	return fetchEgressCA(ctx, c, namespace, refs[0].GatewayRef)
}

// fetchEgressCA fetches the named EG's MITM CA Secret in namespace
// and returns its TLSCertKey value. Returns an error if the secret
// is missing or its TLSCertKey is empty — the caller treats both as
// EG-not-yet-ready and either retries (daemon) or 503s (dispatcher).
func fetchEgressCA(ctx context.Context, c client.Client, namespace, egName string) ([]byte, error) {
	var sec corev1.Secret
	key := types.NamespacedName{Namespace: namespace, Name: clrkcontroller.EgressGatewayCASecretName(egName)}
	if err := c.Get(ctx, key, &sec); err != nil {
		return nil, fmt.Errorf("fetching EgressGateway CA secret %s: %w", key, err)
	}
	caPEM := sec.Data[corev1.TLSCertKey]
	if len(caPEM) == 0 {
		return nil, fmt.Errorf("EgressGateway CA secret %s has empty %s", key, corev1.TLSCertKey)
	}
	return caPEM, nil
}

// resolveEgressBackendsAndPolicy is the EG-status-to-backends +
// L4Routes-to-policy half of ResolveEgress, split out so the daemon
// path's polling loop can reuse it without re-fetching the CA.
//
// Backends come from EgressGateway.Status.Listeners (published by
// the EG controller once the Envoy-Gateway-managed Service is
// provisioned); policy is compiled from the EG's DefaultPolicy plus
// every EgressL4Route in the namespace targeting it. The handle
// returned by SyncPolicy is stable: the ConfigWatcher hot-swaps the
// underlying policy on later CRD changes, so existing sandboxes
// pick up route/policy edits without restart.
func resolveEgressBackendsAndPolicy(ctx context.Context, c client.Client, router *egress.Router, namespace, egName string) ([]egress.BackendListener, *egress.SandboxPolicy, error) {
	var eg clrkv1alpha1.EgressGateway
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: egName}, &eg); err != nil {
		return nil, nil, fmt.Errorf("get EgressGateway %s/%s: %w", namespace, egName, err)
	}

	specByName := make(map[string]clrkv1alpha1.EgressListener, len(eg.Spec.Listeners))
	for _, el := range eg.Spec.Listeners {
		specByName[el.Name] = el
	}
	backends := make([]egress.BackendListener, 0, len(eg.Status.Listeners))
	for _, ls := range eg.Status.Listeners {
		if ls.BackendAddress == "" {
			continue
		}
		spec, ok := specByName[ls.Name]
		if !ok {
			continue
		}
		shape, err := clrkv1alpha1.ShapeForListener(spec)
		if err != nil {
			continue
		}
		var matchPort int32
		if spec.Port != nil {
			matchPort = *spec.Port
		}
		backends = append(backends, egress.BackendListener{
			Name:      ls.Name,
			Addr:      ls.BackendAddress,
			Shape:     string(shape),
			MatchPort: matchPort,
			Priority:  clrkv1alpha1.ShapePriority(shape),
		})
	}
	if len(backends) == 0 {
		return nil, nil, fmt.Errorf("EgressGateway %s/%s has no listener backends ready", namespace, egName)
	}

	var l4Routes clrkv1alpha1.EgressL4RouteList
	if err := c.List(ctx, &l4Routes, client.InNamespace(namespace)); err != nil {
		return nil, nil, fmt.Errorf("list EgressL4Routes in %s: %w", namespace, err)
	}
	policy := router.SyncPolicy(eg, l4Routes.Items)

	return backends, policy, nil
}
