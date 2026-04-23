package controller

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// revisionName is the deterministic name for an AgentSandboxRevision derived
// from an agent. The 5-digit zero-padded generation gives a stable lex order
// that's also human-grokkable in `kubectl get`.
func revisionName(agent string, generation int64) string {
	return fmt.Sprintf("%s-%05d", agent, generation)
}

// validateEgressRefs Gets each EgressGateway in refs from namespace and
// returns the names of any that don't exist. A non-nil err means a Get
// failed for a reason other than NotFound — caller should propagate.
func validateEgressRefs(
	ctx context.Context,
	c client.Client,
	namespace string,
	refs []clrkv1alpha1.AgentEgressRef,
) (missing []string, err error) {
	for _, ref := range refs {
		var egw clrkv1alpha1.EgressGateway
		key := types.NamespacedName{Name: ref.GatewayRef, Namespace: namespace}
		if err := c.Get(ctx, key, &egw); err != nil {
			if apierrors.IsNotFound(err) {
				missing = append(missing, ref.GatewayRef)
				continue
			}
			return nil, fmt.Errorf("looking up EgressGateway %q: %w", ref.GatewayRef, err)
		}
	}
	return missing, nil
}
