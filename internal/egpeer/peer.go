// Package egpeer maps a gRPC peer (the calling Envoy pod) back to the
// EgressGateway it belongs to. Both the certprovider and extproc gRPC
// services need this — neither receives the EG identity inside the
// request payload, but both are dialed by Envoy proxies whose pods carry
// owning-gateway labels.
//
// Lookup chain: peer source IP → Pod in envoy-gateway-system whose
// status.podIP matches → owning-gateway labels → EgressGateway name
// (`clrk-eg-<name>` per the controller's naming convention, with the
// prefix stripped). Falls back to "the only EgressGateway" in the
// cluster when the pod-IP lookup misses, which covers `clrk dev` where
// the controller-manager talks to k3s through a docker network NAT and
// the peer IP is no longer the pod IP.
package egpeer

import (
	"context"
	"fmt"
	"net"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"google.golang.org/grpc/peer"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	clrkcontroller "github.com/apoxy-dev/clrk/internal/controller"
)

// Labels EG sets on the proxy Pods/Services it provisions for each
// Gateway.
const (
	labelOwningGatewayName      = "gateway.envoyproxy.io/owning-gateway-name"
	labelOwningGatewayNamespace = "gateway.envoyproxy.io/owning-gateway-namespace"
)

// envoyGatewayPodNamespace is where Envoy Gateway provisions data-plane
// pods. Standard install path; matches the constant used by the
// EgressGateway controller.
const envoyGatewayPodNamespace = "envoy-gateway-system"

// EGFromContext resolves the EgressGateway identity for the gRPC peer on
// ctx. See package doc for the lookup chain.
func EGFromContext(ctx context.Context, c client.Client) (types.NamespacedName, error) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return types.NamespacedName{}, fmt.Errorf("no gRPC peer info on context")
	}
	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return types.NamespacedName{}, fmt.Errorf("parse peer addr %q: %w", p.Addr.String(), err)
	}

	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(envoyGatewayPodNamespace)); err != nil {
		return types.NamespacedName{}, fmt.Errorf("list pods in %s: %w", envoyGatewayPodNamespace, err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.PodIP != host {
			continue
		}
		gwName := pod.Labels[labelOwningGatewayName]
		gwNs := pod.Labels[labelOwningGatewayNamespace]
		if gwName == "" || gwNs == "" {
			return types.NamespacedName{}, fmt.Errorf("pod %s/%s missing owning-gateway labels",
				pod.Namespace, pod.Name)
		}
		egName := strings.TrimPrefix(gwName, clrkcontroller.GatewayNamePrefix)
		if egName == gwName {
			return types.NamespacedName{}, fmt.Errorf(
				"gateway name %q does not start with clrk EG prefix %q",
				gwName, clrkcontroller.GatewayNamePrefix)
		}
		return types.NamespacedName{Namespace: gwNs, Name: egName}, nil
	}

	var egs clrkv1alpha1.EgressGatewayList
	if err := c.List(ctx, &egs); err != nil {
		return types.NamespacedName{}, fmt.Errorf("list EgressGateways: %w", err)
	}
	if len(egs.Items) == 1 {
		eg := egs.Items[0]
		return types.NamespacedName{Namespace: eg.Namespace, Name: eg.Name}, nil
	}
	return types.NamespacedName{}, fmt.Errorf("no pod in %s matches peer IP %s and %d EgressGateways exist (need 1 for dev fallback)",
		envoyGatewayPodNamespace, host, len(egs.Items))
}
