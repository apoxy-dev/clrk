package install

import (
	"context"
	"fmt"
	"net"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/apoxy-dev/clrk/internal/ports"
)

// controllerManagerNetworkPolicy restricts ingress to the cm pod. The aggregated
// API (cm:8443) is unauthenticated in v1 (--insecure-allow-public), so it is
// admitted only from the host-network CIDRs the aggregation proxy can originate
// from (kube-apiserver Endpoints + node InternalIPs); cluster Pods live on the
// pod network and are walled off. The in-cluster data-plane ports (gRPC,
// ext_proc, OTLP, NATS, EG xDS) and the health/admin ports are left open (empty
// From) so worker Pods, the Envoy data plane, and the kubelet's probes keep
// reaching the cm; a NetworkPolicy that selects a pod is default-deny for any
// port it omits, so every real port is enumerated here.
//
// Returns nil when no CIDRs are known (dev, --network-policy=false, or
// undetected) so the caller emits no policy rather than a half-protective one
// that could silently wall off the cm. Listing 8443 only under the CIDR rule
// (and never under the open rule) is what locks the unauthenticated surface
// down.
func controllerManagerNetworkPolicy(p Profile) *networkingv1.NetworkPolicy {
	if len(p.APIServerCIDRs) == 0 {
		return nil
	}
	tcp := corev1.ProtocolTCP
	port := func(n int32) networkingv1.NetworkPolicyPort {
		v := intstr.FromInt(int(n))
		return networkingv1.NetworkPolicyPort{Protocol: &tcp, Port: &v}
	}
	peers := make([]networkingv1.NetworkPolicyPeer, 0, len(p.APIServerCIDRs))
	for _, cidr := range p.APIServerCIDRs {
		peers = append(peers, networkingv1.NetworkPolicyPeer{IPBlock: &networkingv1.IPBlock{CIDR: cidr}})
	}
	return &networkingv1.NetworkPolicy{
		TypeMeta: metav1.TypeMeta{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      ControllerManagerName,
			Namespace: p.Namespace,
			Labels:    cmLabels,
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: cmLabels},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{
					// The aggregation proxy (kube-apiserver, host network)
					// reaching the unauthenticated aggregated API.
					From:  peers,
					Ports: []networkingv1.NetworkPolicyPort{port(8443)},
				},
				{
					// In-cluster data plane + node-originated probes (empty From
					// = any source). The aggregated-API port (8443) is
					// deliberately absent here.
					Ports: []networkingv1.NetworkPolicyPort{
						port(9443),  // grpc (worker Invocation lifecycle)
						port(9444),  // ext_proc (Envoy)
						port(4318),  // OTLP
						port(8082),  // health (kubelet probes)
						port(8085),  // admin
						port(18000), // EG xDS
						port(ports.NATSClientPort),
					},
				},
			},
		},
	}
}

// DeriveAPIServerCIDRs collects the host-network source CIDRs the aggregation
// proxy can originate from: the kube-apiserver Endpoint addresses (default/
// kubernetes) plus every node InternalIP, each as a single-host CIDR (/32 or
// /128). Aggregation requests reach the cm SNAT'd to one of these, so admitting
// them keeps the API reachable while excluding the pod network. Returns the
// (deduplicated) CIDRs; an error only if neither source can be read.
func DeriveAPIServerCIDRs(ctx context.Context, c client.Client) ([]string, error) {
	seen := map[string]struct{}{}
	var out []string
	add := func(ip string) {
		cidr := hostCIDR(ip)
		if cidr == "" {
			return
		}
		if _, ok := seen[cidr]; ok {
			return
		}
		seen[cidr] = struct{}{}
		out = append(out, cidr)
	}

	var eps corev1.Endpoints
	epErr := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "kubernetes"}, &eps)
	if epErr == nil {
		for _, ss := range eps.Subsets {
			for _, a := range ss.Addresses {
				add(a.IP)
			}
		}
	}

	var nodes corev1.NodeList
	nodeErr := c.List(ctx, &nodes)
	if nodeErr == nil {
		for _, n := range nodes.Items {
			for _, a := range n.Status.Addresses {
				if a.Type == corev1.NodeInternalIP {
					add(a.Address)
				}
			}
		}
	}

	if len(out) == 0 {
		if epErr != nil {
			return nil, fmt.Errorf("reading default/kubernetes endpoints: %w", epErr)
		}
		if nodeErr != nil {
			return nil, fmt.Errorf("listing nodes: %w", nodeErr)
		}
		return nil, fmt.Errorf("no apiserver/node IPs found to derive a NetworkPolicy CIDR")
	}
	return out, nil
}

// hostCIDR turns a single IP into its single-host CIDR (/32 for IPv4, /128 for
// IPv6). Returns "" for an unparseable address.
func hostCIDR(ip string) string {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return ""
	}
	if parsed.To4() != nil {
		return ip + "/32"
	}
	return ip + "/128"
}
