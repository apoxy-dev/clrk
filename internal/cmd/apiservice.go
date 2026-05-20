package cmd

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"

	"github.com/apoxy-dev/clrk/internal/drivers"
)

// bootstrapClrkAPIService registers the clrk apiserver as an aggregated
// extension apiserver in the k3s control plane. It writes three objects:
//
//   - kube-system/clrk-apiserver Service (selector-less, so Endpoints
//     isn't auto-populated from Pods).
//   - kube-system/clrk-apiserver Endpoints pointing at the
//     controller-manager container's IP on the shared docker network.
//   - v1alpha1.clrk.apoxy.dev APIService, marking the clrk apiserver as
//     the backend for clrk.apoxy.dev/v1alpha1 with insecureSkipTLSVerify
//     set — the embedded apiserver uses an in-memory self-signed cert
//     whose CA isn't exposed, so dev skips verification.
//
// Idempotent; safe to call on every `clrk dev` invocation.
func bootstrapClrkAPIService(ctx context.Context, cluster *drivers.ClusterDriver, backendIP string, backendPort int) error {
	port := int32(backendPort)
	svc := &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{Name: "clrk-apiserver", Namespace: "kube-system"},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				Name:       "https",
				Port:       port,
				Protocol:   corev1.ProtocolTCP,
				TargetPort: intstr.FromInt32(port),
			}},
		},
	}
	eps := &corev1.Endpoints{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Endpoints"},
		ObjectMeta: metav1.ObjectMeta{Name: "clrk-apiserver", Namespace: "kube-system"},
		Subsets: []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{{IP: backendIP}},
			Ports: []corev1.EndpointPort{{
				Name:     "https",
				Port:     port,
				Protocol: corev1.ProtocolTCP,
			}},
		}},
	}
	apiSvc := &apiregistrationv1.APIService{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apiregistration.k8s.io/v1", Kind: "APIService"},
		ObjectMeta: metav1.ObjectMeta{Name: "v1alpha1.clrk.apoxy.dev"},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:                "clrk.apoxy.dev",
			Version:              "v1alpha1",
			GroupPriorityMinimum: 1000,
			VersionPriority:      15,
			InsecureSkipTLSVerify: true,
			Service: &apiregistrationv1.ServiceReference{
				Name:      "clrk-apiserver",
				Namespace: "kube-system",
				Port:      &port,
			},
		},
	}
	return cluster.ApplyObjects(ctx, svc, eps, apiSvc)
}
