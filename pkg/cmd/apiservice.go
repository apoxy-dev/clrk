package cmd

import (
	"context"
	"fmt"

	"github.com/apoxy-dev/clrk/pkg/drivers"
)

// bootstrapClrkAPIService registers the clrk apiserver as an aggregated
// extension apiserver in the k3s control plane running at k3sContainer.
// It writes three objects:
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
func bootstrapClrkAPIService(ctx context.Context, k3s *drivers.K3sDriver, backendIP string, backendPort int) error {
	yaml := fmt.Sprintf(`---
apiVersion: v1
kind: Service
metadata:
  name: clrk-apiserver
  namespace: kube-system
spec:
  ports:
  - name: https
    port: %[2]d
    protocol: TCP
    targetPort: %[2]d
---
apiVersion: v1
kind: Endpoints
metadata:
  name: clrk-apiserver
  namespace: kube-system
subsets:
- addresses:
  - ip: %[1]s
  ports:
  - name: https
    port: %[2]d
    protocol: TCP
---
apiVersion: apiregistration.k8s.io/v1
kind: APIService
metadata:
  name: v1alpha1.clrk.apoxy.dev
spec:
  group: clrk.apoxy.dev
  version: v1alpha1
  groupPriorityMinimum: 1000
  versionPriority: 15
  insecureSkipTLSVerify: true
  service:
    name: clrk-apiserver
    namespace: kube-system
    port: %[2]d
`, backendIP, backendPort)

	return k3s.KubectlApply(ctx, "-", []byte(yaml))
}
