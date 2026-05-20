package controller

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	discoveryv1 "k8s.io/api/discovery/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/ports"
)

// HTTPInvoker invokes a TaskAgent by POSTing directly to the WorkerPool's
// in-cluster dispatch Service. Reads the Service's EndpointSlices and
// dials a ready pod IP rather than the Service DNS — the
// controller-manager runs outside the cluster in `clrk dev` and so
// can't resolve `*.svc.cluster.local`, but the docker bridge / pod IPs
// in EndpointSlices are routable in both modes (production: shared pod
// CIDR; dev: shared docker network via the selectorless
// Service+EndpointSlice bridge that `clrk dev` materializes for the
// default WorkerPool).
type HTTPInvoker struct {
	Client client.Client
	HTTP   *http.Client
}

// NewHTTPInvoker returns an HTTPInvoker with a default http.Client. The
// per-call deadline is supplied by the caller's context, so the client
// itself has no Timeout set.
func NewHTTPInvoker(c client.Client) *HTTPInvoker {
	return &HTTPInvoker{
		Client: c,
		HTTP:   &http.Client{},
	}
}

func (h *HTTPInvoker) Invoke(ctx context.Context, ta *clrkv1alpha1.TaskAgent, body []byte) error {
	addr, err := h.dispatchAddress(ctx, ta)
	if err != nil {
		return fmt.Errorf("resolving dispatch address: %w", err)
	}
	url := fmt.Sprintf("http://%s/", addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(ports.HeaderTrigger, "cron")
	req.Header.Set(ports.HeaderTaskAgent, ta.Namespace+"/"+ta.Name)

	resp, err := h.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 500 {
		return fmt.Errorf("POST %s returned %d", url, resp.StatusCode)
	}
	return nil
}

// dispatchAddress lists the EndpointSlices for the WorkerPool's
// dispatch Service and returns the first ready address with the
// `dispatch` port. WorkerPoolDeploymentReconciler creates the
// `{wp}-workers` Service (with a `dispatch` port) selecting worker
// pods; the dev-mode bridge
// (`drivers/cluster_ctlptl.go::ApplyDefaultWorkerPoolBridge`) materializes
// a selectorless Service+EndpointSlice with the same name, pointing
// at worker docker IPs — same shape, same lookup.
func (h *HTTPInvoker) dispatchAddress(ctx context.Context, ta *clrkv1alpha1.TaskAgent) (string, error) {
	svcName := ta.Spec.WorkerPoolRef + "-workers"
	var slices discoveryv1.EndpointSliceList
	if err := h.Client.List(ctx, &slices,
		client.InNamespace(ta.Namespace),
		client.MatchingLabels{discoveryv1.LabelServiceName: svcName},
	); err != nil {
		return "", fmt.Errorf("list EndpointSlices for %s/%s: %w", ta.Namespace, svcName, err)
	}
	for i := range slices.Items {
		s := &slices.Items[i]
		var port int32
		for _, p := range s.Ports {
			if p.Name != nil && *p.Name == "dispatch" && p.Port != nil {
				port = *p.Port
				break
			}
		}
		if port == 0 {
			continue
		}
		for _, ep := range s.Endpoints {
			// Skip endpoints whose Conditions explicitly mark them
			// not-ready. Nil Ready means "unknown" per the API; treat
			// that as ready (matches kube-proxy's behavior).
			if ep.Conditions.Ready != nil && !*ep.Conditions.Ready {
				continue
			}
			for _, ip := range ep.Addresses {
				if ip == "" {
					continue
				}
				return fmt.Sprintf("%s:%d", ip, port), nil
			}
		}
	}
	return "", fmt.Errorf("no ready dispatch endpoint for Service %s/%s", ta.Namespace, svcName)
}

// Compile-time assertion: HTTPInvoker satisfies TaskAgentInvoker.
var _ TaskAgentInvoker = (*HTTPInvoker)(nil)
