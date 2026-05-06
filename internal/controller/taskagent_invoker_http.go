package controller

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// HTTPInvoker invokes a TaskAgent by POSTing to the in-cluster Gateway URL
// that the TaskAgentIngressReconciler materializes for it. Used by the cron
// firer in production; tests inject a fake.
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
	addr, err := h.resolveGatewayAddress(ctx, ta)
	if err != nil {
		return fmt.Errorf("resolving Gateway address: %w", err)
	}

	url := fmt.Sprintf("http://%s/", addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Clrk-Trigger", "cron")

	resp, err := h.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()
	// Drain so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 500 {
		return fmt.Errorf("POST %s returned %d", url, resp.StatusCode)
	}
	return nil
}

// resolveGatewayAddress reads the Gateway named after ta and picks the first
// usable Status.Addresses entry. The Gateway is created by
// TaskAgentIngressReconciler with name=ta.Name in ta.Namespace; Envoy
// Gateway populates Status.Addresses with the LB / ClusterIP / hostname
// once the listener is programmed.
func (h *HTTPInvoker) resolveGatewayAddress(ctx context.Context, ta *clrkv1alpha1.TaskAgent) (string, error) {
	var gw gwapiv1.Gateway
	key := types.NamespacedName{Namespace: ta.Namespace, Name: ta.Name}
	// Tight retry: Gateway.Status.Addresses can lag program-time by a few
	// seconds after a fresh apply. Bound the total wait to the request
	// context — the caller already enforces the per-fire timeout.
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := h.Client.Get(ctx, key, &gw)
		if err == nil {
			if addr, ok := pickAddress(gw.Status.Addresses, gatewayFirstListenerPort(&gw)); ok {
				return addr, nil
			}
		} else if !apierrors.IsNotFound(err) {
			return "", err
		}
		if time.Now().After(deadline) {
			if err != nil {
				return "", fmt.Errorf("Gateway %s not found within deadline: %w", key, err)
			}
			return "", fmt.Errorf("Gateway %s has no usable Status.Addresses", key)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// pickAddress returns the first Hostname or IPAddress entry from addrs and
// appends :port. Returns ok=false if no usable address was found.
func pickAddress(addrs []gwapiv1.GatewayStatusAddress, port int32) (string, bool) {
	for _, a := range addrs {
		if a.Value == "" {
			continue
		}
		if a.Type == nil {
			return fmt.Sprintf("%s:%d", a.Value, port), true
		}
		switch *a.Type {
		case gwapiv1.IPAddressType, gwapiv1.HostnameAddressType:
			return fmt.Sprintf("%s:%d", a.Value, port), true
		}
	}
	return "", false
}

// gatewayFirstListenerPort returns the port of gw's first listener,
// defaulting to 80 when none is set (TaskAgentIngressReconciler always
// creates one).
func gatewayFirstListenerPort(gw *gwapiv1.Gateway) int32 {
	for _, l := range gw.Spec.Listeners {
		return int32(l.Port)
	}
	return 80
}

// Compile-time assertion: HTTPInvoker satisfies TaskAgentInvoker.
var _ TaskAgentInvoker = (*HTTPInvoker)(nil)
