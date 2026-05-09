package egcontrolplane

import (
	"fmt"
	"os"
)

// renderConfig produces the EnvoyGateway YAML the child reads via
// --config-path. Leader election is disabled: one supervised child per
// pod means the lease buys nothing and would need pre-created RBAC.
//
// extensionApis.enableBackend=true is required because the
// per-TaskAgent HTTPRoute backendRef is an EG `Backend{type:
// DynamicResolver}` (not a core Service). Without the toggle, EG
// flags the route as ResolvedRefs=False / UnsupportedValue and the
// data plane returns 500 on every request.
//
// proxyTopologyInjector.disabled=true turns OFF the only webhook EG
// would otherwise stand up. EG ≥ v1.4 ships a mutating admission
// webhook ("envoy-gateway-topology-injector") that injects topology
// spread constraints into Envoy data-plane Pods at create time. clrk
// doesn't use it (we own the per-TA EnvoyProxy spec end-to-end and
// scale data planes via the per-TA Gateway, not via spread hints), and
// without this flag EG's webhook server tries to bind :9443 — the
// same port clrk's control-plane gRPC server already owns.
//
// Disabling here is half the story; see runCertgen in supervisor.go
// for the matching --disable-topology-injector certgen flag (skips
// the webhook caBundle patch step, which would otherwise error out
// because clrk never installed the MutatingWebhookConfiguration).
//
// Net result: NO EG-managed admission webhooks exist in the cluster.
// See apoxy-cloud//docs/clrk-envoy-gateway.md for why and what we
// give up by skipping the topology injector.
func renderConfig(cfg Config) string {
	return fmt.Sprintf(`apiVersion: gateway.envoyproxy.io/v1alpha1
kind: EnvoyGateway
gateway:
  controllerName: gateway.envoyproxy.io/gatewayclass-controller
provider:
  type: Kubernetes
  kubernetes:
    leaderElection:
      disable: true
    proxyTopologyInjector:
      disabled: true
extensionApis:
  enableBackend: true
extensionManager:
  hooks:
    xdsTranslator:
      post:
      - HTTPListener
      - Translation
  service:
    host: %s
    port: %d
`, cfg.ExtensionHost, cfg.ExtensionPort)
}

// writeConfig writes renderConfig to a tempfile and returns its path.
// Caller is responsible for deleting it on shutdown.
func writeConfig(cfg Config) (string, error) {
	f, err := os.CreateTemp("", "clrk-eg-config-*.yaml")
	if err != nil {
		return "", fmt.Errorf("creating EG config tempfile: %w", err)
	}
	defer f.Close()
	if _, err := f.WriteString(renderConfig(cfg)); err != nil {
		_ = os.Remove(f.Name())
		return "", fmt.Errorf("writing EG config: %w", err)
	}
	return f.Name(), nil
}
