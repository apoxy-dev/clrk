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
