package install

import (
	"fmt"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// LoadRESTConfig resolves a *rest.Config for a customer cluster from an
// operator-supplied kubeconfig + context. Precedence for the kubeconfig file:
// explicit kubeconfigPath, else the KUBECONFIG env var, else ~/.kube/config —
// the standard client-go loading rules. contextName, when set, overrides the
// kubeconfig's current-context. Returns the config and the resolved context
// name (so the installer can show the operator exactly which cluster it is
// about to touch).
func LoadRESTConfig(kubeconfigPath, contextName string) (cfg *rest.Config, resolvedContext string, err error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		rules.ExplicitPath = kubeconfigPath
	}
	overrides := &clientcmd.ConfigOverrides{}
	if contextName != "" {
		overrides.CurrentContext = contextName
	}
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides)

	cfg, err = cc.ClientConfig()
	if err != nil {
		return nil, "", fmt.Errorf("loading kubeconfig (path=%q context=%q): %w", kubeconfigPath, contextName, err)
	}

	resolvedContext = contextName
	if resolvedContext == "" {
		raw, rerr := cc.RawConfig()
		if rerr != nil {
			return nil, "", fmt.Errorf("reading kubeconfig current-context: %w", rerr)
		}
		resolvedContext = raw.CurrentContext
	}
	return cfg, resolvedContext, nil
}
