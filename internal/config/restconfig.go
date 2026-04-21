package config

import (
	"fmt"
	"os"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
)

// Environment variables understood by FromEnv.
const (
	EnvAPIServerURL      = "CLRK_APISERVER_URL"
	EnvAPIServerCAFile   = "CLRK_APISERVER_CA"
	EnvAPIServerToken    = "CLRK_APISERVER_TOKEN"
	EnvAPIServerInsecure = "CLRK_APISERVER_INSECURE"
	EnvKubeconfig        = "KUBECONFIG"
)

// FromEnv builds a *rest.Config from CLRK_APISERVER_* environment variables.
// When CLRK_APISERVER_URL is unset, it falls back to the standard
// controller-runtime discovery (KUBECONFIG / in-cluster / $HOME/.kube/config).
func FromEnv() (*rest.Config, error) {
	url := os.Getenv(EnvAPIServerURL)
	if url == "" {
		return ctrl.GetConfig()
	}

	cfg := &rest.Config{Host: url}

	if ca := os.Getenv(EnvAPIServerCAFile); ca != "" {
		data, err := os.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("reading CA file %q: %w", ca, err)
		}
		cfg.TLSClientConfig.CAData = data
	}
	if os.Getenv(EnvAPIServerInsecure) == "true" {
		cfg.TLSClientConfig.Insecure = true
		cfg.TLSClientConfig.CAData = nil
	}
	if token := os.Getenv(EnvAPIServerToken); token != "" {
		cfg.BearerToken = token
	}

	return cfg, nil
}

// FromKubeconfig builds a *rest.Config from an explicit kubeconfig path.
func FromKubeconfig(path string) (*rest.Config, error) {
	return clientcmd.BuildConfigFromFlags("", path)
}
