package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/apoxy-dev/clrk/internal/install"
)

// This file is the single owner of how the clrk CLI picks a cluster and a
// namespace. clrk targets your standard kubeconfig the same way kubectl does;
// the only kubeconfig clrk keeps on disk is the `clrk dev` session's, reached
// with --local. There is no separate clrk context store. Commands never read
// kube's fields or re-derive the namespace themselves — they go through the
// helpers below so the resolution lives in exactly one place.

// kube holds the global cluster-targeting flags (--kubeconfig/--context/--local),
// registered once as persistent flags on RootCmd (registerKubeFlags) and shared by
// every command that resolves a cluster.
var kube kubeFlags

// kubeFlags is the cluster-targeting trio: an explicit kubeconfig path, a context
// override, and the `clrk dev` --local shortcut.
type kubeFlags struct {
	kubeconfig string
	context    string
	local      bool
}

// devKubeconfigFile is the dev cluster's host-network kubeconfig under --clrk-dir,
// written by `clrk dev` and read by --local. It is the only kubeconfig clrk keeps
// on disk; every other invocation targets the operator's standard kubeconfig.
const devKubeconfigFile = "kubeconfig.host"

// registerKubeFlags installs the global cluster-targeting flags as persistent
// flags on cmd (RootCmd), so every subcommand resolves a cluster the same
// kubectl-compatible way. Called once from root.go.
func registerKubeFlags(cmd *cobra.Command) {
	pf := cmd.PersistentFlags()
	pf.StringVar(&kube.kubeconfig, "kubeconfig", "", "Path to the kubeconfig (default: $KUBECONFIG, then ~/.kube/config).")
	pf.StringVar(&kube.context, "context", "", "Kubeconfig context to target (default: the kubeconfig's current-context).")
	pf.BoolVar(&kube.local, "local", false, "Target the running 'clrk dev' session's kubeconfig (~/.clrk/kubeconfig.host).")
}

// path returns the explicit kubeconfig file to load, or "" to use standard loading
// ($KUBECONFIG, then ~/.kube/config). --kubeconfig wins over --local.
func (f kubeFlags) path() (string, error) {
	if f.kubeconfig != "" {
		return f.kubeconfig, nil
	}
	if f.local {
		// --local resolves the dev kubeconfig under --clrk-dir. A `clrk dev` started
		// with a custom --data-dir (distinct from --clrk-dir) writes its kubeconfig
		// there instead; --local can't see it, so point the operator at --kubeconfig.
		p := filepath.Join(clrkDir, devKubeconfigFile)
		if _, err := os.Stat(p); err != nil {
			return "", fmt.Errorf("--local kubeconfig %s not found (is `clrk dev` running? for a custom --data-dir, pass --kubeconfig <data-dir>/kubeconfig.host): %w", p, err)
		}
		return p, nil
	}
	return "", nil
}

// clientConfig resolves the kubeconfig + context the whole CLI shares as a
// clientcmd.ClientConfig (which yields both a *rest.Config and the target namespace
// from one resolution). Resolution order:
//
//  1. --kubeconfig <file>  — that file; --context selects within it.
//  2. --local             — the running `clrk dev` host kubeconfig under --clrk-dir.
//  3. standard kubeconfig — $KUBECONFIG, then ~/.kube/config, honoring the
//     kubeconfig's own current-context (or --context). This is kubectl's own
//     resolution; clrk keeps no separate context store.
//
// Used directly by the manifest/secret apply paths (which want the ClientConfig);
// the live-client helpers below build on it.
func (f kubeFlags) clientConfig() (clientcmd.ClientConfig, error) {
	path, err := f.path()
	if err != nil {
		return nil, err
	}
	overrides := &clientcmd.ConfigOverrides{}
	if f.context != "" {
		overrides.CurrentContext = f.context
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if path != "" {
		rules.ExplicitPath = path
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides), nil
}

// liveConfig resolves the global flags into a live *rest.Config and the
// ClientConfig it came from (kept so the namespace can be derived from the same
// resolution). The shared base for restConfig and clients, so the kubeconfig +
// context selection happens in exactly one place.
func (f kubeFlags) liveConfig() (*rest.Config, clientcmd.ClientConfig, error) {
	cc, err := f.clientConfig()
	if err != nil {
		return nil, nil, err
	}
	cfg, err := restFromClientConfig(cc)
	if err != nil {
		return nil, nil, err
	}
	return cfg, cc, nil
}

// restConfig resolves the global flags into a live *rest.Config plus the namespace
// to operate in: the explicit -n value when set, else the selected context's
// namespace (or "default"), exactly like kubectl. This is the single point where
// the -n fallback is decided for every cluster command.
func (f kubeFlags) restConfig(explicitNS string) (*rest.Config, string, error) {
	cfg, cc, err := f.liveConfig()
	if err != nil {
		return nil, "", err
	}
	ns, err := namespaceOrContext(cc, explicitNS)
	if err != nil {
		return nil, "", err
	}
	return cfg, ns, nil
}

// clients resolves the global flags into the trio the read/stream commands need: a
// *rest.Config (for the clrk REST client), a dynamic client (to resolve the parent
// kind), and the namespace to operate in (explicit -n, else the context default).
// With allNamespaces it returns an empty namespace and skips namespace resolution
// entirely — `-A` lists across every namespace, so the context's namespace is
// irrelevant and need not even resolve.
func (f kubeFlags) clients(explicitNS string, allNamespaces bool) (*rest.Config, dynamic.Interface, string, error) {
	cfg, cc, err := f.liveConfig()
	if err != nil {
		return nil, nil, "", err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, nil, "", fmt.Errorf("dynamic client: %w", err)
	}
	if allNamespaces {
		return cfg, dyn, "", nil
	}
	ns, err := namespaceOrContext(cc, explicitNS)
	if err != nil {
		return nil, nil, "", err
	}
	return cfg, dyn, ns, nil
}

// remoteCluster bridges the global flags to the installer's cluster handle for
// `clrk install`/`clrk upgrade`: the explicit/standard kubeconfig path plus
// --context, or the dev session via --local. The one path from the CLI flags into
// the install package's cluster handle.
func (f kubeFlags) remoteCluster() (*install.RemoteCluster, error) {
	path, err := f.path()
	if err != nil {
		return nil, err
	}
	return install.NewRemoteCluster(path, f.context)
}

// targetingHint returns the lines telling the operator how to reach a freshly
// installed control plane, mirroring the exact targeting flags they used so the
// hint reproduces the targeting. It is the one place outside this file that needs
// to branch on which targeting flag won, so it lives here with the flags.
func (f kubeFlags) targetingHint(namespace, contextName string) []string {
	switch {
	case f.local:
		// The dev session is reached only via --local; its context lives in
		// ~/.clrk/kubeconfig.host, not the standard kubeconfig.
		return []string{fmt.Sprintf("Target it with --local, e.g. `clrk pools list --local -n %s`.", namespace)}
	case f.kubeconfig != "":
		// The context lives in the explicit --kubeconfig file, which may not be the
		// standard kubeconfig, so reproduce the exact per-command targeting.
		return []string{fmt.Sprintf(
			"Target it per command: clrk pools list --kubeconfig %s --context %s -n %s",
			f.kubeconfig, contextName, namespace)}
	case contextName != "":
		// Standard kubeconfig: the kubectl-native default works.
		return []string{
			fmt.Sprintf("Target it by default: kubectl config use-context %s && kubectl config set-context --current --namespace %s", contextName, namespace),
			fmt.Sprintf("or per command: clrk pools list --context %s -n %s", contextName, namespace),
		}
	default:
		return nil
	}
}

// clientConfigForPath builds a ClientConfig that loads exactly path with no
// overrides — for callers (clrk dev) that already hold a concrete kubeconfig path.
func clientConfigForPath(path string) clientcmd.ClientConfig {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = path
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
}

// namespaceOrContext returns explicit when non-empty, else the ClientConfig's
// context namespace (or "default"). The single place the -n fallback is decided,
// shared by restConfig (every read/stream command) and `clrk secret`.
func namespaceOrContext(cc clientcmd.ClientConfig, explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	ns, _, err := cc.Namespace()
	if err != nil {
		// `clrk secret set` resolves the namespace before it builds a live client,
		// so an empty config surfaces here first; translate it to the actionable
		// hint instead of clientcmd's opaque "no configuration has been provided".
		if clientcmd.IsEmptyConfig(err) {
			return "", errNoKubeconfig
		}
		return "", err
	}
	return ns, nil
}

// errNoKubeconfig is the actionable message shown when nothing resolves a cluster —
// replacing clientcmd's opaque "no configuration has been provided".
var errNoKubeconfig = fmt.Errorf("no cluster configured: pass --kubeconfig/--context, set $KUBECONFIG, or run `clrk dev` and add --local; `clrk install` targets your standard kubeconfig")

// restFromClientConfig resolves a *rest.Config, translating clientcmd's empty-config
// error into the actionable errNoKubeconfig hint. Shared by every path that turns a
// ClientConfig into a live client (restConfig, applyManifests, applySecretSpecs).
func restFromClientConfig(cc clientcmd.ClientConfig) (*rest.Config, error) {
	cfg, err := cc.ClientConfig()
	if err != nil {
		if clientcmd.IsEmptyConfig(err) {
			return nil, errNoKubeconfig
		}
		return nil, err
	}
	return cfg, nil
}
