package drivers

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/go-connections/nat"
	k3dclient "github.com/k3d-io/k3d/v5/pkg/client"
	k3dconfig "github.com/k3d-io/k3d/v5/pkg/config"
	k3dconfigtypes "github.com/k3d-io/k3d/v5/pkg/config/types"
	"github.com/k3d-io/k3d/v5/pkg/config/v1alpha5"
	"github.com/k3d-io/k3d/v5/pkg/runtimes"
	k3d "github.com/k3d-io/k3d/v5/pkg/types"
	k3dutil "github.com/k3d-io/k3d/v5/pkg/util"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// RegistryDataVolume backs the local registry's /var/lib/registry so pushed
// images survive `clrk dev` stop/start cycles. Attached via k3d's
// Registry.Volumes field at RegistryRun time.
const (
	RegistryDataVolume = "clrk-registry-data"
	RegistryDataMount  = "/var/lib/registry"
)

const (
	// ClusterName is the k3d cluster identifier. Distinct from
	// NetworkName ("clrk") and the in-cluster "clrk" namespace so the
	// three layers aren't confusable in `docker ps` or `kubectl` output.
	ClusterName = "clrk-dev"

	// RegistryName is the docker container name for the local OCI
	// registry on the shared bridge. In-cluster pods resolve it as
	// `clrk-registry:5000` via docker DNS.
	RegistryName = "clrk-registry"

	// DefaultK3sImage pins a minor version whose k8s.io/api release matches
	// clrk and apoxy-cloud's module resolution (v0.34.x). Override via
	// ClusterDriver.K3sImage for CI pinning.
	DefaultK3sImage = "rancher/k3s:v1.34.1-k3s1"

	// DefaultRegistryImage is the OCI distribution image k3d boots when
	// we hand it a Registry with Image=="" — we pin explicitly so a k3d
	// upgrade can't silently swap the binary.
	DefaultRegistryImage = "docker.io/library/registry:2"

	// KubeconfigFileName is written into the shared data dir so other
	// drivers (and the user) can find the rewritten in-network kubeconfig.
	KubeconfigFileName = "kubeconfig"
)

// ClusterServerContainerName is the docker container name of the
// single-server node, sourced from k3d's own naming helper so any
// future change to k3d's prefix scheme is picked up automatically.
var ClusterServerContainerName = k3dclient.GenerateNodeName(ClusterName, k3d.ServerRole, 0)

// ClusterDriver brings up a k3d cluster (and optionally a colocated local
// OCI registry) by calling k3d v5 as a Go library — no `k3d` binary on
// PATH. Both attach to the shared `clrk` docker network (EnsureNetwork).
// When EnableRegistry is true, pods reach the registry as
// `clrk-registry:5000` and the host pushes to
// `localhost:<RegistryHostPort()>`.
type ClusterDriver struct {
	// DataDir is the host path where rewritten kubeconfigs are written.
	// Must be the same path mounted into the cm/worker drivers so they
	// can read the in-network kubeconfig.
	DataDir string
	// K3sImage overrides the k3s image k3d boots. Empty = DefaultK3sImage.
	K3sImage string
	// EnableRegistry brings up the local OCI registry container alongside
	// the cluster and wires it into k3d's containerd registries.yaml. Only
	// useful for the inner dev loop where you `clrk dev push-image`
	// locally-built images; leave false when pulling published refs.
	EnableRegistry bool
	// RegistryPort is the host port the local registry publishes. Zero
	// means auto-pick a free port; the actual port is available via
	// RegistryHostPort() after Start. Ignored when EnableRegistry is false.
	RegistryPort int

	// Populated by Start; safe to read once Start returns nil.
	registryHostPort int
	apiHostPort      int

	// kubeClient is built lazily from HostKubeconfigPath() the first
	// time we apply objects.
	kubeOnce   sync.Once
	kubeClient client.Client
	kubeErr    error
}

// NewClusterDriver constructs a driver with sane defaults. dataDir is
// required because the kubeconfig has to land somewhere other drivers
// can see. k3sImage may be empty to accept the default.
func NewClusterDriver(dataDir, k3sImage string, registryPort int) *ClusterDriver {
	return &ClusterDriver{
		DataDir:      dataDir,
		K3sImage:     k3sImage,
		RegistryPort: registryPort,
	}
}

// Start brings up the k3d cluster (and the registry when EnableRegistry
// is set) on the shared `clrk` network. Idempotent: if a cluster/registry
// with our names already exists (left over from a previous `clrk dev`
// that wasn't stopped), we reuse it instead of failing.
func (d *ClusterDriver) Start(ctx context.Context) error {
	if err := EnsureNetwork(ctx); err != nil {
		return err
	}

	rt := runtimes.SelectedRuntime

	if d.EnableRegistry {
		regHostPort, err := d.ensureRegistry(ctx, rt)
		if err != nil {
			return fmt.Errorf("registry: %w", err)
		}
		d.registryHostPort = regHostPort
	}

	apiPort, err := d.ensureCluster(ctx, rt)
	if err != nil {
		return fmt.Errorf("cluster: %w", err)
	}
	d.apiHostPort = apiPort

	if err := d.writeKubeconfigs(ctx, rt); err != nil {
		return fmt.Errorf("writing kubeconfigs: %w", err)
	}
	return nil
}

// ensureRegistry creates the local registry container if it doesn't
// exist yet, with the named data volume mounted at /var/lib/registry.
// Returns the host port it's published on.
func (d *ClusterDriver) ensureRegistry(ctx context.Context, rt runtimes.Runtime) (int, error) {
	if reg, err := k3dclient.RegistryGet(ctx, rt, RegistryName); err == nil && reg != nil {
		raw := reg.ExposureOpts.Binding.HostPort
		port, perr := strconv.Atoi(raw)
		if perr != nil {
			return 0, fmt.Errorf("registry %q exists but its host port %q is unparseable: %w", RegistryName, raw, perr)
		}
		if port == 0 {
			return 0, fmt.Errorf("registry %q exists but reports host port 0", RegistryName)
		}
		return port, nil
	}

	hostPort := d.RegistryPort
	if hostPort == 0 {
		p, err := k3dutil.GetFreePort()
		if err != nil {
			return 0, fmt.Errorf("picking registry host port: %w", err)
		}
		hostPort = p
	}

	reg := &k3d.Registry{
		Host:    RegistryName,
		Image:   DefaultRegistryImage,
		Network: NetworkName,
		Volumes: []string{RegistryDataVolume + ":" + RegistryDataMount},
		ExposureOpts: k3d.ExposureOpts{
			Host: RegistryName,
			PortMapping: nat.PortMapping{
				Port: nat.Port(k3d.DefaultRegistryPort + "/tcp"),
				Binding: nat.PortBinding{
					HostIP:   "0.0.0.0",
					HostPort: strconv.Itoa(hostPort),
				},
			},
		},
	}
	if _, err := k3dclient.RegistryRun(ctx, rt, reg); err != nil {
		return 0, fmt.Errorf("running registry: %w", err)
	}
	return hostPort, nil
}

// ensureCluster creates the k3d cluster (single server, no loadbalancer)
// if absent. Returns the host port the kube API is published on.
func (d *ClusterDriver) ensureCluster(ctx context.Context, rt runtimes.Runtime) (int, error) {
	if existing, err := k3dclient.ClusterGet(ctx, rt, &k3d.Cluster{Name: ClusterName}); err == nil && existing != nil {
		// With DisableLoadbalancer the kube API host port lives on the
		// server node itself. KubeAPI.Binding is populated when ClusterGet
		// reconstructs from container metadata.
		if existing.KubeAPI != nil && existing.KubeAPI.Binding.HostPort != "" {
			port, perr := strconv.Atoi(existing.KubeAPI.Binding.HostPort)
			if perr == nil && port > 0 {
				return port, nil
			}
		}
		return 0, fmt.Errorf("cluster %q exists but kube API host port is unreadable", ClusterName)
	}

	apiPort, err := k3dutil.GetFreePort()
	if err != nil {
		return 0, fmt.Errorf("picking kube API host port: %w", err)
	}

	k3sImg := d.K3sImage
	if k3sImg == "" {
		k3sImg = DefaultK3sImage
	}

	simple := v1alpha5.SimpleConfig{
		TypeMeta: k3dconfigtypes.TypeMeta{
			APIVersion: "k3d.io/v1alpha5",
			Kind:       "Simple",
		},
		ObjectMeta: k3dconfigtypes.ObjectMeta{Name: ClusterName},
		Servers:    1,
		Agents:     0,
		Image:      k3sImg,
		Network:    NetworkName,
		ExposeAPI: v1alpha5.SimpleExposureOpts{
			Host:     "0.0.0.0",
			HostIP:   "0.0.0.0",
			HostPort: strconv.Itoa(apiPort),
		},
		Options: v1alpha5.SimpleConfigOptions{
			K3dOptions: v1alpha5.SimpleConfigOptionsK3d{
				NoRollback:          true,
				DisableLoadbalancer: true,
				// Block ClusterRun until the apiserver is reachable so
				// downstream bootstrap doesn't race kubeconfig validity.
				Wait:    true,
				Timeout: 3 * time.Minute,
			},
			K3sOptions: v1alpha5.SimpleConfigOptionsK3s{
				ExtraArgs: serverK3sArgs(),
			},
		},
	}
	if d.EnableRegistry {
		simple.Registries.Use = []string{RegistryName}
	}

	clusterCfg, err := k3dconfig.TransformSimpleToClusterConfig(ctx, rt, simple, "")
	if err != nil {
		return 0, fmt.Errorf("transforming simple config: %w", err)
	}
	if err := k3dclient.ClusterRun(ctx, rt, clusterCfg); err != nil {
		return 0, fmt.Errorf("running cluster: %w", err)
	}
	return apiPort, nil
}

// serverK3sArgs returns the k3s server flags. NodeFilters scope each
// flag to the single server node — k3d would reject unfiltered args.
func serverK3sArgs() []v1alpha5.K3sArgWithNodeFilters {
	flags := []string{
		"--disable=traefik",
		"--disable=servicelb",
		"--disable=metrics-server",
		// Dual-stack so envoy pods can reach AAAA-only or AAAA-preferred
		// upstreams. Pairs with EnsureNetwork's dual-stack bridge.
		"--cluster-cidr=10.42.0.0/16,fd00:42::/56",
		"--service-cidr=10.43.0.0/16,fd00:43::/112",
		"--flannel-ipv6-masq",
		// Add the server node's docker-DNS name to the apiserver's
		// serving cert so the in-network kubeconfig (which connects via
		// `k3d-clrk-dev-server-0:6443`) verifies without InsecureSkipTLSVerify.
		"--tls-san=" + ClusterServerContainerName,
		"--tls-san=localhost",
	}
	out := make([]v1alpha5.K3sArgWithNodeFilters, len(flags))
	for i, f := range flags {
		out[i] = v1alpha5.K3sArgWithNodeFilters{Arg: f, NodeFilters: []string{"server:*"}}
	}
	return out
}

// Stop deletes the k3d cluster and the registry. Idempotent: a missing
// cluster or registry is not an error.
func (d *ClusterDriver) Stop(ctx context.Context) error {
	rt := runtimes.SelectedRuntime

	var firstErr error
	if cl, err := k3dclient.ClusterGet(ctx, rt, &k3d.Cluster{Name: ClusterName}); err == nil && cl != nil {
		if delErr := k3dclient.ClusterDelete(ctx, rt, cl, k3d.ClusterDeleteOpts{SkipRegistryCheck: true}); delErr != nil {
			firstErr = fmt.Errorf("deleting cluster: %w", delErr)
		}
	}
	if reg, err := k3dclient.RegistryGet(ctx, rt, RegistryName); err == nil && reg != nil {
		// k3d represents the registry as a Node externally; delete via NodeDelete.
		regNode := &k3d.Node{Name: RegistryName, Role: k3d.RegistryRole}
		if delErr := k3dclient.NodeDelete(ctx, rt, regNode, k3d.NodeDeleteOpts{SkipLBUpdate: true}); delErr != nil && firstErr == nil {
			firstErr = fmt.Errorf("deleting registry: %w", delErr)
		}
	}
	return firstErr
}

// Reset is Stop plus the shared docker network. Used by the drift-gate
// recreate path in `clrk dev`, which has to evict any container still
// pinning the `clrk` bridge (otherwise the next EnsureNetwork-recreate
// fails). The network is left in place by Stop because a normal
// clrk-dev shutdown doesn't need to disturb it.
func (d *ClusterDriver) Reset(ctx context.Context) error {
	if err := d.Stop(ctx); err != nil {
		return err
	}
	// Best-effort: removing the network can fail if a container we
	// don't manage is still attached. The caller (runDev) follows up
	// with EnsureNetwork which already handles a stale-network case.
	_ = removeDockerNetwork(ctx, NetworkName)
	return nil
}

// KubeconfigPath is the host path to the kubeconfig with the server URL
// rewritten to the in-network k3d node hostname. Bind-mount into cm /
// worker containers so they reach k3s via the shared docker network.
func (d *ClusterDriver) KubeconfigPath() string {
	return filepath.Join(d.DataDir, KubeconfigFileName)
}

// HostKubeconfigPath is the host path to the kubeconfig pointing at the
// kube API's host-published port. Use this for `kubectl` from the host,
// integration tests, and `clrk dev wait-ready`.
func (d *ClusterDriver) HostKubeconfigPath() string {
	return filepath.Join(d.DataDir, "kubeconfig.host")
}

// NodeContainerName is k3d's deterministic name for the server node
// container. Used by dev_status / dev_logs to tail the right container.
func (d *ClusterDriver) NodeContainerName() string { return ClusterServerContainerName }

// NodeHostname is the in-network DNS name for the apiserver. Same as
// NodeContainerName because docker resolves container names as DNS
// labels on the shared bridge network.
func (d *ClusterDriver) NodeHostname() string { return ClusterServerContainerName }

// RegistryHostPort returns the host port the local registry is published
// on. Zero before Start succeeds.
func (d *ClusterDriver) RegistryHostPort() int { return d.registryHostPort }

// writeKubeconfigs materializes two kubeconfig files under DataDir from
// k3d's in-memory clientcmd config (no merge with ~/.kube/config):
//
//   - kubeconfig: server URL rewritten to the server container's docker
//     DNS name. Bind-mounted into the cm / worker containers.
//   - kubeconfig.host: server URL pointing at localhost:<apiHostPort> so
//     host-side kubectl / wait-ready can reach the apiserver.
//
// Both rely on the apiserver cert covering NodeContainerName and
// localhost (--tls-san in serverK3sArgs), so neither file needs
// InsecureSkipTLSVerify.
func (d *ClusterDriver) writeKubeconfigs(ctx context.Context, rt runtimes.Runtime) error {
	if err := os.MkdirAll(d.DataDir, 0o755); err != nil {
		return err
	}
	cfg, err := k3dclient.KubeconfigGet(ctx, rt, &k3d.Cluster{Name: ClusterName})
	if err != nil {
		return fmt.Errorf("getting kubeconfig from k3d: %w", err)
	}
	ctxName := cfg.CurrentContext
	if ctxName == "" {
		return errors.New("k3d returned an empty kubeconfig")
	}

	if err := writeMinified(cfg, ctxName, d.HostKubeconfigPath(), func(c *clientcmdapi.Cluster) {
		c.Server = "https://localhost:" + strconv.Itoa(d.apiHostPort)
	}); err != nil {
		return fmt.Errorf("host kubeconfig: %w", err)
	}
	if err := writeMinified(cfg, ctxName, d.KubeconfigPath(), func(c *clientcmdapi.Cluster) {
		c.Server = "https://" + ClusterServerContainerName + ":6443"
	}); err != nil {
		return fmt.Errorf("in-network kubeconfig: %w", err)
	}
	return nil
}

// writeMinified copies cfg, points it at ctxName, runs MinifyConfig, runs
// rewrite against the active cluster, and writes the result to path.
func writeMinified(cfg *clientcmdapi.Config, ctxName, path string, rewrite func(*clientcmdapi.Cluster)) error {
	out := cfg.DeepCopy()
	out.CurrentContext = ctxName
	if err := clientcmdapi.MinifyConfig(out); err != nil {
		return fmt.Errorf("minify: %w", err)
	}
	if rewrite != nil {
		clusterRef := out.Contexts[ctxName].Cluster
		if c := out.Clusters[clusterRef]; c != nil {
			rewrite(c)
		}
	}
	if err := clientcmd.WriteToFile(*out, path); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

// EnsureNamespace creates ns if it doesn't exist. Idempotent. Called
// before starting the controller-manager so the supervised
// envoy-gateway certgen can write its TLS Secret into the runtime
// namespace. The controller binary doesn't install its own Namespace,
// so whoever brings up the controller has to materialize it.
func (d *ClusterDriver) EnsureNamespace(ctx context.Context, ns string) error {
	return d.ApplyObjects(ctx, &corev1.Namespace{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	})
}

// fieldManager is the field-owner name attached to every server-side
// apply call from this driver. Picked once so reapplies from a later
// `clrk dev reload` settle onto the same set of fields instead of
// stacking distinct managers.
const fieldManager = "clrk-dev"

// kubeClientFor lazily builds the controller-runtime client used by
// ApplyObjects. The scheme covers the union of every typed object the
// dev path reconciles from the host: client-go's combined scheme
// (Namespace/ServiceAccount/Service/Deployment/ClusterRoleBinding/…),
// the aggregated APIService API, and clrk.apoxy.dev (WorkerPool). REST
// config comes from HostKubeconfigPath because k3d publishes the
// apiserver port on the host.
func (d *ClusterDriver) kubeClientFor(ctx context.Context) (client.Client, error) {
	d.kubeOnce.Do(func() {
		cfg, err := clientcmd.BuildConfigFromFlags("", d.HostKubeconfigPath())
		if err != nil {
			d.kubeErr = fmt.Errorf("loading kubeconfig: %w", err)
			return
		}
		sch := runtime.NewScheme()
		if err := scheme.AddToScheme(sch); err != nil {
			d.kubeErr = fmt.Errorf("registering core scheme: %w", err)
			return
		}
		if err := apiregistrationv1.AddToScheme(sch); err != nil {
			d.kubeErr = fmt.Errorf("registering apiregistration scheme: %w", err)
			return
		}
		if err := clrkv1alpha1.Install(sch); err != nil {
			d.kubeErr = fmt.Errorf("registering clrk scheme: %w", err)
			return
		}
		c, err := client.New(cfg, client.Options{Scheme: sch})
		if err != nil {
			d.kubeErr = fmt.Errorf("building client: %w", err)
			return
		}
		d.kubeClient = c
	})
	if d.kubeErr != nil {
		return nil, d.kubeErr
	}
	return d.kubeClient, nil
}

// ApplyObjects server-side-applies one or more typed objects through
// the host's controller-runtime client. Force ownership so re-applies
// from `clrk dev reload` reclaim fields from any previous manager.
func (d *ClusterDriver) ApplyObjects(ctx context.Context, objs ...client.Object) error {
	c, err := d.kubeClientFor(ctx)
	if err != nil {
		return err
	}
	for _, o := range objs {
		if err := c.Patch(ctx, o, client.Apply, client.ForceOwnership, client.FieldOwner(fieldManager)); err != nil {
			return fmt.Errorf("applying %s/%s: %w", o.GetNamespace(), o.GetName(), err)
		}
	}
	return nil
}

// ApplyAndVerify SSAs obj exactly like ApplyObjects, then GETs the
// stored object back and runs verify against it. Returns nil iff SSA
// succeeded *and* verify saw the field it cares about. Used by
// dev_bootstrap to catch the "SSA returned 200 but the spec didn't
// change" case that left a stale WorkerPool image in kine across
// multiple `clrk dev` launches.
//
// A free function with a type parameter (Go forbids generic methods)
// so the verify callback is typed at the call site — no
// `got.(*appsv1.Deployment)` boilerplate per verifier. The freshly-
// zeroed verify target is built via the registered scheme rather than
// reusing obj, because controller-runtime's SSA Patch mutates obj
// (clears TypeMeta) in ways that confuse a subsequent GET.
func ApplyAndVerify[T client.Object](ctx context.Context, d *ClusterDriver, obj T, verify func(got T) error) error {
	c, err := d.kubeClientFor(ctx)
	if err != nil {
		return err
	}
	gvk, err := apiutil.GVKForObject(obj, c.Scheme())
	if err != nil {
		return fmt.Errorf("resolving GVK for %T: %w", obj, err)
	}
	key := client.ObjectKeyFromObject(obj)
	if err := c.Patch(ctx, obj, client.Apply, client.ForceOwnership, client.FieldOwner(fieldManager)); err != nil {
		return fmt.Errorf("applying %s/%s: %w", obj.GetNamespace(), obj.GetName(), err)
	}
	raw, err := c.Scheme().New(gvk)
	if err != nil {
		return fmt.Errorf("instantiating verify object for %s: %w", gvk, err)
	}
	got, ok := raw.(T)
	if !ok {
		return fmt.Errorf("scheme returned %T for %s, expected %T", raw, gvk, obj)
	}
	if err := c.Get(ctx, key, got); err != nil {
		return fmt.Errorf("re-reading %s/%s after apply: %w", key.Namespace, key.Name, err)
	}
	if verify != nil {
		if err := verify(got); err != nil {
			return fmt.Errorf("apply of %s/%s did not persist: %w", key.Namespace, key.Name, err)
		}
	}
	return nil
}

// KubeClient returns the lazy controller-runtime client. Useful for
// callers that need to Get/Watch/Patch outside the SSA-only path
// ApplyObjects offers.
func (d *ClusterDriver) KubeClient(ctx context.Context) (client.Client, error) {
	return d.kubeClientFor(ctx)
}

// WaitDeploymentAvailable blocks until ns/name reports
// DeploymentAvailable=True or timeout elapses.
func (d *ClusterDriver) WaitDeploymentAvailable(ctx context.Context, ns, name string, timeout time.Duration) error {
	c, err := d.kubeClientFor(ctx)
	if err != nil {
		return err
	}
	pollErr := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		var dep appsv1.Deployment
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &dep); err != nil {
			return false, nil
		}
		for _, cond := range dep.Status.Conditions {
			if cond.Type == appsv1.DeploymentAvailable && cond.Status == corev1.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	})
	if wait.Interrupted(pollErr) {
		return fmt.Errorf("deployment %s/%s not available within %s", ns, name, timeout)
	}
	return pollErr
}

// Rollout bumps a `clrk.apoxy.dev/restartedAt` annotation on the
// Deployment's pod template, triggering the same rolling-restart
// behavior as `kubectl rollout restart`. Uses a strategic-merge patch
// so concurrent reconciles (e.g. envoy-gateway, the WorkerPool
// controller) can't lose the rollout to a 409 — no Get + Update race.
func (d *ClusterDriver) Rollout(ctx context.Context, ns, name string) error {
	c, err := d.kubeClientFor(ctx)
	if err != nil {
		return err
	}
	body := fmt.Sprintf(`{"spec":{"template":{"metadata":{"annotations":{"clrk.apoxy.dev/restartedAt":%q}}}}}`,
		time.Now().UTC().Format(time.RFC3339))
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	if err := c.Patch(ctx, dep, client.RawPatch(types.StrategicMergePatchType, []byte(body))); err != nil {
		return fmt.Errorf("patching %s/%s: %w", ns, name, err)
	}
	return nil
}

// imageIDMatches reports whether a container's reported imageID embeds any
// of wantDigests. A node that served a cached `:dev` tag instead of the
// freshly-pushed image reports the OLD digest here, so this is what
// distinguishes a real reload from a no-op rollout. Empty wantDigests means
// "don't check the digest" (the caller only wants rollout completion). Both
// the manifest digest and the image-config digest are accepted because
// container runtimes differ in which one they surface as imageID.
func imageIDMatches(imageID string, wantDigests []string) bool {
	if len(wantDigests) == 0 {
		return true
	}
	for _, w := range wantDigests {
		if w != "" && strings.Contains(imageID, w) {
			return true
		}
	}
	return false
}

// WaitRolloutComplete blocks until ns/name has fully rolled out — observed
// generation caught up, every replica updated and available, none
// unavailable — and at least one running, Ready pod reports one of
// wantDigests as a container image. This turns the fire-and-forget Rollout
// into a trustworthy gate: instead of returning the instant a rollout is
// triggered, it returns only once the new pod is actually serving, and
// (when digests are supplied) fails loudly rather than silently testing
// stale code when the node served a cached `:dev` tag. Pass no wantDigests
// to wait for rollout completion only — e.g. `clrk dev reload`, which has no
// pushed digest to assert against.
func (d *ClusterDriver) WaitRolloutComplete(ctx context.Context, ns, name string, timeout time.Duration, wantDigests ...string) error {
	c, err := d.kubeClientFor(ctx)
	if err != nil {
		return err
	}
	var lastErr error
	pollErr := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		var dep appsv1.Deployment
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &dep); err != nil {
			lastErr = err
			return false, nil
		}
		desired := int32(1)
		if dep.Spec.Replicas != nil {
			desired = *dep.Spec.Replicas
		}
		// The rollout must be observed by the controller and fully
		// converged before we trust the pod set. With the cm's Recreate
		// strategy this also guarantees the old pod (and its digest) is
		// gone, so a digest check below can't pass on a lingering old pod.
		if dep.Status.ObservedGeneration < dep.Generation ||
			dep.Status.UpdatedReplicas < desired ||
			dep.Status.AvailableReplicas < desired ||
			dep.Status.UnavailableReplicas > 0 {
			lastErr = fmt.Errorf("rollout in progress (observed=%d/%d updated=%d available=%d unavailable=%d desired=%d)",
				dep.Status.ObservedGeneration, dep.Generation,
				dep.Status.UpdatedReplicas, dep.Status.AvailableReplicas,
				dep.Status.UnavailableReplicas, desired)
			return false, nil
		}
		sel, err := metav1.LabelSelectorAsSelector(dep.Spec.Selector)
		if err != nil {
			return false, fmt.Errorf("parsing selector for %s/%s: %w", ns, name, err)
		}
		var pods corev1.PodList
		if err := c.List(ctx, &pods, client.InNamespace(ns), client.MatchingLabelsSelector{Selector: sel}); err != nil {
			lastErr = err
			return false, nil
		}
		matched := 0
		for i := range pods.Items {
			p := &pods.Items[i]
			if p.DeletionTimestamp != nil || p.Status.Phase != corev1.PodRunning {
				continue
			}
			for _, cs := range p.Status.ContainerStatuses {
				if !imageIDMatches(cs.ImageID, wantDigests) {
					continue
				}
				if !cs.Ready {
					lastErr = fmt.Errorf("pod %s container %s is on the target image but not Ready yet", p.Name, cs.Name)
					return false, nil
				}
				matched++
			}
		}
		if matched == 0 {
			if len(wantDigests) == 0 {
				lastErr = fmt.Errorf("no running pod for %s/%s yet", ns, name)
			} else {
				lastErr = fmt.Errorf("no running pod for %s/%s reports digest %v yet (node may be serving a cached :dev tag)", ns, name, wantDigests)
			}
			return false, nil
		}
		return true, nil
	})
	if wait.Interrupted(pollErr) {
		if lastErr != nil {
			return fmt.Errorf("rollout of %s/%s did not converge within %s: %w", ns, name, timeout, lastErr)
		}
		return fmt.Errorf("rollout of %s/%s did not converge within %s", ns, name, timeout)
	}
	return pollErr
}
