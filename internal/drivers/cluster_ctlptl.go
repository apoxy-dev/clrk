package drivers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	ctlptlapi "github.com/tilt-dev/ctlptl/pkg/api"
	"github.com/tilt-dev/ctlptl/pkg/api/k3dv1alpha5"
	ctlptlcluster "github.com/tilt-dev/ctlptl/pkg/cluster"
	ctlptlregistry "github.com/tilt-dev/ctlptl/pkg/registry"
)

// RegistryDataVolume is the docker volume that backs the local
// registry's RegistryDataMount. ctlptl v0.9.0's Registry API has no
// volume field; we attach this volume post-Apply (see
// ensurePersistentRegistry) so pushed images survive `clrk dev`
// stop/start cycles.
const (
	RegistryDataVolume = "clrk-registry-data"
	RegistryDataMount  = "/var/lib/registry"
)

const (
	// ClusterName is the name passed to k3d/ctlptl. k3d derives container
	// names from it: server "k3d-clrk-server-0", loadbalancer "k3d-clrk-serverlb",
	// tools "k3d-clrk-tools". The leading "k3d-" is k3d's, not ours.
	ClusterName = "clrk"

	// RegistryName is the name passed to ctlptl for the local OCI
	// registry. ctlptl uses it as the container name on the shared
	// network, so in-cluster pods resolve it as `clrk-registry:5000`.
	RegistryName = "clrk-registry"

	// ClusterServerContainerName is k3d's deterministic name for the
	// single-server node container. Used by dev_status / dev_logs.
	ClusterServerContainerName = "k3d-" + ClusterName + "-server-0"

	// DefaultK3sImage pins a minor version whose k8s.io/api release matches
	// clrk and apoxy-cloud's module resolution (v0.34.x). Override via
	// ClusterDriver.K3sImage for CI pinning.
	DefaultK3sImage = "rancher/k3s:v1.34.1-k3s1"

	// KubeconfigFileName is written into the shared data dir so other
	// drivers (and the user) can find the rewritten in-network kubeconfig.
	KubeconfigFileName = "kubeconfig"
)

// ClusterDriver brings up a k3d cluster and a colocated local OCI
// registry via the ctlptl Go library, pinned to clrk's shared docker
// network (NetworkName). Replaces the legacy K3sDriver which docker-ran
// rancher/k3s directly.
//
// The cluster + registry come up on the existing `clrk` network so the
// host can push to localhost:<RegistryHostPort()> and k3d's containerd
// resolves the same content via `clrk-registry:5000/...`. The
// controller-manager and worker(s) run as Pods inside this cluster.
type ClusterDriver struct {
	// DataDir is the host path where the rewritten kubeconfigs are
	// written. Must be the same path mounted into the cm/worker drivers
	// so they can read the in-network kubeconfig.
	DataDir string
	// K3sImage overrides the k3s image k3d boots. Empty = DefaultK3sImage.
	K3sImage string
	// RegistryPort is the host port the local registry publishes. Zero
	// means auto-pick a free port; the actual port is available via
	// RegistryHostPort() after Start.
	RegistryPort int

	// populated by Start; safe to read once Start returns nil.
	registryHostPort int

	clusterCtl  *ctlptlcluster.Controller
	registryCtl *ctlptlregistry.Controller

	// ctlptlOut/ctlptlErr capture ctlptl + k3d's stdout/stderr so we
	// don't spam the dev TUI. Dumped to slog on error in Start.
	ctlptlOut bytes.Buffer
	ctlptlErr bytes.Buffer

	// kubeClient is built lazily from HostKubeconfigPath() the first
	// time we apply objects. controller-runtime client over a scheme
	// carrying corev1 + apiregistration/v1, used via server-side apply.
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

// Start brings up the registry and the k3d cluster on the shared `clrk`
// network. Idempotent: ctlptl reconciles existing Registry/Cluster
// objects on the host, so a second call returns nil after detecting the
// existing state matches.
func (d *ClusterDriver) Start(ctx context.Context) error {
	if err := EnsureNetwork(ctx); err != nil {
		return err
	}
	if err := d.initControllers(); err != nil {
		return err
	}

	regSpec := &ctlptlapi.Registry{
		TypeMeta: ctlptlapi.TypeMeta{APIVersion: "ctlptl.dev/v1alpha1", Kind: "Registry"},
		Name:     RegistryName,
		Port:     d.RegistryPort,
		// k3d only connects to registries with this label, even when
		// the Cluster references the Registry by name — see ctlptl's
		// admin_k3d.go where it uses `--registry-use <name>` and trusts
		// the registry only when it carries this app label.
		Labels: map[string]string{"app": "k3d"},
	}
	reg, err := d.registryCtl.Apply(ctx, regSpec)
	if err != nil {
		d.dumpBuffers("registry.Apply")
		return fmt.Errorf("applying registry %q: %w", RegistryName, err)
	}
	d.registryHostPort = reg.Status.HostPort

	k3sImg := d.K3sImage
	if k3sImg == "" {
		k3sImg = DefaultK3sImage
	}

	clusterSpec := &ctlptlapi.Cluster{
		TypeMeta: ctlptlapi.TypeMeta{APIVersion: "ctlptl.dev/v1alpha1", Kind: "Cluster"},
		Product:  "k3d",
		Name:     "k3d-" + ClusterName,
		Registry: RegistryName,
		K3D: &ctlptlapi.K3DCluster{
			V1Alpha5Simple: &k3dv1alpha5.SimpleConfig{
				Network: NetworkName,
				Image:   k3sImg,
				Options: k3dv1alpha5.SimpleConfigOptions{
					K3dOptions: k3dv1alpha5.SimpleConfigOptionsK3d{
						// Leave failed clusters intact so we can read
						// k3s logs from the restart-loop container
						// instead of getting a generic ctlptl error.
						NoRollback: true,
					},
					K3sOptions: k3dv1alpha5.SimpleConfigOptionsK3s{
						ExtraArgs: serverK3sArgs(),
					},
				},
			},
		},
	}
	if _, err := d.clusterCtl.Apply(ctx, clusterSpec); err != nil {
		d.dumpBuffers("cluster.Apply")
		return fmt.Errorf("applying cluster %q: %w", ClusterName, err)
	}

	// ctlptl's cluster.Apply tears down and re-creates the registry
	// container as part of wiring it to the new k3d cluster, so the
	// volume swap has to land *after* cluster.Apply — otherwise the
	// container we just created with the named volume gets replaced
	// by ctlptl's freshly-spawned anonymous-volume variant.
	if err := ensurePersistentRegistry(ctx, RegistryName, RegistryDataVolume, d.registryHostPort); err != nil {
		return fmt.Errorf("persisting registry storage: %w", err)
	}

	if err := d.writeKubeconfigs(ctx); err != nil {
		return fmt.Errorf("writing kubeconfigs: %w", err)
	}
	return nil
}

// serverK3sArgs returns the k3s flags that move from the old K3sDriver
// into k3d's SimpleConfig.Options.K3sOptions.ExtraArgs. NodeFilters scope
// each flag to the single server node — k3d would reject unfiltered args.
func serverK3sArgs() []k3dv1alpha5.K3sArgWithNodeFilters {
	flags := []string{
		"--disable=traefik",
		"--disable=servicelb",
		"--disable=metrics-server",
		// Dual-stack so envoy pods can reach AAAA-only or AAAA-preferred
		// upstreams (api.anthropic.com etc.). Pairs with EnsureNetwork's
		// dual-stack bridge — without both, pod-side ENETUNREACH on every
		// v6 destination. ULA pod/service CIDRs; outbound v6 NATs through
		// the docker bridge.
		"--cluster-cidr=10.42.0.0/16,fd00:42::/56",
		"--service-cidr=10.43.0.0/16,fd00:43::/112",
		"--flannel-ipv6-masq",
	}
	out := make([]k3dv1alpha5.K3sArgWithNodeFilters, len(flags))
	for i, f := range flags {
		out[i] = k3dv1alpha5.K3sArgWithNodeFilters{Arg: f, NodeFilters: []string{"server:*"}}
	}
	return out
}

// initControllers lazily constructs the ctlptl controllers and binds
// them to internal buffers, so ctlptl's klog/colorized progress output
// never leaks to the user's terminal during a successful start.
func (d *ClusterDriver) initControllers() error {
	if d.clusterCtl != nil && d.registryCtl != nil {
		return nil
	}
	ios := genericclioptions.IOStreams{
		In:     nil,
		Out:    &d.ctlptlOut,
		ErrOut: &d.ctlptlErr,
	}
	regCtl, err := ctlptlregistry.DefaultController(ios)
	if err != nil {
		return fmt.Errorf("ctlptl registry controller: %w", err)
	}
	cluCtl, err := ctlptlcluster.DefaultController(ios)
	if err != nil {
		return fmt.Errorf("ctlptl cluster controller: %w", err)
	}
	d.clusterCtl = cluCtl
	d.registryCtl = regCtl
	return nil
}

// Stop deletes the k3d cluster and the registry. Idempotent.
func (d *ClusterDriver) Stop(ctx context.Context) error {
	if err := d.initControllers(); err != nil {
		return err
	}
	var firstErr error
	if err := d.clusterCtl.Delete(ctx, "k3d-"+ClusterName); err != nil {
		firstErr = fmt.Errorf("deleting cluster: %w", err)
	}
	if err := d.registryCtl.Delete(ctx, RegistryName); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("deleting registry: %w", err)
	}
	return firstErr
}

// KubeconfigPath is the host path to the kubeconfig with the server URL
// rewritten to the in-network k3d node hostname. Bind-mount into cm /
// worker containers so they reach k3s via the shared docker network.
func (d *ClusterDriver) KubeconfigPath() string {
	return filepath.Join(d.DataDir, KubeconfigFileName)
}

// HostKubeconfigPath is the host path to the kubeconfig pointing at
// k3d's host-published loadbalancer port. Use this for `kubectl` from
// the host, integration tests, and `clrk dev wait-ready`.
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

// writeKubeconfigs materializes two kubeconfig files under DataDir:
//
//   - kubeconfig: server URL rewritten to k3d's in-network DNS name. Bind-
//     mounted into the cm and worker containers via WithVolume.
//   - kubeconfig.host: minified copy of the cluster's context in the
//     user's default kubeconfig. Used by integration tests, the host
//     kubectl, and `clrk dev wait-ready`.
//
// ctlptl-as-library only merges the new context into the default
// kubeconfig (~/.kube/config or KUBECONFIG); unlike the k3d CLI it does
// NOT write a standalone ~/.k3d/kubeconfig-<name>.yaml. We extract the
// context using clientcmd's standard loading rules + MinifyConfig.
func (d *ClusterDriver) writeKubeconfigs(ctx context.Context) error {
	if err := os.MkdirAll(d.DataDir, 0o755); err != nil {
		return err
	}

	ctxName := "k3d-" + ClusterName
	merged, err := loadMergedKubeconfig(ctx, ctxName)
	if err != nil {
		return err
	}

	if err := writeMinified(merged, ctxName, d.HostKubeconfigPath(), nil); err != nil {
		return fmt.Errorf("host kubeconfig: %w", err)
	}
	// k3d's serverlb cert covers 0.0.0.0 + 127.0.0.1 + the loadbalancer
	// container name, but NOT the server node's docker-DNS name. Pods
	// don't share a trust store with the cm container, so insecure-skip
	// is equivalent to wiring k3s --tls-san for our hostname and avoids
	// a re-issue dance per `clrk dev` boot.
	rewrite := func(c *clientcmdapi.Cluster) {
		c.Server = "https://" + ClusterServerContainerName + ":6443"
		c.InsecureSkipTLSVerify = true
		c.CertificateAuthorityData = nil
		c.CertificateAuthority = ""
	}
	if err := writeMinified(merged, ctxName, d.KubeconfigPath(), rewrite); err != nil {
		return fmt.Errorf("in-network kubeconfig: %w", err)
	}
	return nil
}

// loadMergedKubeconfig reads the user's default kubeconfig (respecting
// $KUBECONFIG / ~/.kube/config precedence) and returns it once ctxName
// appears. Tries once synchronously — ctlptl's Apply merges the new
// context before returning, so the common path is a single load with
// no sleep. Falls back to a 15s poll for the rare case where the file
// hasn't flushed yet on Docker Desktop.
func loadMergedKubeconfig(ctx context.Context, ctxName string) (*clientcmdapi.Config, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if c, err := rules.Load(); err == nil && c != nil && c.Contexts[ctxName] != nil {
		return c, nil
	}
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, fmt.Errorf("k3d context %q never appeared in default kubeconfig (rules: %v)", ctxName, rules.GetLoadingPrecedence())
		case <-time.After(200 * time.Millisecond):
		}
		if c, err := rules.Load(); err == nil && c != nil && c.Contexts[ctxName] != nil {
			return c, nil
		}
	}
}

// writeMinified copies merged, points it at ctxName, runs MinifyConfig,
// optionally rewrites the active cluster (e.g. to swap the server URL),
// and writes the result to path.
func writeMinified(merged *clientcmdapi.Config, ctxName, path string, rewrite func(*clientcmdapi.Cluster)) error {
	cfg := merged.DeepCopy()
	cfg.CurrentContext = ctxName
	if err := clientcmdapi.MinifyConfig(cfg); err != nil {
		return fmt.Errorf("minify: %w", err)
	}
	if rewrite != nil {
		clusterRef := cfg.Contexts[ctxName].Cluster
		if c := cfg.Clusters[clusterRef]; c != nil {
			rewrite(c)
		}
	}
	if err := clientcmd.WriteToFile(*cfg, path); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

// ensurePersistentRegistry attaches a named docker volume to the
// registry container ctlptl just brought up, so `/var/lib/registry`
// survives the next `clrk dev` stop/start cycle. ctlptl v0.9.0's
// Registry type has no Volumes field, so the only way to inject one is
// to recreate the container post-Apply with the same image / labels /
// network / port mapping plus the mount. Idempotent: re-runs on the
// same boot detect the existing mount and no-op.
//
// After recreate we block until the new registry answers `/v2/` on the
// host port — without that wait, the next bring-up step
// (`bootstrapControllerManager`) can race the registry's HTTP listener
// and force the cm Pod through an ImagePullBackOff retry cycle.
//
// On Docker Desktop for Mac the volume lives inside the Linux VM, not
// on the user's host filesystem; that's fine — we just need persistence
// across container destroy, not host-side inspection.
func ensurePersistentRegistry(ctx context.Context, container, volume string, hostPort int) error {
	if out, err := exec.CommandContext(ctx, "docker", "volume", "create", volume).CombinedOutput(); err != nil {
		return fmt.Errorf("creating docker volume %q: %w: %s", volume, err, bytes.TrimSpace(out))
	}
	image, labels, hasMount, err := inspectRegistry(ctx, container, volume)
	if err != nil {
		return err
	}
	if !hasMount {
		if out, err := exec.CommandContext(ctx, "docker", "rm", "-f", container).CombinedOutput(); err != nil {
			return fmt.Errorf("removing registry container %q: %w: %s", container, err, bytes.TrimSpace(out))
		}
		args := []string{
			"run", "-d",
			"--name", container,
			"--restart", "always",
			"--network", NetworkName,
			"-p", fmt.Sprintf("%d:5000", hostPort),
			"-v", volume + ":" + RegistryDataMount,
		}
		for k, v := range labels {
			args = append(args, "--label", k+"="+v)
		}
		args = append(args, image)
		if out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
			return fmt.Errorf("recreating registry container with volume: %w: %s", err, bytes.TrimSpace(out))
		}
	}
	return waitRegistryReady(ctx, hostPort, 30*time.Second)
}

// waitRegistryReady polls the registry's /v2/ endpoint until it
// returns 200, eliminating the rm → run → first-serve gap that would
// otherwise cause the immediately-following cm Deployment to land in
// ImagePullBackOff and wait out kubelet's retry backoff.
func waitRegistryReady(ctx context.Context, hostPort int, timeout time.Duration) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/v2/", hostPort)
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err == nil {
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("registry at %s never became ready within %s", url, timeout)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// inspectRegistry pulls image, labels, and mount presence from one
// docker inspect — the recreate path needs all three before destroying
// the existing container.
func inspectRegistry(ctx context.Context, container, volume string) (image string, labels map[string]string, hasMount bool, err error) {
	want := volume + ":" + RegistryDataMount
	out, err := exec.CommandContext(ctx, "docker", "inspect",
		"--format",
		`{{.Config.Image}}`+"\n"+
			`{{json .Config.Labels}}`+"\n"+
			`{{range .Mounts}}{{.Name}}:{{.Destination}}|{{end}}`,
		container,
	).Output()
	if err != nil {
		return "", nil, false, fmt.Errorf("inspecting registry container %q: %w", container, err)
	}
	parts := strings.SplitN(strings.TrimRight(string(out), "\n"), "\n", 3)
	if len(parts) < 3 {
		return "", nil, false, fmt.Errorf("unexpected docker inspect output: %q", out)
	}
	if err := json.Unmarshal([]byte(parts[1]), &labels); err != nil {
		return "", nil, false, fmt.Errorf("parsing labels for %q: %w", container, err)
	}
	for _, m := range strings.Split(parts[2], "|") {
		if strings.TrimSpace(m) == want {
			hasMount = true
			break
		}
	}
	return strings.TrimSpace(parts[0]), labels, hasMount, nil
}

// dumpBuffers routes ctlptl's captured stdout/stderr through slog so a
// failure in cluster.Apply / registry.Apply surfaces the actual k3d
// error chain via the same log sink as the rest of `clrk dev` (the dev
// TUI in interactive mode, plain stderr otherwise) instead of bypassing
// it with a direct fprintf.
func (d *ClusterDriver) dumpBuffers(stage string) {
	if d.ctlptlOut.Len() > 0 {
		slog.Error("ctlptl output", "stage", stage, "stream", "stdout", "log", d.ctlptlOut.String())
	}
	if d.ctlptlErr.Len() > 0 {
		slog.Error("ctlptl output", "stage", stage, "stream", "stderr", "log", d.ctlptlErr.String())
	}
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
// from `clrk dev reload` reclaim fields from any previous manager
// (e.g. an earlier session that used kubectl).
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

// KubeClient returns the lazy controller-runtime client. Useful for
// callers that need to Get/Watch/Patch outside the SSA-only path
// ApplyObjects offers (e.g. waiting on Deployment conditions, streaming
// pod logs, triggering rollouts).
func (d *ClusterDriver) KubeClient(ctx context.Context) (client.Client, error) {
	return d.kubeClientFor(ctx)
}

// WaitDeploymentAvailable blocks until ns/name reports
// DeploymentAvailable=True or timeout elapses. Polls every 500ms;
// keeps the implementation independent of Watch lifecycles so any
// transient apiserver hiccup just retries on the next tick.
func (d *ClusterDriver) WaitDeploymentAvailable(ctx context.Context, ns, name string, timeout time.Duration) error {
	c, err := d.kubeClientFor(ctx)
	if err != nil {
		return err
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		var dep appsv1.Deployment
		if getErr := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &dep); getErr == nil {
			for _, cond := range dep.Status.Conditions {
				if cond.Type == appsv1.DeploymentAvailable && cond.Status == corev1.ConditionTrue {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("deployment %s/%s not available within %s", ns, name, timeout)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Rollout bumps a `clrk.apoxy.dev/restartedAt` annotation on the
// Deployment's pod template, triggering the same rolling-restart
// behavior as `kubectl rollout restart`. Idempotent across multiple
// calls — each one stamps a fresh RFC3339 timestamp.
func (d *ClusterDriver) Rollout(ctx context.Context, ns, name string) error {
	c, err := d.kubeClientFor(ctx)
	if err != nil {
		return err
	}
	var dep appsv1.Deployment
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &dep); err != nil {
		return fmt.Errorf("get %s/%s: %w", ns, name, err)
	}
	if dep.Spec.Template.Annotations == nil {
		dep.Spec.Template.Annotations = map[string]string{}
	}
	dep.Spec.Template.Annotations["clrk.apoxy.dev/restartedAt"] = time.Now().UTC().Format(time.RFC3339)
	return c.Update(ctx, &dep)
}
