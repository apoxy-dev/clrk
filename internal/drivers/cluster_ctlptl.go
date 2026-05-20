package drivers

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ctlptlapi "github.com/tilt-dev/ctlptl/pkg/api"
	"github.com/tilt-dev/ctlptl/pkg/api/k3dv1alpha5"
	ctlptlcluster "github.com/tilt-dev/ctlptl/pkg/cluster"
	ctlptlregistry "github.com/tilt-dev/ctlptl/pkg/registry"
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
// bridge pattern is unchanged: controller-manager and worker containers
// still run as plain `docker run` siblings to the k3d nodes, with
// selectorless Services + hand-managed Endpoints pointing back at their
// docker IPs.
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
// applyObjects. The scheme covers corev1 (Namespace/Service/Endpoints)
// and apiregistration/v1 (APIService) — every type the dev path
// reconciles from the host. REST config comes from HostKubeconfigPath
// because k3d publishes the apiserver port on the host.
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

// ApplyDefaultWorkerPoolBridge wires a `default-workers` Service in
// the default namespace to the worker docker container(s) so
// in-cluster pods can dial the dispatcher by the same DNS name the
// WorkerPoolDeploymentReconciler would use in cluster mode
// (`{wp.Name}-workers`). Without this bridge clrk dev has no way to
// reach the worker dispatch port from inside k3s — the worker runs
// as a docker container outside the pod network.
//
// Called once per bringUp after every worker container is up and its
// docker-network IP is known.
func (d *ClusterDriver) ApplyDefaultWorkerPoolBridge(ctx context.Context, workerIPs []string, dispatchPort int32) error {
	if len(workerIPs) == 0 {
		return nil
	}
	svc, eps := bridgeObjects("default", "default-workers", workerIPs, []bridgePort{
		{Name: "dispatch", Port: dispatchPort},
	})
	return d.ApplyObjects(ctx, svc, eps)
}

// ApplyControllerManagerBridge wires two selectorless Services to the
// clrk-controller-manager docker container so in-cluster pods can dial
// it by DNS name. Both bridges live in clrkNS — the controller-
// manager's runtime namespace — to match the in-cluster shape where
// EG provisions all data-plane resources alongside the controller via
// `ENVOY_GATEWAY_NAMESPACE`.
//
//  1. <clrkNS>/clrk-controller-manager:{grpcPort,extProcPort,healthPort} —
//     clrk's gRPC services. grpcPort fronts the extension hooks +
//     cert provider; extProcPort fronts the TaskAgent ingress
//     ExternalProcessor consumed by the per-TA EG. Multiple named
//     ports on a single Service so EG extension config and the
//     per-namespace `clrk-ingress-extproc` Backend (FQDN.Port=9444)
//     resolve through one bridge.
//  2. <clrkNS>/envoy-gateway:xdsPort — the EG xDS server. EG's data-
//     plane bootstrap dials `envoy-gateway.${ENVOY_GATEWAY_NAMESPACE}.svc:18000`,
//     so this Service name is fixed; only the namespace floats with
//     the runtime ns.
func (d *ClusterDriver) ApplyControllerManagerBridge(ctx context.Context, clrkNS, cmIP string, grpcPort, extProcPort, healthPort, xdsPort int32) error {
	ns := &corev1.Namespace{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{Name: clrkNS},
	}
	cmSvc, cmEps := bridgeObjects(clrkNS, "clrk-controller-manager", []string{cmIP}, []bridgePort{
		{Name: "grpc", Port: grpcPort},
		{Name: "extproc", Port: extProcPort},
		{Name: "health", Port: healthPort},
	})
	egSvc, egEps := bridgeObjects(clrkNS, "envoy-gateway", []string{cmIP}, []bridgePort{
		{Name: "grpc", Port: xdsPort},
	})
	return d.ApplyObjects(ctx, ns, cmSvc, cmEps, egSvc, egEps)
}

// bridgePort names a single port-and-targetPort entry on a bridge
// Service. Port and targetPort are always equal — the bridge points
// at a docker container that listens on the same port the cluster
// dials.
type bridgePort struct {
	Name string
	Port int32
}

// bridgeObjects builds the typed Service + Endpoints pair that fronts
// one or more docker IPs under an in-cluster DNS name. Selectorless
// Service relies on the hand-managed Endpoints object — kube-proxy
// programs routes from those addresses straight to the docker network.
func bridgeObjects(ns, name string, ips []string, ports []bridgePort) (*corev1.Service, *corev1.Endpoints) {
	svcPorts := make([]corev1.ServicePort, len(ports))
	epPorts := make([]corev1.EndpointPort, len(ports))
	for i, p := range ports {
		svcPorts[i] = corev1.ServicePort{
			Name:       p.Name,
			Port:       p.Port,
			Protocol:   corev1.ProtocolTCP,
			TargetPort: intstr.FromInt32(p.Port),
		}
		epPorts[i] = corev1.EndpointPort{
			Name:     p.Name,
			Port:     p.Port,
			Protocol: corev1.ProtocolTCP,
		}
	}
	addrs := make([]corev1.EndpointAddress, len(ips))
	for i, ip := range ips {
		addrs[i] = corev1.EndpointAddress{IP: ip}
	}
	svc := &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       corev1.ServiceSpec{Ports: svcPorts},
	}
	eps := &corev1.Endpoints{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Endpoints"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Subsets:    []corev1.EndpointSubset{{Addresses: addrs, Ports: epPorts}},
	}
	return svc, eps
}

// ApplyObjects server-side-applies one or more typed objects through
// the host's controller-runtime client. Force ownership so re-applies
// from `clrk dev reload` reclaim fields from any previous manager
// (e.g. an earlier session that used kubectl). Endpoints objects
// don't accept SSA because they predate the apply machinery — fall
// back to Create-or-Update for them.
func (d *ClusterDriver) ApplyObjects(ctx context.Context, objs ...client.Object) error {
	c, err := d.kubeClientFor(ctx)
	if err != nil {
		return err
	}
	for _, o := range objs {
		if _, isEndpoints := o.(*corev1.Endpoints); isEndpoints {
			if err := createOrUpdateEndpoints(ctx, c, o.(*corev1.Endpoints)); err != nil {
				return fmt.Errorf("applying %s/%s: %w", o.GetNamespace(), o.GetName(), err)
			}
			continue
		}
		if err := c.Patch(ctx, o, client.Apply, client.ForceOwnership, client.FieldOwner(fieldManager)); err != nil {
			return fmt.Errorf("applying %s/%s: %w", o.GetNamespace(), o.GetName(), err)
		}
	}
	return nil
}

// createOrUpdateEndpoints handles the Endpoints type's lack of SSA
// support: Create if missing, otherwise overwrite Subsets on the
// existing object. Our bridge re-applies always supply the full
// Subsets list, so a blind overwrite is correct (no field merge
// needed).
func createOrUpdateEndpoints(ctx context.Context, c client.Client, desired *corev1.Endpoints) error {
	var cur corev1.Endpoints
	err := c.Get(ctx, client.ObjectKeyFromObject(desired), &cur)
	if apierrors.IsNotFound(err) {
		return c.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	cur.Subsets = desired.Subsets
	return c.Update(ctx, &cur)
}
