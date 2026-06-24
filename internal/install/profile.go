package install

import (
	corev1 "k8s.io/api/core/v1"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// Canonical object names shared by every component of the control plane. Kept
// here as the single source of truth so the cm Service/APIService/Deployment
// wiring and the downstream advertise URIs can't drift, and so callers outside
// this package (e.g. dev.go waiting on the cm Deployment) reference one name.
const (
	// ControllerManagerName is shared by the cm ServiceAccount,
	// (Cluster)RoleBinding, Deployment, and Service.
	ControllerManagerName = "clrk-controller-manager"
	// WorkerAccountName is the ServiceAccount the worker Pods run under. It
	// aliases the API package's constant so the value is defined once (the pod
	// builder defaults spec.template.serviceAccountName to it).
	WorkerAccountName = clrkv1alpha1.WorkerServiceAccountName
	// EnvoyGatewayServiceName fronts the cm's in-process envoy-gateway xDS
	// listener under the well-known name the EG data-plane bootstrap dials.
	EnvoyGatewayServiceName = "envoy-gateway"
	// ConsoleServiceName fronts the cm's embedded web console port. It's a
	// dedicated single-port Service (not a port on the cm Service) so the
	// `clrk dev` auto-forwarder — which forwards a Service's first TCP port —
	// targets the console rather than the apiserver.
	ConsoleServiceName = "clrk-console"
	// APIServiceName is the aggregated APIService routing
	// clrk.apoxy.dev/v1alpha1 to the cm.
	APIServiceName = "v1alpha1.clrk.apoxy.dev"
	// MetricsAPIServiceName is the aggregated APIService routing the
	// Tier-1 metrics group (metrics.clrk.apoxy.dev/v1alpha1) to the cm.
	// Served by the same apiserver as APIServiceName; kube-aggregator
	// keys one APIService per group, so the metrics group needs its own.
	MetricsAPIServiceName = "v1alpha1.metrics.clrk.apoxy.dev"
	// DefaultWorkerPoolName is the name of the WorkerPool the installer creates
	// (its Deployment is WorkerDeploymentName, "default-workers").
	DefaultWorkerPoolName = "default"

	// PVC names backing the cm pod's embedded stores.
	clickhouseDataPVC = "clickhouse-data"
	natsDataPVC       = "nats-data"
	kineDataPVC       = "kine-data"

	// DefaultNamespace is the control-plane namespace used by both `clrk dev`
	// and `clrk install` when none is specified. Same name in dev + prod.
	DefaultNamespace = "clrk"

	// consolePort is the cm's embedded-console plain-HTTP port. Single source
	// of truth for the --console-addr arg, the cm containerPort, and the
	// clrk-console Service so they can't drift; matches the binary default.
	consolePort = 8086
)

// cmLabels are the selector labels stamped on the cm Deployment's pod template
// and matched by its Services. Stable across reloads/upgrades so a rollout
// never orphans Endpoints.
var cmLabels = map[string]string{
	"app.kubernetes.io/name":      "clrk-controller-manager",
	"app.kubernetes.io/component": "controller-manager",
}

// TLSMode selects how the aggregated APIService verifies the cm's serving
// cert.
type TLSMode int

const (
	// TLSInsecureSkipVerify sets APIService.spec.insecureSkipTLSVerify=true and
	// lets the cm serve a self-signed in-memory cert (empty --cert-dir). The
	// dev default; never used on a customer cluster.
	TLSInsecureSkipVerify TLSMode = iota
	// TLSSelfSigned mounts an installer-minted serving-cert Secret at
	// --cert-dir and sets APIService.spec.caBundle to the installer CA.
	TLSSelfSigned
	// TLSCertManager mounts a cert-manager-issued serving-cert Secret at
	// --cert-dir and annotates the APIService with
	// cert-manager.io/inject-ca-from so the CA injector fills caBundle.
	TLSCertManager
)

// Profile is the single parameterization the manifest builders consume. The
// same struct drives `clrk dev` (Dev=true reproduces today's dev bootstrap
// byte-for-byte) and `clrk install`/`upgrade` (Dev=false, cluster-shaped
// fields set). Dev-only knobs are zero-valued and inert on a cluster profile.
type Profile struct {
	// Namespace holds the control-plane objects (cm Deployment/Service/
	// APIService/PVCs/RBAC). WorkerNamespace holds the WorkerPool + worker
	// RBAC.
	Namespace       string
	WorkerNamespace string

	// Images + pull behaviour.
	ControllerImage string
	WorkerImage     string
	PullPolicy      string // "always" | "missing" | "never"; "" => IfNotPresent
	// ImagePullSecret, when set, is attached to the cm + worker PodSpecs.
	// Cluster-only (M4).
	ImagePullSecret string

	// Sizing.
	Replicas int32 // cm replicas; v1 is always 1 (single-writer embedded stores).
	Workers  int32 // worker replicas.

	// StorageClass for the three cm PVCs. Empty => the cluster default
	// (k3d local-path in dev).
	StorageClass string

	// RBACScoped selects a scoped ClusterRole (cluster) vs the cluster-admin
	// dev shortcut. Cluster-only (M4).
	RBACScoped bool

	// APIServerCIDRs, when non-empty, emits a NetworkPolicy admitting the
	// unauthenticated aggregated API (cm:8443) only from these CIDRs — the
	// host-network sources the aggregation proxy can originate from (the
	// kube-apiserver Endpoint IPs plus node InternalIPs, since aggregation
	// requests are commonly SNAT'd to a node IP). Pods live on the pod network,
	// so they are excluded. Derived by DeriveAPIServerCIDRs with an
	// --apiserver-cidr override; empty (dev, or undetected, or --network-policy=
	// false) emits no policy. Cluster-only (M4).
	APIServerCIDRs []string

	// TLS holds the APIService serving-cert posture. CABundle is the PEM the
	// installer sets on APIService.spec.caBundle for TLSSelfSigned;
	// CertManagerCertRef ("<ns>/<name>") drives the inject-ca-from annotation
	// for TLSCertManager. Cluster cert objects are built in M4.
	TLS                TLSMode
	CABundle           []byte
	CertManagerCertRef string

	// Version is stamped onto the cm Deployment + namespace for upgrade
	// gating (M5). Empty in dev.
	Version string

	// Console gates the embedded web console: its clrk-console Service, the
	// cm containerPort, and the --console-addr arg. nil/true installs it
	// (the default); false disables it entirely (--console-addr= empty, no
	// Service/port). The console proxies the unauthenticated apiserver, so an
	// operator who doesn't want that surface sets --console=false. Reached in
	// prod via `kubectl port-forward` -- the NetworkPolicy deliberately does
	// not open 8086 (see controllerManagerNetworkPolicy).
	Console *bool

	// Dev marks the local k3d profile and gates the dev-only knobs below.
	Dev bool
	// HostAliasIP, when set, plants a host.docker.internal HostAlias on the cm
	// pod (dev: k3d-nested pods don't inherit Docker Desktop's /etc/hosts).
	HostAliasIP string
	// OTLPFallbackEndpoint, when set, becomes --dev-otlp-fallback-endpoint so
	// every captured signal is mirrored to the dev TUI receiver.
	OTLPFallbackEndpoint string
	// EnableTestWrites opens the header-gated Invocation Create/Update door
	// (--enable-invocation-test-writes). Dev/CI only.
	EnableTestWrites bool
	// EgressBackendHost, when set, becomes --dev-egress-backend-host (k3d
	// NodePort routing). Empty on a cluster (in-cluster Service DNS).
	EgressBackendHost string
}

// consoleEnabled reports whether the embedded web console is installed. It
// defaults on (nil), so an explicit --console=false is the only way off.
func (p Profile) consoleEnabled() bool {
	return p.Console == nil || *p.Console
}

// pullPolicy maps the docker --pull value to the matching corev1.PullPolicy,
// defaulting to IfNotPresent.
func (p Profile) pullPolicy() corev1.PullPolicy {
	switch p.PullPolicy {
	case "always", "Always":
		return corev1.PullAlways
	case "never", "Never":
		return corev1.PullNever
	default:
		return corev1.PullIfNotPresent
	}
}
