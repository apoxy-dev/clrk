package install

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Level is the severity of a preflight result. FAIL aborts the install; WARN is
// confirmable (auto-accepted under --yes with a logged warning); PASS is
// informational.
type Level int

const (
	LevelPass Level = iota
	LevelWarn
	LevelFail
)

func (l Level) String() string {
	switch l {
	case LevelFail:
		return "FAIL"
	case LevelWarn:
		return "WARN"
	default:
		return "PASS"
	}
}

// PreflightResult is one cluster-readiness finding. Detail says what was found;
// Hint is the actionable fix (empty on PASS).
type PreflightResult struct {
	Name   string
	Level  Level
	Detail string
	Hint   string
}

// minKubeMinor is the oldest Kubernetes minor clrk supports. Aggregation, SSA,
// and CRD v1 schemas are all mature by here.
const minKubeMinor = 28

// Preflight runs every cluster-readiness check against the target cluster and
// returns the findings (collect-all-then-report). The caller decides policy:
// any LevelFail aborts; LevelWarn is confirmable. upgrade flips the existing-
// install check from a WARN ("an install exists; use `clrk upgrade`") to an
// informational PASS, since on the upgrade path an existing install is exactly
// what is expected. The Profile shapes namespace-scoped checks (worker-ns PSA,
// RBAC on the install namespace, named StorageClass).
func Preflight(ctx context.Context, c client.Client, disco discovery.DiscoveryInterface, p Profile, upgrade bool) []PreflightResult {
	var out []PreflightResult
	add := func(r PreflightResult) { out = append(out, r) }

	add(checkKubeVersion(disco))
	groups, groupsErr := disco.ServerGroups()
	add(checkAggregationLayer(groups, groupsErr))
	out = append(out, checkRBAC(ctx, c, p)...)
	add(checkStorageClass(ctx, c, p))
	add(checkCRDConflict(groups, groupsErr))
	add(checkWorkerNamespacePSA(ctx, c, p))
	add(checkCertManager(ctx, c, groups, groupsErr))
	add(checkMetricsServer(groups, groupsErr))
	add(checkExistingInstall(ctx, c, p, upgrade))
	return out
}

func checkKubeVersion(disco discovery.DiscoveryInterface) PreflightResult {
	const name = "Kubernetes version"
	v, err := disco.ServerVersion()
	if err != nil {
		return PreflightResult{Name: name, Level: LevelFail, Detail: err.Error(),
			Hint: "cluster API unreachable — check kubeconfig/context and network"}
	}
	minor, perr := parseMinor(v.Minor)
	if perr != nil {
		return PreflightResult{Name: name, Level: LevelWarn, Detail: fmt.Sprintf("server %s.%s (unparseable minor)", v.Major, v.Minor),
			Hint: "could not verify version; proceed only if >= 1." + strconv.Itoa(minKubeMinor)}
	}
	if minor < minKubeMinor {
		return PreflightResult{Name: name, Level: LevelFail, Detail: fmt.Sprintf("server v%s.%d", v.Major, minor),
			Hint: fmt.Sprintf("clrk requires Kubernetes >= 1.%d (aggregation + SSA + CRD v1)", minKubeMinor)}
	}
	return PreflightResult{Name: name, Level: LevelPass, Detail: fmt.Sprintf("server v%s.%d", v.Major, minor)}
}

func checkAggregationLayer(groups *metav1.APIGroupList, err error) PreflightResult {
	const name = "Aggregation layer"
	if err != nil {
		return PreflightResult{Name: name, Level: LevelFail, Detail: err.Error(),
			Hint: "could not list API groups; the aggregation layer is required for the clrk API"}
	}
	if hasGroup(groups, "apiregistration.k8s.io") {
		return PreflightResult{Name: name, Level: LevelPass, Detail: "apiregistration.k8s.io present"}
	}
	return PreflightResult{Name: name, Level: LevelFail, Detail: "apiregistration.k8s.io missing",
		Hint: "enable the kube-aggregator (the clrk API is served via an aggregated APIService)"}
}

// rbacProbe is one (group,resource,namespace) the installer must be able to
// create. Namespace empty => cluster-scoped.
type rbacProbe struct {
	group, resource, namespace, label string
}

func checkRBAC(ctx context.Context, c client.Client, p Profile) []PreflightResult {
	probes := []rbacProbe{
		{"apiregistration.k8s.io", "apiservices", "", "APIService"},
		{"rbac.authorization.k8s.io", "clusterroles", "", "ClusterRole"},
		{"rbac.authorization.k8s.io", "clusterrolebindings", "", "ClusterRoleBinding"},
		{"apiextensions.k8s.io", "customresourcedefinitions", "", "CRD"},
		{"", "namespaces", "", "Namespace"},
		{"apps", "deployments", p.Namespace, "Deployment"},
		{"", "services", p.Namespace, "Service"},
		{"", "serviceaccounts", p.Namespace, "ServiceAccount"},
		{"", "secrets", p.Namespace, "Secret"},
		{"", "persistentvolumeclaims", p.Namespace, "PVC"},
		{"networking.k8s.io", "networkpolicies", p.Namespace, "NetworkPolicy"},
	}
	// When the workers land in a separate namespace, the installer creates a
	// ServiceAccount and the default WorkerPool there too -- probe those so a
	// permission gap fails preflight up front rather than mid-apply.
	if p.WorkerNamespace != "" && p.WorkerNamespace != p.Namespace {
		probes = append(probes,
			rbacProbe{"", "serviceaccounts", p.WorkerNamespace, "ServiceAccount (worker ns)"},
			rbacProbe{"clrk.apoxy.dev", "workerpools", p.WorkerNamespace, "WorkerPool (worker ns)"},
		)
	}
	var denied []string
	failed := false
	for _, pr := range probes {
		ssar := &authorizationv1.SelfSubjectAccessReview{
			Spec: authorizationv1.SelfSubjectAccessReviewSpec{
				ResourceAttributes: &authorizationv1.ResourceAttributes{
					Namespace: pr.namespace,
					Verb:      "create",
					Group:     pr.group,
					Resource:  pr.resource,
				},
			},
		}
		if err := c.Create(ctx, ssar); err != nil {
			// Couldn't run the review at all — report once and stop probing.
			return []PreflightResult{{Name: "RBAC (can-i create)", Level: LevelWarn,
				Detail: "SelfSubjectAccessReview failed: " + err.Error(),
				Hint:   "could not verify permissions; ensure your context can create cluster-scoped + namespaced objects"}}
		}
		if !ssar.Status.Allowed {
			denied = append(denied, pr.label)
			failed = true
		}
	}
	if failed {
		return []PreflightResult{{Name: "RBAC (can-i create)", Level: LevelFail,
			Detail: "cannot create: " + strings.Join(denied, ", "),
			Hint:   "use a kubeconfig/context with cluster-admin (or equivalent) to install clrk"}}
	}
	return []PreflightResult{{Name: "RBAC (can-i create)", Level: LevelPass, Detail: "all required creates allowed"}}
}

func checkStorageClass(ctx context.Context, c client.Client, p Profile) PreflightResult {
	const name = "StorageClass"
	var list storagev1.StorageClassList
	if err := c.List(ctx, &list); err != nil {
		return PreflightResult{Name: name, Level: LevelWarn, Detail: "could not list StorageClasses: " + err.Error(),
			Hint: "the cm PVCs need a provisioner; verify a default or named StorageClass exists"}
	}
	if p.StorageClass != "" {
		for i := range list.Items {
			if list.Items[i].Name == p.StorageClass {
				return PreflightResult{Name: name, Level: LevelPass, Detail: "named StorageClass " + p.StorageClass + " present"}
			}
		}
		return PreflightResult{Name: name, Level: LevelFail, Detail: "named StorageClass " + p.StorageClass + " not found",
			Hint: "pass an existing --storage-class or omit it to use the cluster default"}
	}
	for i := range list.Items {
		if isDefaultStorageClass(list.Items[i]) {
			return PreflightResult{Name: name, Level: LevelPass, Detail: "default StorageClass " + list.Items[i].Name + " present"}
		}
	}
	if len(list.Items) == 0 {
		return PreflightResult{Name: name, Level: LevelFail, Detail: "no StorageClasses found",
			Hint: "the cm's kine/ClickHouse/NATS PVCs need a dynamic provisioner; install one or pre-provision PVs"}
	}
	return PreflightResult{Name: name, Level: LevelFail, Detail: "no default StorageClass",
		Hint: "set a default StorageClass or pass --storage-class <name>"}
}

func checkCRDConflict(groups *metav1.APIGroupList, err error) PreflightResult {
	const name = "Gateway-API / Envoy-Gateway CRDs"
	if err != nil {
		return PreflightResult{Name: name, Level: LevelWarn, Detail: "could not list API groups"}
	}
	var present []string
	if hasGroup(groups, "gateway.networking.k8s.io") {
		present = append(present, "gateway.networking.k8s.io")
	}
	if hasGroup(groups, "gateway.envoyproxy.io") {
		present = append(present, "gateway.envoyproxy.io")
	}
	if len(present) == 0 {
		return PreflightResult{Name: name, Level: LevelPass, Detail: "absent — clrk will install pinned versions"}
	}
	return PreflightResult{Name: name, Level: LevelWarn, Detail: "already present: " + strings.Join(present, ", "),
		Hint: "clrk pins Gateway-API v1.2.1 + Envoy Gateway v1.4.0 and installs missing CRDs only (if-missing); confirm the existing versions are compatible"}
}

func checkWorkerNamespacePSA(ctx context.Context, c client.Client, p Profile) PreflightResult {
	const name = "Worker namespace Pod Security"
	var ns corev1.Namespace
	if err := c.Get(ctx, client.ObjectKey{Name: p.WorkerNamespace}, &ns); err != nil {
		if apierrors.IsNotFound(err) {
			return PreflightResult{Name: name, Level: LevelPass, Detail: p.WorkerNamespace + " will be created with privileged Pod Security labels"}
		}
		return PreflightResult{Name: name, Level: LevelWarn, Detail: "could not read namespace: " + err.Error()}
	}
	enforce := ns.Labels["pod-security.kubernetes.io/enforce"]
	switch enforce {
	case "restricted":
		return PreflightResult{Name: name, Level: LevelFail, Detail: p.WorkerNamespace + " enforces PSA=restricted",
			Hint: "workers need privileged + unconfined seccomp/AppArmor for gVisor; pick a worker namespace without enforce=restricted (or relabel it)"}
	case "baseline":
		return PreflightResult{Name: name, Level: LevelWarn, Detail: p.WorkerNamespace + " enforces PSA=baseline",
			Hint: "baseline rejects privileged pods; relabel the worker namespace to privileged"}
	default:
		return PreflightResult{Name: name, Level: LevelPass, Detail: p.WorkerNamespace + " allows privileged pods"}
	}
}

func checkCertManager(ctx context.Context, c client.Client, groups *metav1.APIGroupList, err error) PreflightResult {
	const name = "cert-manager"
	if err != nil {
		return PreflightResult{Name: name, Level: LevelWarn, Detail: "could not list API groups"}
	}
	if !hasGroup(groups, "cert-manager.io") {
		return PreflightResult{Name: name, Level: LevelPass, Detail: "not detected — APIService cert via installer self-signed CA"}
	}
	// cert-manager is present, so the cert-manager TLS path wires the APIService
	// caBundle through cert-manager's CA injector (the inject-ca-from annotation).
	// That only works if the cainjector component is running; a CRD-only or
	// cainjector-disabled install would leave caBundle empty and the aggregated
	// API would never become Available. Surface that as a confirmable warning.
	if !cainjectorRunning(ctx, c) {
		return PreflightResult{Name: name, Level: LevelWarn,
			Detail: "cert-manager.io present but the cainjector component was not found",
			Hint:   "cert-manager's CA injector populates the APIService caBundle; install cainjector or pass --tls=self-signed"}
	}
	return PreflightResult{Name: name, Level: LevelPass, Detail: "detected — APIService cert via cert-manager"}
}

// cainjectorRunning reports whether a cert-manager cainjector Deployment is
// present (any namespace), keyed off its standard component label. A List error
// is treated as "present" so a transient/RBAC hiccup never blocks the install
// on a heuristic check.
func cainjectorRunning(ctx context.Context, c client.Client) bool {
	var deps appsv1.DeploymentList
	if err := c.List(ctx, &deps, client.MatchingLabels{"app.kubernetes.io/component": "cainjector"}); err != nil {
		return true
	}
	return len(deps.Items) > 0
}

func checkMetricsServer(groups *metav1.APIGroupList, err error) PreflightResult {
	const name = "metrics-server"
	if err != nil {
		return PreflightResult{Name: name, Level: LevelWarn, Detail: "could not list API groups"}
	}
	if hasGroup(groups, "metrics.k8s.io") {
		return PreflightResult{Name: name, Level: LevelPass, Detail: "present"}
	}
	return PreflightResult{Name: name, Level: LevelWarn, Detail: "absent",
		Hint: "optional; install metrics-server for `kubectl top` and HPA support"}
}

func checkExistingInstall(ctx context.Context, c client.Client, p Profile, upgrade bool) PreflightResult {
	const name = "Existing clrk install"
	exists, version, err := DetectInstall(ctx, c, p.Namespace)
	if err != nil {
		return PreflightResult{Name: name, Level: LevelWarn, Detail: "could not check: " + err.Error()}
	}
	if !exists {
		if upgrade {
			// runUpgrade already requires an existing install before preflight, so
			// this is effectively unreachable — surface it plainly rather than as a
			// reassuring PASS if it ever is.
			return PreflightResult{Name: name, Level: LevelWarn, Detail: "none in namespace " + p.Namespace,
				Hint: "nothing to upgrade in this namespace; use `clrk install`"}
		}
		return PreflightResult{Name: name, Level: LevelPass, Detail: "none in namespace " + p.Namespace}
	}
	detail := "controller-manager already present in " + p.Namespace
	if version != "" {
		detail += " (version " + version + ")"
	}
	if upgrade {
		// On upgrade an existing install is the expected precondition, not a
		// warning — flagging it WARN would add a spurious confirm whose hint tells
		// the operator to run the very command already in progress.
		return PreflightResult{Name: name, Level: LevelPass, Detail: detail + " — will be upgraded"}
	}
	return PreflightResult{Name: name, Level: LevelWarn, Detail: detail,
		Hint: "an install already exists; use `clrk upgrade` to change versions, or re-run install to reconcile"}
}

// --- helpers ---

func parseMinor(minor string) (int, error) {
	// Server minor can carry a "+" suffix (e.g. "29+") on patched distros.
	trimmed := strings.TrimRight(minor, "+")
	// And occasionally non-numeric trailers; keep the leading digits.
	end := 0
	for end < len(trimmed) && trimmed[end] >= '0' && trimmed[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0, fmt.Errorf("no numeric minor in %q", minor)
	}
	return strconv.Atoi(trimmed[:end])
}

func hasGroup(groups *metav1.APIGroupList, name string) bool {
	if groups == nil {
		return false
	}
	for _, g := range groups.Groups {
		if g.Name == name {
			return true
		}
	}
	return false
}

func isDefaultStorageClass(sc storagev1.StorageClass) bool {
	return sc.Annotations["storageclass.kubernetes.io/is-default-class"] == "true" ||
		sc.Annotations["storageclass.beta.kubernetes.io/is-default-class"] == "true"
}
