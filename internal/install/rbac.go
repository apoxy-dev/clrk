package install

import (
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RBAC object names. The dev shortcut binds cluster-admin under a clrk-dev-
// prefixed binding; the scoped path installs purpose-built ClusterRoles.
const (
	cmClusterRoleName        = "clrk-controller-manager"
	cmClusterRoleBindingName = "clrk-controller-manager"
	cmDevBindingName         = "clrk-dev-controller-manager"

	workerClusterRoleName        = "clrk-worker"
	workerClusterRoleBindingName = "clrk-worker"
	workerDevBindingName         = "clrk-dev-worker"
)

// controllerManagerRBAC returns the RBAC objects binding the cm ServiceAccount.
// Scoped builds a purpose-built ClusterRole (narrower than cluster-admin);
// otherwise it reproduces the dev cluster-admin binding. The cm is an
// aggregated apiserver that also supervises Envoy Gateway in-process under this
// ServiceAccount, so the scoped role unions the reconciler RBAC markers, the
// standard aggregated-apiserver needs (delegated authn/authz, the
// extension-apiserver-authentication configmap), and the EG control-plane needs.
func controllerManagerRBAC(p Profile) []rbacObject {
	if !p.RBACScoped {
		return []rbacObject{{binding: clusterAdminBinding(cmDevBindingName, ControllerManagerName, p.Namespace)}}
	}
	role := &rbacv1.ClusterRole{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{Name: cmClusterRoleName},
		Rules: []rbacv1.PolicyRule{
			// clrk's own API group (the aggregated resources the reconcilers own).
			rule([]string{"clrk.apoxy.dev"}, []string{"*"}, "get", "list", "watch", "create", "update", "patch", "delete"),
			// Gateway API + Envoy Gateway (ingress + egress reconcilers).
			rule([]string{"gateway.networking.k8s.io"}, []string{"*"}, "get", "list", "watch", "create", "update", "patch", "delete"),
			rule([]string{"gateway.envoyproxy.io"}, []string{"*"}, "get", "list", "watch", "create", "update", "patch", "delete"),
			// Core objects the reconcilers + EG manage.
			rule([]string{""}, []string{"secrets"}, "get", "list", "watch", "create", "update", "patch", "delete"),
			rule([]string{""}, []string{"services"}, "get", "list", "watch", "create", "update", "patch", "delete"),
			rule([]string{""}, []string{"configmaps"}, "get", "list", "watch", "create", "update", "patch", "delete"),
			// EG provisions a per-proxy ServiceAccount via server-side apply, so the
			// cm (which supervises the EG control plane under this SA) must create/
			// update them, not just read.
			rule([]string{""}, []string{"serviceaccounts"}, "get", "list", "watch", "create", "update", "patch", "delete"),
			rule([]string{""}, []string{"pods"}, "get", "list", "watch"),
			rule([]string{""}, []string{"endpoints"}, "get", "list", "watch"),
			rule([]string{""}, []string{"namespaces"}, "get", "list", "watch"),
			rule([]string{""}, []string{"events"}, "create", "patch"),
			// Workloads the WorkerPool + EG reconcilers create. EG provisions its
			// proxy data plane as a Deployment or DaemonSet (with an optional PDB +
			// HPA), so the cm manages daemonsets/poddisruptionbudgets/
			// horizontalpodautoscalers too, not only deployments.
			rule([]string{"apps"}, []string{"deployments", "daemonsets", "replicasets"}, "get", "list", "watch", "create", "update", "patch", "delete"),
			rule([]string{"policy"}, []string{"poddisruptionbudgets"}, "get", "list", "watch", "create", "update", "patch", "delete"),
			rule([]string{"autoscaling"}, []string{"horizontalpodautoscalers"}, "get", "list", "watch", "create", "update", "patch", "delete"),
			// Endpoint resolution (dispatch IP join).
			rule([]string{"discovery.k8s.io"}, []string{"endpointslices"}, "get", "list", "watch"),
			// The cm installs/verifies the Gateway-API + EG CRD bundles.
			rule([]string{"apiextensions.k8s.io"}, []string{"customresourcedefinitions"}, "get", "list", "watch", "create", "update", "patch"),
			// The cm reads its own aggregated APIService registration.
			rule([]string{"apiregistration.k8s.io"}, []string{"apiservices"}, "get", "list", "watch"),
			// Standard aggregated-apiserver delegated authn/authz.
			rule([]string{"authentication.k8s.io"}, []string{"tokenreviews"}, "create"),
			rule([]string{"authorization.k8s.io"}, []string{"subjectaccessreviews"}, "create"),
			// Coordination for the embedded apiserver / optional leader election.
			rule([]string{"coordination.k8s.io"}, []string{"leases"}, "get", "list", "watch", "create", "update", "patch", "delete"),
		},
	}
	return []rbacObject{
		{role: role},
		{binding: roleBinding(cmClusterRoleBindingName, cmClusterRoleName, ControllerManagerName, p.Namespace)},
	}
}

// workerRBAC returns the RBAC objects binding the worker ServiceAccount. Scoped
// grants only what cmd/worker reconciles: the clrk CRDs (read + status) and
// pods (read); otherwise the dev cluster-admin binding.
func workerRBAC(p Profile) []rbacObject {
	if !p.RBACScoped {
		return []rbacObject{{binding: clusterAdminBinding(workerDevBindingName, WorkerAccountName, p.WorkerNamespace)}}
	}
	role := &rbacv1.ClusterRole{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{Name: workerClusterRoleName},
		Rules: []rbacv1.PolicyRule{
			rule([]string{"clrk.apoxy.dev"}, []string{"*"}, "get", "list", "watch", "update", "patch"),
			rule([]string{""}, []string{"pods"}, "get", "list", "watch"),
		},
	}
	return []rbacObject{
		{role: role},
		{binding: roleBinding(workerClusterRoleBindingName, workerClusterRoleName, WorkerAccountName, p.WorkerNamespace)},
	}
}

// rbacObject is either a ClusterRole or a ClusterRoleBinding (exactly one set),
// so the builder can append a variable-length RBAC set to the object list.
type rbacObject struct {
	role    *rbacv1.ClusterRole
	binding *rbacv1.ClusterRoleBinding
}

// flattenRBAC expands rbacObjects into client.Objects, role before binding so an
// apply ordering creates the ClusterRole ahead of the ClusterRoleBinding that
// references it.
func flattenRBAC(objs []rbacObject) []client.Object {
	out := make([]client.Object, 0, len(objs))
	for _, o := range objs {
		if o.role != nil {
			out = append(out, o.role)
		}
		if o.binding != nil {
			out = append(out, o.binding)
		}
	}
	return out
}

func rule(groups, resources []string, verbs ...string) rbacv1.PolicyRule {
	return rbacv1.PolicyRule{APIGroups: groups, Resources: resources, Verbs: verbs}
}

func clusterAdminBinding(name, sa, ns string) *rbacv1.ClusterRoleBinding {
	return roleBinding(name, "cluster-admin", sa, ns)
}

func roleBinding(name, roleName, sa, ns string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      sa,
			Namespace: ns,
		}},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     roleName,
		},
	}
}
