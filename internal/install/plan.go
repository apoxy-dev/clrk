package install

import (
	"context"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/yaml"
)

// Action is the effect applying an object will have on the cluster.
type Action int

const (
	ActionCreate Action = iota
	ActionUpdate
	ActionUnchanged
	ActionUnknown
)

func (a Action) String() string {
	switch a {
	case ActionCreate:
		return "create"
	case ActionUpdate:
		return "update"
	case ActionUnchanged:
		return "unchanged"
	default:
		return "unknown"
	}
}

// ResourcePlan is the planned effect of applying one object, computed by a
// server-side-apply dry-run diffed against the live object. Risky flags
// cluster-wide or destructive changes the operator should see before
// confirming; Why explains it. Note carries a caveat (e.g. an API not yet
// served at plan time).
type ResourcePlan struct {
	Kind   string
	Name   string // "<ns>/<name>" or "<name>" for cluster-scoped
	Action Action
	Diff   string
	Risky  bool
	Why    string
	Note   string
}

// BuildPlan computes, for each object, what applying it would do — via an SSA
// dry-run compared against the live object. It never mutates the cluster.
// Objects whose API isn't served yet at plan time (e.g. the WorkerPool before
// the controller-manager is up) can't be dry-run-applied; they're reported as
// creates with a note rather than failing the whole plan.
func BuildPlan(ctx context.Context, c client.Client, fieldManager string, objs []client.Object) []ResourcePlan {
	plans := make([]ResourcePlan, 0, len(objs))
	for _, obj := range objs {
		plans = append(plans, planOne(ctx, c, fieldManager, obj))
	}
	return plans
}

func planOne(ctx context.Context, c client.Client, fieldManager string, obj client.Object) ResourcePlan {
	risky, why := classifyRisk(obj)
	plan := ResourcePlan{
		Kind:  kindOf(obj, c),
		Name:  nameOf(obj),
		Risky: risky,
		Why:   why,
	}

	dry := obj.DeepCopyObject().(client.Object)
	if err := c.Patch(ctx, dry, client.Apply, client.ForceOwnership, client.FieldOwner(fieldManager), client.DryRunAll); err != nil {
		if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			plan.Action = ActionCreate
			plan.Note = "API not served yet; created after controller-manager is up"
			return plan
		}
		plan.Action = ActionUnknown
		plan.Note = err.Error()
		return plan
	}

	gvk, gerr := apiutil.GVKForObject(obj, c.Scheme())
	if gerr != nil {
		plan.Action = ActionUnknown
		plan.Note = gerr.Error()
		return plan
	}
	rawLive, nerr := c.Scheme().New(gvk)
	if nerr != nil {
		plan.Action = ActionUnknown
		plan.Note = nerr.Error()
		return plan
	}
	live := rawLive.(client.Object)
	if err := c.Get(ctx, client.ObjectKeyFromObject(obj), live); err != nil {
		if apierrors.IsNotFound(err) {
			plan.Action = ActionCreate
			return plan
		}
		plan.Action = ActionUnknown
		plan.Note = err.Error()
		return plan
	}

	diff := diffObjects(live, dry)
	if diff == "" {
		plan.Action = ActionUnchanged
		return plan
	}
	plan.Action = ActionUpdate
	plan.Diff = diff
	plan.Risky = true // mutating an existing object is worth surfacing
	if plan.Why == "" {
		plan.Why = "updates an existing object"
	}
	return plan
}

// classifyRisk flags cluster-scoped objects whose (re)application has
// blast radius beyond the install namespace.
func classifyRisk(obj client.Object) (bool, string) {
	switch obj.GetObjectKind().GroupVersionKind().Kind {
	case "CustomResourceDefinition":
		return true, "cluster-wide CRD"
	case "ClusterRole", "ClusterRoleBinding":
		return true, "cluster-wide RBAC"
	case "APIService":
		return true, "aggregation-layer registration"
	default:
		return false, ""
	}
}

func nameOf(obj client.Object) string {
	if ns := obj.GetNamespace(); ns != "" {
		return ns + "/" + obj.GetName()
	}
	return obj.GetName()
}

func kindOf(obj client.Object, c client.Client) string {
	if k := obj.GetObjectKind().GroupVersionKind().Kind; k != "" {
		return k
	}
	if gvk, err := apiutil.GVKForObject(obj, c.Scheme()); err == nil {
		return gvk.Kind
	}
	return "Object"
}

// diffObjects returns a line diff of the meaningful fields of two objects,
// stripping server-managed noise so only real changes show. Empty means no
// meaningful difference.
func diffObjects(live, desired client.Object) string {
	a, err := sanitizedYAML(live)
	if err != nil {
		return "(diff unavailable: " + err.Error() + ")"
	}
	b, err := sanitizedYAML(desired)
	if err != nil {
		return "(diff unavailable: " + err.Error() + ")"
	}
	if a == b {
		return ""
	}
	return lineDiff(strings.Split(a, "\n"), strings.Split(b, "\n"))
}

// sanitizedYAML marshals obj to YAML after dropping server-managed fields that
// always differ (resourceVersion, uid, timestamps, managedFields, status, etc.)
// and would otherwise mask real changes.
func sanitizedYAML(obj client.Object) (string, error) {
	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return "", err
	}
	delete(u, "status")
	// TypeMeta is stripped by the typed client on a GET but present on the
	// dry-run result, so leaving apiVersion/kind in would show every object as a
	// spurious diff. They carry no real change (same GVK) — drop from both sides.
	delete(u, "apiVersion")
	delete(u, "kind")
	if md, ok := u["metadata"].(map[string]interface{}); ok {
		for _, k := range []string{
			"managedFields", "resourceVersion", "uid", "generation",
			"creationTimestamp", "selfLink", "ownerReferences",
			"finalizers", "annotations",
		} {
			delete(md, k)
		}
		// Keep only our labels; servers add their own (e.g. PVC binding labels).
		// Annotations are dropped wholesale above because controllers add many.
	}
	out, err := yaml.Marshal(u)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// lineDiff returns a compact diff of two line slices using an LCS so only
// changed lines are emitted (removed lines prefixed "-", added "+"). No
// surrounding context, which keeps an install plan readable.
func lineDiff(a, b []string) string {
	m, n := len(a), len(b)
	// lcs[i][j] = length of the longest common subsequence of a[i:] and b[j:].
	lcs := make([][]int, m+1)
	for i := range lcs {
		lcs[i] = make([]int, n+1)
	}
	for i := m - 1; i >= 0; i-- {
		for j := n - 1; j >= 0; j-- {
			if a[i] == b[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else if lcs[i+1][j] >= lcs[i][j+1] {
				lcs[i][j] = lcs[i+1][j]
			} else {
				lcs[i][j] = lcs[i][j+1]
			}
		}
	}
	var sb strings.Builder
	i, j := 0, 0
	for i < m && j < n {
		switch {
		case a[i] == b[j]:
			i++
			j++
		case lcs[i+1][j] >= lcs[i][j+1]:
			sb.WriteString("- " + a[i] + "\n")
			i++
		default:
			sb.WriteString("+ " + b[j] + "\n")
			j++
		}
	}
	for ; i < m; i++ {
		sb.WriteString("- " + a[i] + "\n")
	}
	for ; j < n; j++ {
		sb.WriteString("+ " + b[j] + "\n")
	}
	return sb.String()
}
