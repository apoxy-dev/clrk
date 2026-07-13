package egressmetrics

import (
	"sort"

	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	metricsv1 "github.com/apoxy-dev/clrk/api/metrics/v1alpha1"
)

// Route CRD kinds carried on EgressRouteMetrics.Kind and used as the
// leading routeUsage key segment. They match the CRD kind names so the
// snapshot reads like the topology it summarizes.
const (
	kindAIProviderRoute = "AIProviderRoute"
	kindMCPRoute        = "MCPRoute"
)

// attachedRoute is one route CR attached to a gateway: its identity plus
// the listener names its parentRefs select. An empty sections set means
// "every listener" (a parentRef with no sectionName attaches to all of
// them, per Gateway API semantics; clrk's data plane matches routes
// gateway-wide today, so this is also how traffic actually flows).
type attachedRoute struct {
	kind      string
	namespace string
	name      string
	sections  map[string]bool // nil/empty => all listeners
}

// attachesTo inspects refs and reports whether they attach the route
// (routeNS-local by default) to the gateway (egNS, egName), collecting
// the sectionNames of the matching refs. Mirrors the data plane's
// routeAttachesTo / mcpRouteAttachesTo: the group and kind must name a
// clrk EgressGateway explicitly.
func attachesTo(refs []gwapiv1.ParentReference, routeNS, egNS, egName string) (bool, map[string]bool) {
	attached := false
	sections := map[string]bool{}
	all := false
	for _, ref := range refs {
		if !refIsClrkEgressGateway(ref) || string(ref.Name) != egName {
			continue
		}
		ns := routeNS
		if ref.Namespace != nil && *ref.Namespace != "" {
			ns = string(*ref.Namespace)
		}
		if ns != egNS {
			continue
		}
		attached = true
		if ref.SectionName == nil || *ref.SectionName == "" {
			all = true
			continue
		}
		sections[string(*ref.SectionName)] = true
	}
	if !attached {
		return false, nil
	}
	if all {
		return true, nil
	}
	return true, sections
}

// refIsClrkEgressGateway reports whether the ref's group+kind name a
// clrk EgressGateway. Both must be explicit — Gateway API defaults an
// absent group/kind to gateway.networking.k8s.io/Gateway, which is not
// ours.
func refIsClrkEgressGateway(ref gwapiv1.ParentReference) bool {
	group := ""
	if ref.Group != nil {
		group = string(*ref.Group)
	}
	kind := ""
	if ref.Kind != nil {
		kind = string(*ref.Kind)
	}
	return group == clrkv1alpha1.GroupName && kind == "EgressGateway"
}

// attachedRoutes filters the cluster's route CRs down to the ones
// attached to the gateway. EgressL4Routes are deliberately absent: the
// snapshot counts L7 HTTP exchanges, and L4 sessions carry neither a
// response status nor a route attribution to count under.
func attachedRoutes(eg *clrkv1alpha1.EgressGateway, aprs []clrkv1alpha1.AIProviderRoute, mcps []clrkv1alpha1.MCPRoute) []attachedRoute {
	var out []attachedRoute
	for i := range aprs {
		r := &aprs[i]
		if ok, sections := attachesTo(r.Spec.ParentRefs, r.Namespace, eg.Namespace, eg.Name); ok {
			out = append(out, attachedRoute{kind: kindAIProviderRoute, namespace: r.Namespace, name: r.Name, sections: sections})
		}
	}
	for i := range mcps {
		r := &mcps[i]
		if ok, sections := attachesTo(r.Spec.ParentRefs, r.Namespace, eg.Namespace, eg.Name); ok {
			out = append(out, attachedRoute{kind: kindMCPRoute, namespace: r.Namespace, name: r.Name, sections: sections})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].kind != out[j].kind {
			return out[i].kind < out[j].kind
		}
		if out[i].namespace != out[j].namespace {
			return out[i].namespace < out[j].namespace
		}
		return out[i].name < out[j].name
	})
	return out
}

// buildListeners assembles the per-listener breakdown: the gateway's
// declared listeners in spec order, each carrying its attached routes
// (in the stable attachedRoutes order) with the routes' scanned usage,
// zeros for a route with no traffic in the window. Egress spans carry
// route identity, not listener identity, so a route attached to several
// listeners contributes its full usage to each (see the API type's
// double-counting note); the caller derives the gateway totals from the
// raw rows, never from these entries.
func buildListeners(eg *clrkv1alpha1.EgressGateway, routes []attachedRoute, usageByRoute map[string]metricsv1.UsageList) []metricsv1.EgressListenerMetrics {
	out := make([]metricsv1.EgressListenerMetrics, 0, len(eg.Spec.Listeners))
	for _, l := range eg.Spec.Listeners {
		lm := metricsv1.EgressListenerMetrics{Name: l.Name, Usage: usageOf(0, 0, 0, 0)}
		for _, r := range routes {
			if len(r.sections) > 0 && !r.sections[l.Name] {
				continue
			}
			u, ok := usageByRoute[routeKey(r.kind, r.namespace, r.name)]
			if !ok {
				u = usageOf(0, 0, 0, 0)
			}
			lm.Routes = append(lm.Routes, metricsv1.EgressRouteMetrics{Kind: r.kind, Name: r.name, Usage: u})
			lm.Usage = sumUsage(lm.Usage, u)
		}
		out = append(out, lm)
	}
	return out
}
