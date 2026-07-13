package egressmetrics

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/apoxy-dev/clrk/internal/apiserver/chsql"
	"github.com/apoxy-dev/clrk/internal/chwriter"
	"github.com/apoxy-dev/clrk/internal/otelemit"

	metricsv1 "github.com/apoxy-dev/clrk/api/metrics/v1alpha1"
)

// sqlString / dt64Nano alias the shared chsql helpers so the query
// builders below read naturally while the literal-escaping and
// DateTime64 logic stays single-sourced with the invocation, telemetry,
// and agentmetrics read models (see internal/apiserver/chsql).
var (
	sqlString = chsql.String
	dt64Nano  = chsql.DateTime64Nano
)

// routeUsage is one (gateway, route) rollup row over the window. A row
// with an empty routeKind is the gateway's unrouted traffic (a request
// that matched no AIProviderRoute / MCPRoute, e.g. allowed by an
// allow-all default policy); it counts toward the gateway totals but
// belongs to no listener entry.
type routeUsage struct {
	egRef     string
	routeKind string
	routeNS   string
	routeName string
	requests  uint64
	status2xx uint64
	status4xx uint64
	status5xx uint64
}

// key identifies the route within one gateway's row set. Empty for the
// unrouted row.
func (r routeUsage) key() string {
	if r.routeKind == "" {
		return ""
	}
	return r.routeKind + "/" + r.routeNS + "/" + r.routeName
}

// routeKey builds the routeUsage key for a route CR, matching key().
func routeKey(kind, namespace, name string) string {
	return kind + "/" + namespace + "/" + name
}

// scanBody builds the one GROUP BY scan behind List and Get: L7 egress
// exchanges (egress ext_proc spans carrying a response status) bucketed
// by gateway and matched route. Route identity prefers the MCPRoute
// attribution: the MCP parse is the more specific match and an MCP
// exchange may also carry the AIProviderRoute attrs of the provider it
// rode through.
//
// TCP/L4 records (egress.tcp spans) are deliberately out: they carry no
// response status and no route attribution, so a "requests" count over
// them would be a connection count silently mixed into an HTTP metric.
// toString pins the LowCardinality EGRef to a plain String for ch-go's
// strict scan type-check (same normalization as agentmetrics).
func scanBody(egPin, namespacePin string, since, until time.Time) string {
	where := []string{
		"Component = " + sqlString(otelemit.ComponentEgressExtproc),
		"mapContains(SpanAttributes, 'http.response.status_code')",
		"Timestamp >= " + dt64Nano(since.UnixNano()),
		"Timestamp < " + dt64Nano(until.UnixNano()),
	}
	switch {
	case egPin != "":
		where = append(where, "EGRef = "+sqlString(egPin))
	case namespacePin != "":
		// EGRef is "<ns>/<name>" and a namespace is a DNS label (no "/"),
		// so the prefix pin is exact.
		where = append(where, "startsWith(EGRef, "+sqlString(namespacePin+"/")+")")
	}
	// CAST pins the gateway column to a plain String: EGRef is
	// LowCardinality(String), toString over it is a no-op (still
	// LowCardinality on the wire), and ch-go's scan type-check is
	// strict. The output alias must also differ from the inner alias
	// (gateway, not eg) or ClickHouse resolves it to the raw inner
	// column, dropping the cast.
	return fmt.Sprintf(
		"SELECT CAST(eg, 'String') AS gateway, route_kind, route_ns, route_name, "+
			"count() AS requests, "+
			"countIf(st >= 200 AND st < 300) AS status_2xx, "+
			"countIf(st >= 400 AND st < 500) AS status_4xx, "+
			"countIf(st >= 500) AS status_5xx "+
			"FROM ("+
			"SELECT EGRef AS eg, "+
			"multiIf(SpanAttributes['clrk.mcproute.name'] != '', 'MCPRoute', "+
			"SpanAttributes['clrk.aiproviderroute.name'] != '', 'AIProviderRoute', '') AS route_kind, "+
			"multiIf(SpanAttributes['clrk.mcproute.name'] != '', SpanAttributes['clrk.mcproute.namespace'], "+
			"SpanAttributes['clrk.aiproviderroute.name'] != '', SpanAttributes['clrk.aiproviderroute.namespace'], '') AS route_ns, "+
			"multiIf(SpanAttributes['clrk.mcproute.name'] != '', SpanAttributes['clrk.mcproute.name'], "+
			"SpanAttributes['clrk.aiproviderroute.name'] != '', SpanAttributes['clrk.aiproviderroute.name'], '') AS route_name, "+
			"toInt64OrZero(SpanAttributes['http.response.status_code']) AS st "+
			"FROM %s.%s WHERE %s"+
			") GROUP BY eg, route_kind, route_ns, route_name "+
			"ORDER BY eg, route_kind, route_ns, route_name",
		chwriter.Database, chwriter.TracesTable, strings.Join(where, " AND "),
	)
}

// scanUsage runs body and decodes one routeUsage per returned row.
func scanUsage(ctx context.Context, pool Doer, body string) ([]routeUsage, error) {
	var (
		eg        proto.ColStr
		routeKind proto.ColStr
		routeNS   proto.ColStr
		routeName proto.ColStr
		requests  proto.ColUInt64
		s2xx      proto.ColUInt64
		s4xx      proto.ColUInt64
		s5xx      proto.ColUInt64
	)
	var out []routeUsage
	if err := pool.Do(ctx, ch.Query{
		Body: body,
		Result: proto.Results{
			{Name: "gateway", Data: &eg},
			{Name: "route_kind", Data: &routeKind},
			{Name: "route_ns", Data: &routeNS},
			{Name: "route_name", Data: &routeName},
			{Name: "requests", Data: &requests},
			{Name: "status_2xx", Data: &s2xx},
			{Name: "status_4xx", Data: &s4xx},
			{Name: "status_5xx", Data: &s5xx},
		},
		// Drain every block: ClickHouse splits a result across blocks when
		// the SELECT reads many parts at once (the steady state, since
		// chwriter's concurrent inserts leave unmerged parts), and without
		// an OnResult ch-go fails the second block with "no OnResult
		// provided". Columns reset per block, so copy out before the next.
		OnResult: func(context.Context, proto.Block) error {
			n := eg.Rows()
			for i := 0; i < n; i++ {
				out = append(out, routeUsage{
					egRef:     eg.Row(i),
					routeKind: routeKind.Row(i),
					routeNS:   routeNS.Row(i),
					routeName: routeName.Row(i),
					requests:  requests.Row(i),
					status2xx: s2xx.Row(i),
					status4xx: s4xx.Row(i),
					status5xx: s5xx.Row(i),
				})
			}
			return nil
		},
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// usageList projects one row's counters into the wire UsageList.
func (r routeUsage) usageList() metricsv1.UsageList {
	return usageOf(r.requests, r.status2xx, r.status4xx, r.status5xx)
}

// usageOf builds a UsageList from the four counters. CH aggregates are
// UInt64; resource.Quantity is int64-valued, so clamp rather than let an
// (unrealistic but unguarded) sum above MaxInt64 wrap negative.
func usageOf(requests, s2xx, s4xx, s5xx uint64) metricsv1.UsageList {
	qu := func(n uint64) resource.Quantity {
		if n > math.MaxInt64 {
			n = math.MaxInt64
		}
		return *resource.NewQuantity(int64(n), resource.DecimalSI)
	}
	return metricsv1.UsageList{
		metricsv1.UsageRequests:  qu(requests),
		metricsv1.UsageStatus2xx: qu(s2xx),
		metricsv1.UsageStatus4xx: qu(s4xx),
		metricsv1.UsageStatus5xx: qu(s5xx),
	}
}

// sumUsage adds b's counters into a (both built by usageOf, so the four
// keys are always present).
func sumUsage(a, b metricsv1.UsageList) metricsv1.UsageList {
	out := metricsv1.UsageList{}
	for _, k := range []string{
		metricsv1.UsageRequests,
		metricsv1.UsageStatus2xx,
		metricsv1.UsageStatus4xx,
		metricsv1.UsageStatus5xx,
	} {
		av, bv := a[k], b[k]
		av.Add(bv)
		out[k] = av
	}
	return out
}
