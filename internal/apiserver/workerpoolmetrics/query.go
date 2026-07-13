package workerpoolmetrics

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"k8s.io/apimachinery/pkg/api/resource"

	metricsv1 "github.com/apoxy-dev/clrk/api/metrics/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/apiserver/chsql"
	"github.com/apoxy-dev/clrk/internal/chwriter"
	"github.com/apoxy-dev/clrk/internal/otelemit"
)

// sqlString / dt64Nano alias the shared chsql helpers so the query
// builders read naturally while the literal-escaping and DateTime64 logic
// stays single-sourced with the invocation, telemetry, agentmetrics, and
// egressmetrics read models (see internal/apiserver/chsql).
var (
	sqlString = chsql.String
	dt64Nano  = chsql.DateTime64Nano
)

// poolUsage is one pool's dispatch rollup over the window, as scanned from
// the conditional-aggregate query. namespace/pool identify the row; the
// remaining fields are the windowed aggregates. The CR-status gauges are
// NOT here -- they are point-in-time reads joined from the WorkerPool CR.
type poolUsage struct {
	namespace     string
	pool          string
	invocations   uint64
	errors        uint64
	dispatchP50Ms float64
	dispatchP99Ms float64
}

// scanBody builds the one GROUP BY scan behind List and Get: the pool's
// ingress.dispatch spans (the per-request routing span the ingress
// ext_proc emits) bucketed by pool and namespace. clrk.worker.pool is
// stamped on that span only once a ready revision is resolved and worker
// selection begins, so a request rejected earlier (unknown TaskAgent, no
// ready revision, malformed) carries no pool and is excluded by the
// worker.pool-nonempty predicate -- it belongs to no pool. Within the pool's
// spans, a StatusCode of Error marks a pool-attributable dispatch failure
// (at cluster MaxConcurrent, no ready worker); every other span is a
// successful dispatch.
//
// The pool's namespace is the dispatch span's agent.namespace: an agent
// references a WorkerPool by name in its own namespace, so the two always
// share it. Duration is the span wall time in nanoseconds; /1e6 yields the
// dispatch-latency milliseconds, and ifNotFinite collapses the
// quantile-over-empty NaN (a pool with no dispatches in the window) to 0.
func scanBody(namespacePin, poolPin string, since, until time.Time) string {
	where := []string{
		"Component = " + sqlString(otelemit.ComponentIngressExtproc),
		"SpanName = " + sqlString(otelemit.SpanNameIngressDispatch),
		"SpanAttributes['" + otelemit.AttrWorkerPool + "'] != ''",
		"Timestamp >= " + dt64Nano(since.UnixNano()),
		"Timestamp < " + dt64Nano(until.UnixNano()),
	}
	if namespacePin != "" {
		where = append(where, "SpanAttributes['"+otelemit.AttrAgentNamespace+"'] = "+sqlString(namespacePin))
	}
	if poolPin != "" {
		where = append(where, "SpanAttributes['"+otelemit.AttrWorkerPool+"'] = "+sqlString(poolPin))
	}
	// pool and ns are plain-String map lookups (not LowCardinality), so
	// they group and scan as String without the CAST the egressmetrics
	// EGRef column needs. StatusCode is the span status the ingress filter
	// flips to Error on any >=400 dispatch outcome.
	return fmt.Sprintf(
		"SELECT pool, ns, "+
			"countIf(status != 'Error') AS invocations, "+
			"countIf(status = 'Error') AS errors, "+
			"ifNotFinite(quantile(0.5)(dispatch_ms), 0) AS p50_ms, "+
			"ifNotFinite(quantile(0.99)(dispatch_ms), 0) AS p99_ms "+
			"FROM ("+
			"SELECT SpanAttributes['"+otelemit.AttrWorkerPool+"'] AS pool, "+
			"SpanAttributes['"+otelemit.AttrAgentNamespace+"'] AS ns, "+
			"StatusCode AS status, "+
			"Duration / 1e6 AS dispatch_ms "+
			"FROM %s.%s WHERE %s"+
			") GROUP BY pool, ns ORDER BY ns, pool",
		chwriter.Database, chwriter.TracesTable, strings.Join(where, " AND "),
	)
}

// scanUsage runs body and decodes one poolUsage per returned row.
func scanUsage(ctx context.Context, pool Doer, body string) ([]poolUsage, error) {
	var (
		poolCol proto.ColStr
		nsCol   proto.ColStr
		inv     proto.ColUInt64
		errs    proto.ColUInt64
		p50     proto.ColFloat64
		p99     proto.ColFloat64
	)
	var out []poolUsage
	if err := pool.Do(ctx, ch.Query{
		Body: body,
		Result: proto.Results{
			{Name: "pool", Data: &poolCol},
			{Name: "ns", Data: &nsCol},
			{Name: "invocations", Data: &inv},
			{Name: "errors", Data: &errs},
			{Name: "p50_ms", Data: &p50},
			{Name: "p99_ms", Data: &p99},
		},
		// Drain every block: ClickHouse splits a result across blocks when
		// the SELECT reads many parts at once (the steady state, since
		// chwriter's concurrent inserts leave unmerged parts), and without
		// an OnResult ch-go fails the second block with "no OnResult
		// provided". Columns reset per block, so copy out before the next.
		OnResult: func(context.Context, proto.Block) error {
			n := poolCol.Rows()
			for i := 0; i < n; i++ {
				out = append(out, poolUsage{
					namespace:     nsCol.Row(i),
					pool:          poolCol.Row(i),
					invocations:   inv.Row(i),
					errors:        errs.Row(i),
					dispatchP50Ms: p50.Row(i),
					dispatchP99Ms: p99.Row(i),
				})
			}
			return nil
		},
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// usageList projects the scanned dispatch aggregates into the wire
// UsageList. The CR-status gauges are added by the storage's CR join, not
// here. Latency percentiles are rounded to whole milliseconds.
func (u poolUsage) usageList() metricsv1.UsageList {
	// CH count aggregates are UInt64; resource.Quantity is int64-valued.
	// Clamp rather than let an (unrealistic but unguarded) count above
	// MaxInt64 wrap to a negative usage value on the wire.
	qu := func(n uint64) resource.Quantity {
		if n > math.MaxInt64 {
			n = math.MaxInt64
		}
		return *resource.NewQuantity(int64(n), resource.DecimalSI)
	}
	q := func(n int64) resource.Quantity { return *resource.NewQuantity(n, resource.DecimalSI) }
	return metricsv1.UsageList{
		metricsv1.UsageInvocations:   qu(u.invocations),
		metricsv1.UsageErrors:        qu(u.errors),
		metricsv1.UsageDispatchP50Ms: q(int64(math.Round(u.dispatchP50Ms))),
		metricsv1.UsageDispatchP99Ms: q(int64(math.Round(u.dispatchP99Ms))),
	}
}
