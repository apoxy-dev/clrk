package agentmetrics

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
	"github.com/apoxy-dev/clrk/internal/chwriter"
)

// agentUsage is one agent's rollup over the window, as scanned from the
// conditional-aggregate query. agent/namespace identify the row; the
// remaining fields are the windowed aggregates.
type agentUsage struct {
	agent        string
	namespace    string
	invocations  uint64
	errors       uint64
	inputTokens  uint64
	outputTokens uint64
	toolCalls    uint64
	latencyP50Ms float64
	latencyP99Ms float64
}

// outerAggregates rolls the per-invocation inner rows (perInvocationBody)
// up to one row per agent: the list path adds GROUP BY a, ns; the get
// path leaves it ungrouped (an aggregate with no GROUP BY always yields
// exactly one row, zeros over an empty set).
//
// invocations counts only inner rows with a non-empty invocation id, and
// the latency percentiles are taken only over those rows' wall-clocks: a
// DaemonAgent is not request-invoked, so its spans carry no
// invocation.id and collapse into a single empty-invocation-id inner row
// -- its token/tool/error totals still sum (so daemon usage is reported),
// but it contributes 0 invocations and no latency. The count/token
// columns sum across ALL inner rows so that empty-id usage is not
// dropped. ifNotFinite
// collapses the quantile-over-empty NaN (e.g. a daemon, or an agent with
// no invoked spans) to 0. toString pins the agent to a plain String:
// any() over the LowCardinality column can return String or
// LowCardinality(String) depending on the CH version, and ch-go's scan
// type-check is strict, so we normalize and scan a plain ColStr.
const outerAggregates = "toString(any(a)) AS agent, " +
	"any(ns) AS namespace, " +
	"countIf(inv != '') AS invocations, " +
	"sum(errs) AS errors, " +
	"sum(in_tok) AS input_tokens, " +
	"sum(out_tok) AS output_tokens, " +
	"sum(tool_calls) AS tool_calls, " +
	"ifNotFinite(quantileIf(0.5)(wall_ms, inv != ''), 0) AS p50_ms, " +
	"ifNotFinite(quantileIf(0.99)(wall_ms, inv != ''), 0) AS p99_ms"

// perInvocationBody is the inner per-invocation rollup: one row per
// (agent, namespace, invocation), carrying that invocation's trace
// wall-clock and its windowed sums. The outer query aggregates these
// across invocations.
//
// Latency MUST be a per-invocation wall-clock, NOT a single span's
// duration. The ingress.dispatch span is request-header-only: the
// ingress ext_proc picks a worker, rewrites :authority, and its stream
// EOFs in ~1ms, so that span's duration is dispatch overhead, not the
// end-to-end request -- the agent run and the streamed response flow
// back through Envoy and never re-enter the ext_proc. The true
// per-invocation latency is the trace wall-clock: the first span's start
// to the last span's end across everything sharing the invocation id
// (ingress.dispatch start through the final egress call's end).
// Timestamp and Duration are nanoseconds; the difference /1e6 yields
// milliseconds. The grouping is over ALL of the agent's spans, including
// any with an empty invocation.id (a DaemonAgent's spans, which are not
// request-scoped): those collapse into one empty-id row whose token/tool/
// error totals still count, while outerAggregates excludes that row from
// the invocation count and the latency percentiles.
func perInvocationBody(agentKind, namespace, agent string, since, until time.Time) string {
	return fmt.Sprintf(
		"SELECT Agent AS a, "+
			"SpanAttributes['agent.namespace'] AS ns, "+
			"InvocationId AS inv, "+
			"(max(toUnixTimestamp64Nano(Timestamp) + Duration) - min(toUnixTimestamp64Nano(Timestamp))) / 1e6 AS wall_ms, "+
			"countIf(StatusCode = 'Error') AS errs, "+
			"sum(toUInt64OrZero(SpanAttributes['gen_ai.usage.input_tokens'])) AS in_tok, "+
			"sum(toUInt64OrZero(SpanAttributes['gen_ai.usage.output_tokens'])) AS out_tok, "+
			"countIf(mapContains(SpanAttributes, 'mcp.method')) AS tool_calls "+
			"FROM %s.%s WHERE %s GROUP BY a, ns, inv",
		chwriter.Database, chwriter.TracesTable,
		scopeWhere(agentKind, namespace, agent, since, until),
	)
}

// scopeWhere builds the WHERE predicates: the server-enforced agent kind
// + window, plus an optional namespace and single-agent pin. Every
// request-derived value is sqlString-escaped.
func scopeWhere(agentKind, namespace, agent string, since, until time.Time) string {
	cl := []string{
		"SpanAttributes['agent.kind'] = " + sqlString(agentKind),
		"Timestamp >= " + dt64Nano(since.UnixNano()),
		"Timestamp < " + dt64Nano(until.UnixNano()),
	}
	if namespace != "" {
		cl = append(cl, "SpanAttributes['agent.namespace'] = "+sqlString(namespace))
	}
	if agent != "" {
		cl = append(cl, "Agent = "+sqlString(agent))
	}
	return strings.Join(cl, " AND ")
}

// listBody aggregates every agent of the kind in scope, one row per
// (agent, namespace) so a cluster-scoped list never collides two agents
// that share a name across namespaces. The outer GROUP BY a, ns rolls up
// the per-invocation inner rows.
func listBody(agentKind, namespace string, since, until time.Time) string {
	return fmt.Sprintf(
		"SELECT %s FROM (%s) GROUP BY a, ns ORDER BY ns, a",
		outerAggregates, perInvocationBody(agentKind, namespace, "", since, until),
	)
}

// getBody aggregates a single named agent. The un-grouped outer aggregate
// always returns exactly one row: zeros (and an empty scanned agent /
// namespace, which the caller overrides with the requested identity)
// when the agent has no spans, hence no inner rows, in the window.
func getBody(agentKind, namespace, agent string, since, until time.Time) string {
	return fmt.Sprintf(
		"SELECT %s FROM (%s)",
		outerAggregates, perInvocationBody(agentKind, namespace, agent, since, until),
	)
}

// scanUsage runs body and decodes one agentUsage per returned row.
func scanUsage(ctx context.Context, pool Doer, body string) ([]agentUsage, error) {
	var (
		agent     proto.ColStr
		namespace proto.ColStr
		inv       proto.ColUInt64
		errs      proto.ColUInt64
		inTok     proto.ColUInt64
		outTok    proto.ColUInt64
		tools     proto.ColUInt64
		p50       proto.ColFloat64
		p99       proto.ColFloat64
	)
	var out []agentUsage
	if err := pool.Do(ctx, ch.Query{
		Body: body,
		Result: proto.Results{
			{Name: "agent", Data: &agent},
			{Name: "namespace", Data: &namespace},
			{Name: "invocations", Data: &inv},
			{Name: "errors", Data: &errs},
			{Name: "input_tokens", Data: &inTok},
			{Name: "output_tokens", Data: &outTok},
			{Name: "tool_calls", Data: &tools},
			{Name: "p50_ms", Data: &p50},
			{Name: "p99_ms", Data: &p99},
		},
		// Drain every block: ClickHouse splits a result across blocks when
		// the SELECT reads many parts at once (the steady state, since
		// chwriter's concurrent inserts leave unmerged parts), and without
		// an OnResult ch-go fails the second block with "no OnResult
		// provided". Columns reset per block, so copy out before the next.
		OnResult: func(context.Context, proto.Block) error {
			n := agent.Rows()
			for i := 0; i < n; i++ {
				out = append(out, agentUsage{
					agent:        agent.Row(i),
					namespace:    namespace.Row(i),
					invocations:  inv.Row(i),
					errors:       errs.Row(i),
					inputTokens:  inTok.Row(i),
					outputTokens: outTok.Row(i),
					toolCalls:    tools.Row(i),
					latencyP50Ms: p50.Row(i),
					latencyP99Ms: p99.Row(i),
				})
			}
			return nil
		},
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// usageList projects the scanned aggregates into the wire UsageList. The
// `active` gauge is deliberately absent: it is a point-in-time CR-status
// read, not a window aggregate, and is populated by the CR-join. Latency
// keys are emitted only when includeLatency is set (TaskAgent), rounded
// to whole milliseconds.
func (u agentUsage) usageList(includeLatency bool) metricsv1.UsageList {
	q := func(n int64) resource.Quantity { return *resource.NewQuantity(n, resource.DecimalSI) }
	// CH aggregates are UInt64; resource.Quantity is int64-valued. Clamp
	// rather than let an (unrealistic but unguarded) sum above MaxInt64
	// wrap to a negative usage value on the wire.
	qu := func(n uint64) resource.Quantity {
		if n > math.MaxInt64 {
			n = math.MaxInt64
		}
		return q(int64(n))
	}
	ul := metricsv1.UsageList{
		metricsv1.UsageInvocations:  qu(u.invocations),
		metricsv1.UsageErrors:       qu(u.errors),
		metricsv1.UsageInputTokens:  qu(u.inputTokens),
		metricsv1.UsageOutputTokens: qu(u.outputTokens),
		metricsv1.UsageToolCalls:    qu(u.toolCalls),
	}
	if includeLatency {
		ul[metricsv1.UsageLatencyP50Ms] = q(int64(math.Round(u.latencyP50Ms)))
		ul[metricsv1.UsageLatencyP99Ms] = q(int64(math.Round(u.latencyP99Ms)))
	}
	return ul
}
