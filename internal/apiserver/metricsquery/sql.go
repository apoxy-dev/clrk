package metricsquery

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/apoxy-dev/clrk/internal/apiserver/chsql"
)

// sqlString / dt64Nano alias the shared chsql helpers so the query builders
// read naturally while the literal-escaping and DateTime64 logic stays
// single-sourced with the invocation, telemetry, and agentmetrics read models
// (ch-go has no parameter binding for the chpool.Pool.Do raw-SQL access
// pattern, so every request-derived value is escaped here).
var (
	sqlString = chsql.String
	dt64Nano  = chsql.DateTime64Nano
)

// inList renders a set of already-trusted-shape string values as a
// ClickHouse IN (...) tuple, each value escaped as a string literal.
func inList(vals []string) string {
	quoted := make([]string, len(vals))
	for i, v := range vals {
		quoted[i] = sqlString(v)
	}
	return "(" + strings.Join(quoted, ", ") + ")"
}

// intervalSeconds renders a ClickHouse INTERVAL literal for a bucket width.
// Steps are whole seconds (the minimum useful step is a minute), so a second
// granularity is exact for every supported step.
func intervalSeconds(step time.Duration) string {
	return fmt.Sprintf("INTERVAL %d SECOND", int64(step/time.Second))
}

// quantityFromCounter parses an exact integer/decimal string (a counter/sum
// scanned as toString(...) for exactness) into a resource.Quantity. ParseQuantity
// preserves arbitrary precision, so a token sum above 2^53 stays exact -- the
// whole reason counters are scanned as strings rather than float64 columns.
func quantityFromCounter(s string) (resource.Quantity, error) {
	return resource.ParseQuantity(s)
}

// quantityFromHistogram converts a histogram quantile value (a duration in ms,
// possibly fractional) into a resource.Quantity, rounded to a whole unit -- the
// same convention the Tier-1 UsageList uses for its latency percentiles
// (agentmetrics rounds p50/p99 to whole milliseconds), so the two tiers report
// durations identically.
func quantityFromHistogram(v float64) resource.Quantity {
	return *resource.NewQuantity(int64(math.Round(v)), resource.DecimalSI)
}

// formatQuantile renders a quantile fraction as the label value (e.g. 0.95).
func formatQuantile(q float64) string {
	return strconv.FormatFloat(q, 'f', -1, 64)
}
