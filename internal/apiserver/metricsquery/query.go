package metricsquery

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	metricsv1 "github.com/apoxy-dev/clrk/api/metrics/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/chwriter"
)

// Query-shaping bounds. A range query is capped on both axes so a single
// request can never fan out unboundedly: maxBuckets bounds the time axis
// (window / step) and maxSeries bounds the cardinality axis (distinct groups,
// kept by total value), with the overflow reported via MetricSeriesSet.Truncated.
const (
	defaultWindow = time.Hour
	minStep       = time.Second
	maxBuckets    = 1500
	maxSeries     = 50
	// maxLookback bounds the scanned window on both the scalar and range query
	// (the scalar query has no step, so maxBuckets cannot bound it). 31 days
	// sits above any console range selector; deeper history is the
	// materialized-view path.
	maxLookback = 31 * 24 * time.Hour
)

// minTime is the earliest instant time.Time.UnixNano() represents without
// overflow (~1678). Below it the nanosecond literal baseWhere emits wraps sign,
// rendering an impossible WHERE range that ClickHouse evaluates to an empty
// result -- a silent zero-data 200 instead of an error. The upper edge needs no
// constant: a future until is clamped to now (always representable), and
// maxLookback then bounds the window width.
var minTime = time.Unix(0, math.MinInt64).UTC()

// defaultQuantiles is the histogram percentile set when no quantiles are
// requested -- the p50/p95/p99 the metrics page renders.
var defaultQuantiles = []float64{0.5, 0.95, 0.99}

// params is the parsed, validated query for a scalar or range series request.
type params struct {
	metric    metricDef
	groupBy   string
	step      time.Duration // 0 for a scalar query
	quantiles []float64     // histogram only
	since     time.Time
	until     time.Time
}

// isRange reports whether this is a time-bucketed (stepped) request.
func (p params) isRange() bool { return p.step > 0 }

// parseParams validates the query string against the metric named by metricID.
// A non-empty step selects a bucketed range query; an empty step selects a
// scalar query. now anchors the default window. The params arrive as raw
// url.Values (the apoxy apiserver builder's connect ParameterCodec only knows
// metav1 types, so a typed connect-options object cannot decode here; the
// telemetry read connecter parses its query the same way). The metric id is
// resolved by the connecter (path element on the fleet surface, ?metric= on the
// per-agent surface) and passed in.
func parseParams(q url.Values, metricID string, now time.Time) (params, error) {
	var p params

	id := strings.TrimSpace(metricID)
	if id == "" {
		return p, fmt.Errorf("metric is required")
	}
	def, ok := lookupMetric(id)
	if !ok {
		return p, fmt.Errorf("unknown metric %q (see the metrics catalog for valid ids)", id)
	}
	p.metric = def

	if gb := strings.TrimSpace(q.Get("groupBy")); gb != "" {
		if !def.allowsDim(gb) {
			return p, fmt.Errorf("metric %q cannot be grouped by %q (allowed: %s)", id, gb, strings.Join(def.dims, ", "))
		}
		p.groupBy = gb
	}

	// Quantiles arrive as a repeated query param (?quantiles=0.5&quantiles=0.95)
	// or a single comma list; joining then splitting accepts both forms.
	if raw := strings.TrimSpace(strings.Join(q["quantiles"], ",")); raw != "" {
		if def.typ != typeHistogram {
			return p, fmt.Errorf("quantiles apply only to histogram metrics; %q is a %s", id, def.typ)
		}
		qs, err := parseQuantiles(raw)
		if err != nil {
			return p, err
		}
		p.quantiles = qs
	} else if def.typ == typeHistogram {
		p.quantiles = defaultQuantiles
	}

	// Window: half-open [since, until). Defaults to the trailing defaultWindow
	// ending now, so a bare query still returns something sensible.
	until := now
	if s := strings.TrimSpace(q.Get("until")); s != "" {
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return p, fmt.Errorf("invalid until (want RFC3339): %w", err)
		}
		until = t
	}
	// Clamp a future until to now: there is no data past now, an unclamped
	// future until would default since into the future and return an empty
	// window, and it keeps until within the UnixNano-representable range. The
	// result echoes the effective until, so the clamp is visible to the client.
	if until.After(now) {
		until = now
	}
	since := until.Add(-defaultWindow)
	if s := strings.TrimSpace(q.Get("since")); s != "" {
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return p, fmt.Errorf("invalid since (want RFC3339): %w", err)
		}
		since = t
	}
	if !until.After(since) {
		return p, fmt.Errorf("until must be after since")
	}
	// Floor the lower bound: a since below the UnixNano-representable range
	// (~1678) emits a sign-wrapped nanosecond literal and silently returns zero
	// data. until is already <= now from the clamp and since < until, so this one
	// lower-bound check keeps the whole window representable.
	if since.Before(minTime) {
		return p, fmt.Errorf("since is before the earliest supported time (%s)", minTime.Format(time.RFC3339))
	}
	// Bound the scan range. maxBuckets only caps a range query's response
	// fan-out, not the rows scanned, and a scalar query has no step at all, so
	// without this a since far in the past drives a full-retention aggregate
	// over every partition. The cap is well above the console's range selector.
	if window := until.Sub(since); window > maxLookback {
		return p, fmt.Errorf("window %s exceeds the %s maximum lookback; narrow since/until", window, maxLookback)
	}
	p.since, p.until = since, until

	if s := strings.TrimSpace(q.Get("step")); s != "" {
		step, err := time.ParseDuration(s)
		if err != nil {
			return p, fmt.Errorf("invalid step (want a Go duration, e.g. 5m): %w", err)
		}
		if step < minStep {
			return p, fmt.Errorf("step must be at least %s", minStep)
		}
		if step%time.Second != 0 {
			return p, fmt.Errorf("step must be a whole number of seconds")
		}
		// Round up: a window that is not an exact multiple of step still emits
		// one trailing partial bucket, so divide-and-truncate would let
		// maxBuckets+1 buckets slip past the cap.
		if buckets := (until.Sub(since) + step - 1) / step; buckets > maxBuckets {
			return p, fmt.Errorf("window / step yields %d buckets, exceeds the %d cap; widen step or narrow the window", buckets, maxBuckets)
		}
		p.step = step
	}
	return p, nil
}

// parseQuantiles parses a comma list of fractions in (0, 1). Duplicates are
// dropped (keeping first-seen order): each quantile becomes one output column
// labeled {quantile: <q>}, so a repeat would yield two series with identical
// labels -- a malformed labeled-series set.
func parseQuantiles(raw string) ([]float64, error) {
	parts := strings.Split(raw, ",")
	out := make([]float64, 0, len(parts))
	seen := make(map[float64]bool, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		q, err := strconv.ParseFloat(p, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid quantile %q: %w", p, err)
		}
		if q <= 0 || q >= 1 {
			return nil, fmt.Errorf("quantile %v out of range (want 0 < q < 1)", q)
		}
		if seen[q] {
			continue
		}
		seen[q] = true
		out = append(out, q)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("quantiles is empty")
	}
	return out, nil
}

// outputCol is one value column the query selects: its ClickHouse aggregate
// expression plus the label (key/value) that distinguishes its series when a
// metric reports several values (the measures of a multi-value counter, or the
// quantiles of a histogram). labelKey is "" for a single-valued counter.
type outputCol struct {
	expr     string
	labelKey string
	labelVal string
}

// outputCols resolves a metric + requested quantiles into the value columns to
// select. A histogram becomes one quantile() column per requested quantile; a
// counter/gauge becomes its measures, labeled only when there is more than one.
func outputCols(p params) []outputCol {
	if p.metric.typ == typeHistogram {
		cols := make([]outputCol, 0, len(p.quantiles))
		for _, q := range p.quantiles {
			qs := formatQuantile(q)
			cols = append(cols, outputCol{
				// quantile() over an empty bucket is NaN; collapse it to 0 so
				// the wire carries a number, matching the Tier-1 convention.
				expr:     fmt.Sprintf("ifNotFinite(quantile(%s)(%s), 0)", qs, p.metric.histoExpr),
				labelKey: "quantile",
				labelVal: qs,
			})
		}
		return cols
	}
	multi := len(p.metric.measures) > 1
	cols := make([]outputCol, 0, len(p.metric.measures))
	for _, ms := range p.metric.measures {
		oc := outputCol{expr: ms.expr}
		if multi {
			oc.labelKey, oc.labelVal = "measure", ms.name
		}
		cols = append(cols, oc)
	}
	return cols
}

// baseWhere builds the scope + window + metric-filter predicates shared by the
// ranking and main queries.
func baseWhere(p params, sc scope) string {
	cl := sc.clauses(p.metric.source.attrCol)
	cl = append(cl,
		"Timestamp >= "+dt64Nano(p.since.UnixNano()),
		"Timestamp < "+dt64Nano(p.until.UnixNano()),
	)
	if p.metric.filter != "" {
		cl = append(cl, p.metric.filter)
	}
	return strings.Join(cl, " AND ")
}

// topGroups resolves the group values to chart, capped at maxSeries by total
// value over the window. The bool reports whether more groups existed than the
// cap (so the caller can flag the breakdown as truncated). Only called when a
// groupBy is set.
func topGroups(ctx context.Context, pool Doer, p params, sc scope) ([]string, bool, error) {
	// CAST(... AS String) normalizes the group key to a plain String: a
	// materialized dimension (Agent / SeverityText / Component) is
	// LowCardinality(String), which toString() preserves and ch-go's strict
	// scan rejects against a plain ColStr; the CAST strips it (and is a no-op
	// for the map-value String dimensions).
	body := fmt.Sprintf(
		"SELECT CAST(%s AS String) AS g FROM %s.%s WHERE %s GROUP BY g ORDER BY %s DESC LIMIT %d",
		dimExpr[p.groupBy], chwriter.Database, p.metric.source.name, baseWhere(p, sc),
		p.metric.rankExpr(), maxSeries+1,
	)
	var g proto.ColStr
	var groups []string
	if err := pool.Do(ctx, ch.Query{
		Body:   body,
		Result: proto.Results{{Name: "g", Data: &g}},
		OnResult: func(context.Context, proto.Block) error {
			for i := 0; i < g.Rows(); i++ {
				groups = append(groups, g.Row(i))
			}
			return nil
		},
	}); err != nil {
		return nil, false, err
	}
	truncated := len(groups) > maxSeries
	if truncated {
		groups = groups[:maxSeries]
	}
	return groups, truncated, nil
}

// scannedRow is one decoded row of the main query: an optional bucket (range
// only) and group value, plus the per-output-column value already converted to
// a resource.Quantity (exact for counter/sum totals, rounded-to-whole for
// histogram quantiles).
type scannedRow struct {
	bucketSec uint32
	groupVal  string
	values    []resource.Quantity
}

// runMainQuery builds and runs the aggregate, returning the decoded rows. When
// groups is non-nil the GROUP BY dimension is constrained to that set (the
// top-N cap). The SELECT column order is [t,] [g,] m0..mN, matching the scan.
func runMainQuery(ctx context.Context, pool Doer, p params, sc scope, outs []outputCol, groups []string) ([]scannedRow, error) {
	var sel, groupBy []string
	if p.isRange() {
		// Pin bucket alignment to UTC: the Timestamp column is timezone-naive,
		// so toStartOfInterval would otherwise align hour+ buckets to the CH
		// server zone while the Go side labels them as UTC instants.
		sel = append(sel, fmt.Sprintf("toUnixTimestamp(toStartOfInterval(Timestamp, %s, 'UTC')) AS t", intervalSeconds(p.step)))
		groupBy = append(groupBy, "t")
	}
	if p.groupBy != "" {
		// CAST(... AS String): see topGroups -- strips LowCardinality so the
		// group key scans into a plain ColStr.
		sel = append(sel, fmt.Sprintf("CAST(%s AS String) AS g", dimExpr[p.groupBy]))
		groupBy = append(groupBy, "g")
	}
	// Counter/sum measures select as toString(...) and scan as a plain String,
	// so an integer total above 2^53 stays EXACT end to end -- a float64 column
	// would round it, breaking the exact-value promise. Histogram quantiles are
	// genuinely fractional, so they stay Float64 and are rounded Go-side.
	isHisto := p.metric.typ == typeHistogram
	for i, oc := range outs {
		if isHisto {
			sel = append(sel, fmt.Sprintf("%s AS m%d", oc.expr, i))
		} else {
			sel = append(sel, fmt.Sprintf("toString(%s) AS m%d", oc.expr, i))
		}
	}

	where := baseWhere(p, sc)
	if p.groupBy != "" {
		where += fmt.Sprintf(" AND CAST(%s AS String) IN %s", dimExpr[p.groupBy], inList(groups))
	}

	body := fmt.Sprintf("SELECT %s FROM %s.%s WHERE %s",
		strings.Join(sel, ", "), chwriter.Database, p.metric.source.name, where)
	if len(groupBy) > 0 {
		body += " GROUP BY " + strings.Join(groupBy, ", ")
	}
	if p.isRange() {
		body += " ORDER BY t"
	}

	bucket := new(proto.ColUInt32)
	group := new(proto.ColStr)
	// Per-measure column + reader: a histogram quantile decodes as Float64 and
	// is rounded Go-side; a counter/sum measure decodes as the exact decimal
	// String the SQL toString(...) produced.
	type measureCol struct {
		data proto.ColResult
		read func(row int) (resource.Quantity, error)
	}
	mcols := make([]measureCol, len(outs))
	results := make(proto.Results, 0, len(outs)+2)
	if p.isRange() {
		results = append(results, proto.ResultColumn{Name: "t", Data: bucket})
	}
	if p.groupBy != "" {
		results = append(results, proto.ResultColumn{Name: "g", Data: group})
	}
	for i := range outs {
		if isHisto {
			col := new(proto.ColFloat64)
			mcols[i] = measureCol{data: col, read: func(row int) (resource.Quantity, error) {
				return quantityFromHistogram(col.Row(row)), nil
			}}
		} else {
			col := new(proto.ColStr)
			mcols[i] = measureCol{data: col, read: func(row int) (resource.Quantity, error) {
				return quantityFromCounter(col.Row(row))
			}}
		}
		results = append(results, proto.ResultColumn{Name: fmt.Sprintf("m%d", i), Data: mcols[i].data})
	}

	var rows []scannedRow
	var scanErr error
	if err := pool.Do(ctx, ch.Query{
		Body:   body,
		Result: results,
		// Drain every block: a SELECT over many unmerged parts (chwriter's
		// steady state) splits its result across blocks, and ch-go fails the
		// second block without an OnResult. Columns reset per block, so copy
		// each row out before the next.
		OnResult: func(context.Context, proto.Block) error {
			n := mcols[0].data.Rows()
			for i := 0; i < n; i++ {
				r := scannedRow{values: make([]resource.Quantity, len(mcols))}
				if p.isRange() {
					r.bucketSec = bucket.Row(i)
				}
				if p.groupBy != "" {
					r.groupVal = group.Row(i)
				}
				for j := range mcols {
					q, err := mcols[j].read(i)
					if err != nil {
						scanErr = fmt.Errorf("decode value: %w", err)
						return scanErr
					}
					r.values[j] = q
				}
				rows = append(rows, r)
			}
			return nil
		},
	}); err != nil {
		if scanErr != nil {
			return nil, scanErr
		}
		return nil, err
	}
	return rows, nil
}

// queryMetric runs the full scalar/range query and assembles the labeled-series
// result object.
func queryMetric(ctx context.Context, pool Doer, p params, sc scope) (*metricsv1.MetricSeriesSet, error) {
	res := &metricsv1.MetricSeriesSet{
		ObjectMeta: metav1.ObjectMeta{Name: p.metric.id},
		Metric:     p.metric.id,
		Type:       apiMetricType(p.metric.typ),
		Unit:       p.metric.unit,
		Since:      metav1.NewTime(p.since.UTC()),
		Until:      metav1.NewTime(p.until.UTC()),
		GroupBy:    p.groupBy,
		Series:     []metricsv1.MetricSeries{},
	}
	if p.isRange() {
		d := metav1.Duration{Duration: p.step}
		res.Step = &d
	}

	var groups []string
	if p.groupBy != "" {
		var truncated bool
		var err error
		groups, truncated, err = topGroups(ctx, pool, p, sc)
		if err != nil {
			return nil, err
		}
		res.Truncated = truncated
		if len(groups) == 0 {
			// No group has any data in the window: an empty breakdown.
			return res, nil
		}
	}

	outs := outputCols(p)
	rows, err := runMainQuery(ctx, pool, p, sc, outs, groups)
	if err != nil {
		return nil, err
	}
	res.Series = assembleSeries(p, outs, rows)
	return res, nil
}

// assembleSeries pivots the flat scanned rows into one MetricSeries per
// (group value x output column), each output column's value landing on its own
// labeled series at the row's bucket (range) or as a single point at the window
// end (scalar).
func assembleSeries(p params, outs []outputCol, rows []scannedRow) []metricsv1.MetricSeries {
	type entry struct {
		series *metricsv1.MetricSeries
		order  int
		key    string // labelKey(Labels), computed once for the sort
	}
	byKey := map[string]*entry{}
	var order int
	scalarT := metav1.NewTime(p.until.UTC())

	// makeEntry returns (creating on first use) the series for a (group value,
	// output column) pair, attaching the group + measure/quantile labels.
	makeEntry := func(groupVal string, i int, oc outputCol) *entry {
		key := groupVal + "\x00" + strconv.Itoa(i)
		if e, ok := byKey[key]; ok {
			return e
		}
		labels := map[string]string{}
		if p.groupBy != "" {
			labels[p.groupBy] = groupVal
		}
		if oc.labelKey != "" {
			labels[oc.labelKey] = oc.labelVal
		}
		e := &entry{series: &metricsv1.MetricSeries{Labels: labels, Points: []metricsv1.MetricPoint{}}, order: order}
		order++
		byKey[key] = e
		return e
	}

	// An ungrouped query must return exactly one series per output column even
	// with zero matching rows: a scalar count() always yields a 0 row, but a
	// range query's GROUP BY t yields no rows over an empty window. Pre-seed
	// those series so the no-groupBy contract holds on both paths.
	if p.groupBy == "" {
		for i, oc := range outs {
			makeEntry("", i, oc)
		}
	}

	for _, r := range rows {
		for i, oc := range outs {
			e := makeEntry(r.groupVal, i, oc)
			pt := metricsv1.MetricPoint{Value: r.values[i], Timestamp: scalarT}
			if p.isRange() {
				pt.Timestamp = metav1.NewTime(time.Unix(int64(r.bucketSec), 0).UTC())
			}
			e.series.Points = append(e.series.Points, pt)
		}
	}

	entries := make([]*entry, 0, len(byKey))
	for _, e := range byKey {
		sort.Slice(e.series.Points, func(a, b int) bool {
			return e.series.Points[a].Timestamp.Before(&e.series.Points[b].Timestamp)
		})
		e.key = labelKey(e.series.Labels)
		entries = append(entries, e)
	}
	// Stable, deterministic series order: by label set, falling back to scan
	// order for ties (a scalar no-groupBy single-measure query has one series).
	// The label key is computed once per entry above, not re-derived inside the
	// O(N log N) comparator.
	sort.Slice(entries, func(a, b int) bool {
		if entries[a].key != entries[b].key {
			return entries[a].key < entries[b].key
		}
		return entries[a].order < entries[b].order
	})
	out := make([]metricsv1.MetricSeries, 0, len(entries))
	for _, e := range entries {
		out = append(out, *e.series)
	}
	return out
}

// labelKey is a stable serialization of a label set for ordering.
func labelKey(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(m[k])
		b.WriteByte(';')
	}
	return b.String()
}
