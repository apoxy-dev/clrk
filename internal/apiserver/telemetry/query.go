package telemetry

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// List limit bounds, mirroring the invocation read model: "unset" is
// never "unbounded" (clients page via the continue token); an over-eager
// explicit limit is capped.
const (
	defaultListLimit = 100
	maxListLimit     = 1000
)

// scope is the server-enforced read scope derived from the request path:
// the agent name + namespace, and (per parent mount) the agent kind. A
// caller can only ever read the agent its path names.
type scope struct {
	namespace string
	agent     string
	agentKind string // "TaskAgent"/"DaemonAgent"/""
}

// filters are the per-request query-param filters, all optional.
type filters struct {
	invocation string
	components []string
	iostream   string // logs only
	since      time.Time
	until      time.Time
	limit      int
	follow     bool
	cont       string // continue token (paged GET only)
}

// parseFilters reads the typed filters off the raw query string. signal
// gates iostream, which only applies to logs.
func parseFilters(q url.Values, signal Signal) (filters, error) {
	var f filters
	f.invocation = q.Get("invocation")
	if c := q.Get("component"); c != "" {
		for _, p := range strings.Split(c, ",") {
			if p = strings.TrimSpace(p); p != "" {
				f.components = append(f.components, p)
			}
		}
	}
	if io := q.Get("iostream"); io != "" {
		if signal != SignalLogs {
			return f, fmt.Errorf("iostream filter is only valid for logs")
		}
		f.iostream = io
	}
	if s := q.Get("since"); s != "" {
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return f, fmt.Errorf("invalid since (want RFC3339): %w", err)
		}
		f.since = t
	}
	if s := q.Get("until"); s != "" {
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return f, fmt.Errorf("invalid until (want RFC3339): %w", err)
		}
		f.until = t
	}
	if s := q.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			return f, fmt.Errorf("invalid limit %q (want a non-negative integer)", s)
		}
		f.limit = n
	}
	if s := q.Get("follow"); s != "" {
		b, err := strconv.ParseBool(s)
		if err != nil {
			return f, fmt.Errorf("invalid follow %q (want a bool)", s)
		}
		f.follow = b
	}
	f.cont = q.Get("continue")
	return f, nil
}

// effectiveLimit clamps the requested limit into [1, maxListLimit],
// defaulting to defaultListLimit.
func (f filters) effectiveLimit() int {
	limit := f.limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	return limit
}

// pageCursor is the decoded continue token. Snap pins the snapshot upper
// bound so a multi-page list is consistent (every page reads Timestamp
// <= Snap); (TS, TID, SID) is the keyset position of the last row
// returned, used as a strict tuple bound for the next page. Timestamps
// are unix nanos.
type pageCursor struct {
	Snap int64  `json:"s"`
	TS   int64  `json:"t"`
	TID  string `json:"ti"`
	SID  string `json:"si"`
}

func encodeCursor(c pageCursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (pageCursor, error) {
	var c pageCursor
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return c, fmt.Errorf("decode continue token: %w", err)
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return c, fmt.Errorf("parse continue token: %w", err)
	}
	return c, nil
}

// pageBound resolves the snapshot upper bound and optional keyset cursor
// for a paged request. On the first page (no continue) the snapshot is
// pinned to now; a continuation re-uses the pinned Snap so later pages
// never see rows that landed mid-list.
func pageBound(f filters, now time.Time) (snapNano int64, cur *pageCursor, err error) {
	if f.cont == "" {
		return now.UnixNano(), nil, nil
	}
	c, derr := decodeCursor(f.cont)
	if derr != nil {
		return 0, nil, derr
	}
	return c.Snap, &c, nil
}

// scopeAndFilterClauses builds the WHERE predicates for a query against
// the given per-record attribute-map column (LogAttributes for logs,
// SpanAttributes for traces). The agent/namespace/kind scope is
// server-enforced; the rest are user filters. Every request- or
// user-derived value is sqlString-escaped (ch-go has no bind params for
// this access pattern).
func scopeAndFilterClauses(sc scope, f filters, attrCol string) []string {
	var cl []string
	cl = append(cl, "Agent = "+sqlString(sc.agent))
	if sc.namespace != "" {
		cl = append(cl, attrCol+"['agent.namespace'] = "+sqlString(sc.namespace))
	}
	if sc.agentKind != "" {
		cl = append(cl, attrCol+"['agent.kind'] = "+sqlString(sc.agentKind))
	}
	if f.invocation != "" {
		cl = append(cl, "InvocationId = "+sqlString(f.invocation))
	}
	if len(f.components) > 0 {
		quoted := make([]string, len(f.components))
		for i, c := range f.components {
			quoted[i] = sqlString(c)
		}
		cl = append(cl, "Component IN ("+strings.Join(quoted, ", ")+")")
	}
	if !f.since.IsZero() {
		cl = append(cl, "Timestamp >= "+dt64Nano(f.since.UnixNano()))
	}
	if !f.until.IsZero() {
		cl = append(cl, "Timestamp < "+dt64Nano(f.until.UnixNano()))
	}
	return cl
}

// keysetClause is the strict descending tuple bound that advances paging
// past the last row returned: (Timestamp, TraceId, SpanId) < (cursor).
func keysetClause(c *pageCursor) string {
	return fmt.Sprintf("(Timestamp, TraceId, SpanId) < (%s, %s, %s)",
		dt64Nano(c.TS), sqlString(c.TID), sqlString(c.SID))
}

// whereOf joins clauses into a WHERE fragment (empty when no clauses).
func whereOf(clauses []string) string {
	if len(clauses) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(clauses, " AND ")
}

// dt64Nano renders a unix-nano value as a DateTime64(9) literal CH can
// compare against the Timestamp column.
func dt64Nano(nano int64) string {
	return "fromUnixTimestamp64Nano(toInt64(" + strconv.FormatInt(nano, 10) + "))"
}

// sqlString wraps s as a ClickHouse single-quoted string literal,
// escaping backslash and single-quote with a backslash. Same helper as
// the invocation read model; ch-go has no parameter binding for the
// chpool.Pool.Do raw-SQL access pattern.
func sqlString(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('\'')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\\', '\'':
			b.WriteByte('\\')
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('\'')
	return b.String()
}

// hexDecode decodes a lowercase-hex id back to bytes, returning nil for
// empty or malformed input (the writer stored ids via hexOrEmpty, so a
// well-formed row is always valid hex or "").
func hexDecode(s string) []byte {
	if s == "" {
		return nil
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil
	}
	return b
}

// mapGroupKey is a stable, collision-resistant serialization of an
// attribute map, used to group flattened rows back into their shared
// Resource / Scope. Keys are sorted; key and value are length-prefixed
// so distinct maps cannot alias.
func mapGroupKey(m map[string]string) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%d:%s=%d:%s;", len(k), k, len(m[k]), m[k])
	}
	return b.String()
}
