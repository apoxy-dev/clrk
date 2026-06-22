// Package chsql holds the small ClickHouse SQL-building primitives shared
// by the apiserver read models (invocation, telemetry, agentmetrics).
// ch-go has no parameter binding for the chpool.Pool.Do raw-SQL access
// pattern, so each read model builds its SELECT by hand and escapes
// request-derived values here. Keeping the escaper and the DateTime64
// literal in one place keeps the SQL-injection boundary single-sourced —
// a hardening (e.g. handling more control bytes) lands once.
package chsql

import (
	"context"
	"strconv"
	"strings"

	"github.com/ClickHouse/ch-go"
)

// Doer is the subset of *chpool.Pool the read models need. Pulled out as
// an interface so the manager can pass the shared LazyPool (which
// satisfies it structurally) and so unit tests can inject a fake that
// records the issued ch.Query and populates Result columns from a canned
// dataset.
type Doer interface {
	Do(ctx context.Context, q ch.Query) error
}

// String wraps s as a ClickHouse single-quoted string literal, escaping
// backslash and single-quote with a backslash. Callers must still keep
// request-derived values to apiserver-validated shapes (DNS-1123 names,
// enum constants): this escaping covers quote/backslash, not arbitrary
// control bytes.
func String(s string) string {
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

// DateTime64Nano renders a unix-nano value as a DateTime64(9) literal CH
// can compare against a DateTime64 Timestamp column.
func DateTime64Nano(nano int64) string {
	return "fromUnixTimestamp64Nano(toInt64(" + strconv.FormatInt(nano, 10) + "))"
}
