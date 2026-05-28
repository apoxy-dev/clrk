package invocation

import "strings"

// sqlString returns s wrapped as a ClickHouse single-quoted string
// literal. ch-go has no parameter binding for our access pattern
// (chpool.Pool.Do takes a SQL body); we build the SQL by hand and
// escape values here. CH literals escape with backslash for both
// single-quote and backslash itself.
func sqlString(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('\'')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
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
