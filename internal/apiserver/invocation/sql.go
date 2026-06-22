package invocation

import "github.com/apoxy-dev/clrk/internal/apiserver/chsql"

// sqlString aliases the shared ClickHouse literal escaper so the
// invocation query builders read naturally while the escaping logic
// stays single-sourced with the telemetry and agentmetrics read models
// (see internal/apiserver/chsql).
var sqlString = chsql.String
