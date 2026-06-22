package agentmetrics

import "github.com/apoxy-dev/clrk/internal/apiserver/chsql"

// sqlString / dt64Nano alias the shared chsql helpers so the query
// builders below read naturally while the literal-escaping and
// DateTime64 logic stays single-sourced with the invocation and
// telemetry read models (see internal/apiserver/chsql).
var (
	sqlString = chsql.String
	dt64Nano  = chsql.DateTime64Nano
)
