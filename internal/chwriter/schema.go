// Package chwriter persists OTLP logs and traces received by the
// controller-manager into the embedded ClickHouse engine
// (internal/clickhouse) using the ch-go columnar driver.
//
// Two MergeTree tables — otel_logs and otel_traces — are created on
// first Run(). The Writer pulls decoded batches off the channels its
// Enqueue methods feed, projects them into columnar blocks, and emits
// an INSERT every 1s OR at 10k rows OR ~8MiB whichever first. Enqueue
// is non-blocking: when the buffer is full, records are dropped and a
// counter is bumped so an unhealthy CH never back-pressures the OTLP
// receiver.
package chwriter

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// Database is the schema name. Embedded engine starts with "default"
// pre-created; we use it as-is so DDL is idempotent across cm restarts.
const Database = "default"

// LogsTable and TracesTable are the table names matching the otel-
// collector-contrib ClickHouse exporter conventions, so off-the-shelf
// dashboards work out of the box.
const (
	LogsTable   = "otel_logs"
	TracesTable = "otel_traces"
)

// Sentinel columns identify the current schema generation. ensureSchema
// drops+recreates a table only when it exists but is missing its
// sentinel — i.e. it was created by an older binary. Each sentinel must
// be a column that exists ONLY in the current schema; bump it whenever a
// future schema change again requires a recreate.
const (
	tracesSentinelColumn = "TraceState"
	logsSentinelColumn   = "TraceFlags"
)

// createLogsTableTmpl is the otel_logs DDL, modeled on the
// opentelemetry-collector-contrib ClickHouse exporter so off-the-shelf
// dashboards read it unchanged. %d is the TTL in days. ORDER BY leads
// with EGRef so per-tenant range scans hit contiguous parts; the
// MATERIALIZED columns + bloom skip indexes serve the read-API point
// lookups (by invocation.id / agent.name) without a full scan.
const createLogsTableTmpl = `
CREATE TABLE IF NOT EXISTS ` + Database + `.` + LogsTable + ` (
  Timestamp           DateTime64(9)                              CODEC(Delta, ZSTD(1)),
  EGRef               LowCardinality(String)                     CODEC(ZSTD(1)),
  TraceId             String                                     CODEC(ZSTD(1)),
  SpanId              String                                     CODEC(ZSTD(1)),
  TraceFlags          UInt8,
  SeverityText        LowCardinality(String)                     CODEC(ZSTD(1)),
  SeverityNumber      UInt8,
  ServiceName         LowCardinality(String)                     CODEC(ZSTD(1)),
  Body                String                                     CODEC(ZSTD(1)),
  ResourceSchemaUrl   LowCardinality(String)                     CODEC(ZSTD(1)),
  ResourceAttributes  Map(LowCardinality(String), String)        CODEC(ZSTD(1)),
  ScopeSchemaUrl      LowCardinality(String)                     CODEC(ZSTD(1)),
  ScopeName           LowCardinality(String)                     CODEC(ZSTD(1)),
  ScopeVersion        LowCardinality(String)                     CODEC(ZSTD(1)),
  ScopeAttributes     Map(LowCardinality(String), String)        CODEC(ZSTD(1)),
  LogAttributes       Map(LowCardinality(String), String)        CODEC(ZSTD(1)),
  Component    LowCardinality(String) MATERIALIZED ResourceAttributes['clrk.component'] CODEC(ZSTD(1)),
  InvocationId String                 MATERIALIZED LogAttributes['invocation.id']       CODEC(ZSTD(1)),
  Agent        LowCardinality(String) MATERIALIZED LogAttributes['agent.name']          CODEC(ZSTD(1)),
  IoStream     LowCardinality(String) MATERIALIZED LogAttributes['log.iostream']        CODEC(ZSTD(1)),
  INDEX idx_trace_id         TraceId                       TYPE bloom_filter(0.001) GRANULARITY 1,
  INDEX idx_invocation       InvocationId                  TYPE bloom_filter(0.001) GRANULARITY 1,
  INDEX idx_res_attr_key     mapKeys(ResourceAttributes)   TYPE bloom_filter(0.01)  GRANULARITY 1,
  INDEX idx_res_attr_value   mapValues(ResourceAttributes) TYPE bloom_filter(0.01)  GRANULARITY 1,
  INDEX idx_scope_attr_key   mapKeys(ScopeAttributes)      TYPE bloom_filter(0.01)  GRANULARITY 1,
  INDEX idx_scope_attr_value mapValues(ScopeAttributes)    TYPE bloom_filter(0.01)  GRANULARITY 1,
  INDEX idx_log_attr_key     mapKeys(LogAttributes)        TYPE bloom_filter(0.01)  GRANULARITY 1,
  INDEX idx_log_attr_value   mapValues(LogAttributes)      TYPE bloom_filter(0.01)  GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (EGRef, ServiceName, toUnixTimestamp(Timestamp))
TTL toDateTime(Timestamp) + INTERVAL %d DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
`

// createTracesTableTmpl is the otel_traces DDL. Span Events and Links
// are persisted as ClickHouse Nested columns (parallel arrays), so a
// span row carries its own context — including the egress sink's
// request/response header+body events — without a separate join.
const createTracesTableTmpl = `
CREATE TABLE IF NOT EXISTS ` + Database + `.` + TracesTable + ` (
  Timestamp           DateTime64(9)                              CODEC(Delta, ZSTD(1)),
  EGRef               LowCardinality(String)                     CODEC(ZSTD(1)),
  TraceId             String                                     CODEC(ZSTD(1)),
  SpanId              String                                     CODEC(ZSTD(1)),
  ParentSpanId        String                                     CODEC(ZSTD(1)),
  TraceState          String                                     CODEC(ZSTD(1)),
  SpanName            LowCardinality(String)                     CODEC(ZSTD(1)),
  SpanKind            LowCardinality(String)                     CODEC(ZSTD(1)),
  ServiceName         LowCardinality(String)                     CODEC(ZSTD(1)),
  ScopeName           LowCardinality(String)                     CODEC(ZSTD(1)),
  ScopeVersion        LowCardinality(String)                     CODEC(ZSTD(1)),
  Duration            UInt64                                     CODEC(ZSTD(1)),
  StatusCode          LowCardinality(String)                     CODEC(ZSTD(1)),
  StatusMessage       String                                     CODEC(ZSTD(1)),
  ResourceAttributes  Map(LowCardinality(String), String)        CODEC(ZSTD(1)),
  SpanAttributes      Map(LowCardinality(String), String)        CODEC(ZSTD(1)),
  Events Nested (
    Timestamp  DateTime64(9),
    Name       LowCardinality(String),
    Attributes Map(LowCardinality(String), String)
  ) CODEC(ZSTD(1)),
  Links Nested (
    TraceId    String,
    SpanId     String,
    TraceState String,
    Attributes Map(LowCardinality(String), String)
  ) CODEC(ZSTD(1)),
  Component    LowCardinality(String) MATERIALIZED ResourceAttributes['clrk.component'] CODEC(ZSTD(1)),
  InvocationId String                 MATERIALIZED SpanAttributes['invocation.id']      CODEC(ZSTD(1)),
  Agent        LowCardinality(String) MATERIALIZED SpanAttributes['agent.name']         CODEC(ZSTD(1)),
  INDEX idx_trace_id        TraceId                       TYPE bloom_filter(0.001) GRANULARITY 1,
  INDEX idx_invocation      InvocationId                  TYPE bloom_filter(0.001) GRANULARITY 1,
  INDEX idx_res_attr_key    mapKeys(ResourceAttributes)   TYPE bloom_filter(0.01)  GRANULARITY 1,
  INDEX idx_res_attr_value  mapValues(ResourceAttributes) TYPE bloom_filter(0.01)  GRANULARITY 1,
  INDEX idx_span_attr_key   mapKeys(SpanAttributes)       TYPE bloom_filter(0.01)  GRANULARITY 1,
  INDEX idx_span_attr_value mapValues(SpanAttributes)     TYPE bloom_filter(0.01)  GRANULARITY 1,
  INDEX idx_duration        Duration                      TYPE minmax              GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (EGRef, ServiceName, toUnixTimestamp(Timestamp), TraceId)
TTL toDateTime(Timestamp) + INTERVAL %d DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
`

// renderCreateLogsTable substitutes the TTL day count into the DDL.
func renderCreateLogsTable(ttlDays int) string {
	return fmt.Sprintf(createLogsTableTmpl, ttlDays)
}

// renderCreateTracesTable substitutes the TTL day count into the DDL.
func renderCreateTracesTable(ttlDays int) string {
	return fmt.Sprintf(createTracesTableTmpl, ttlDays)
}

// ensureSchema creates both otel tables, performing a one-time
// drop+recreate when an older-schema table is detected. OTLP is 7-day
// disposable and has no in-cluster SELECT consumers (otelforward and
// the dev TUI both read in-memory OTLP, never these tables), so
// dropping a stale table on the first boot of a new binary is
// acceptable. The sentinel-column check makes this idempotent: once the
// current schema is in place the sentinel is present and no further drop
// happens on subsequent restarts.
func ensureSchema(ctx context.Context, client *ch.Client, ttlDays int) error {
	if err := ensureTable(ctx, client, TracesTable, tracesSentinelColumn, renderCreateTracesTable(ttlDays)); err != nil {
		return fmt.Errorf("ensure %s: %w", TracesTable, err)
	}
	if err := ensureTable(ctx, client, LogsTable, logsSentinelColumn, renderCreateLogsTable(ttlDays)); err != nil {
		return fmt.Errorf("ensure %s: %w", LogsTable, err)
	}
	return nil
}

// ensureTable creates table from createDDL, first dropping it only when
// it already exists but lacks sentinelCol (a column unique to the
// current schema) — i.e. it was created by an older binary. A fresh
// install (table absent) takes the plain CREATE path and never issues a
// spurious DROP.
func ensureTable(ctx context.Context, client *ch.Client, table, sentinelCol, createDDL string) error {
	exists, err := tableExists(ctx, client, table)
	if err != nil {
		return err
	}
	if exists {
		hasSentinel, err := columnExists(ctx, client, table, sentinelCol)
		if err != nil {
			return err
		}
		if !hasSentinel {
			if err := client.Do(ctx, ch.Query{Body: fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", Database, table)}); err != nil {
				return fmt.Errorf("drop stale-schema %s: %w", table, err)
			}
			slog.Warn("Dropped stale-schema table for one-time migration", "table", table, "missing_column", sentinelCol)
		}
	}
	if err := client.Do(ctx, ch.Query{Body: createDDL}); err != nil {
		return fmt.Errorf("create %s: %w", table, err)
	}
	return nil
}

// tableExists reports whether Database.table is present.
func tableExists(ctx context.Context, client *ch.Client, table string) (bool, error) {
	return countQuery(ctx, client,
		fmt.Sprintf("SELECT count() AS c FROM system.tables WHERE database = '%s' AND name = '%s'", Database, table),
		fmt.Sprintf("introspect system.tables for %s", table))
}

// columnExists reports whether Database.table has a column named column.
func columnExists(ctx context.Context, client *ch.Client, table, column string) (bool, error) {
	return countQuery(ctx, client,
		fmt.Sprintf("SELECT count() AS c FROM system.columns WHERE database = '%s' AND table = '%s' AND name = '%s'", Database, table, column),
		fmt.Sprintf("introspect system.columns for %s.%s", table, column))
}

// countQuery runs a single-row `SELECT count() AS c` and reports c > 0.
// table/column/db are package constants, so the inlined literals are not
// attacker-controlled.
func countQuery(ctx context.Context, client *ch.Client, body, what string) (bool, error) {
	var c proto.ColUInt64
	if err := client.Do(ctx, ch.Query{
		Body:   body,
		Result: proto.Results{{Name: "c", Data: &c}},
	}); err != nil {
		return false, fmt.Errorf("%s: %w", what, err)
	}
	return c.Rows() > 0 && c.Row(0) > 0, nil
}
