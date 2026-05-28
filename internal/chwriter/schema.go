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

import "fmt"

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

// createLogsTableTmpl is the otel_logs DDL. %d is substituted with the
// TTL in days. ORDER BY leads with EGRef so per-tenant range scans hit
// contiguous parts, and the partition key keeps TTL drops aligned.
const createLogsTableTmpl = `
CREATE TABLE IF NOT EXISTS ` + Database + `.` + LogsTable + ` (
  Timestamp           DateTime64(9)                              CODEC(Delta, ZSTD(1)),
  EGRef               LowCardinality(String)                     CODEC(ZSTD(1)),
  TraceId             String                                     CODEC(ZSTD(1)),
  SpanId              String                                     CODEC(ZSTD(1)),
  SeverityText        LowCardinality(String)                     CODEC(ZSTD(1)),
  SeverityNumber      UInt8,
  ServiceName         LowCardinality(String)                     CODEC(ZSTD(1)),
  Body                String                                     CODEC(ZSTD(1)),
  ScopeName           LowCardinality(String)                     CODEC(ZSTD(1)),
  ResourceAttributes  Map(LowCardinality(String), String)        CODEC(ZSTD(1)),
  LogAttributes       Map(LowCardinality(String), String)        CODEC(ZSTD(1))
) ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (EGRef, ServiceName, toUnixTimestamp(Timestamp))
TTL toDateTime(Timestamp) + INTERVAL %d DAY
SETTINGS index_granularity = 8192
`

// createTracesTableTmpl is the otel_traces DDL. Events / Links are
// stored as parallel arrays so a span row carries its own context
// without a separate join.
const createTracesTableTmpl = `
CREATE TABLE IF NOT EXISTS ` + Database + `.` + TracesTable + ` (
  Timestamp           DateTime64(9)                              CODEC(Delta, ZSTD(1)),
  EGRef               LowCardinality(String)                     CODEC(ZSTD(1)),
  TraceId             String                                     CODEC(ZSTD(1)),
  SpanId              String                                     CODEC(ZSTD(1)),
  ParentSpanId        String                                     CODEC(ZSTD(1)),
  SpanName            LowCardinality(String)                     CODEC(ZSTD(1)),
  SpanKind            LowCardinality(String)                     CODEC(ZSTD(1)),
  ServiceName         LowCardinality(String)                     CODEC(ZSTD(1)),
  Duration            UInt64,
  StatusCode          LowCardinality(String)                     CODEC(ZSTD(1)),
  StatusMessage       String                                     CODEC(ZSTD(1)),
  ScopeName           LowCardinality(String)                     CODEC(ZSTD(1)),
  ResourceAttributes  Map(LowCardinality(String), String)        CODEC(ZSTD(1)),
  SpanAttributes      Map(LowCardinality(String), String)        CODEC(ZSTD(1))
) ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (EGRef, ServiceName, toUnixTimestamp(Timestamp), TraceId)
TTL toDateTime(Timestamp) + INTERVAL %d DAY
SETTINGS index_granularity = 8192
`

// renderCreateLogsTable substitutes the TTL day count into the DDL.
func renderCreateLogsTable(ttlDays int) string {
	return fmt.Sprintf(createLogsTableTmpl, ttlDays)
}

// renderCreateTracesTable substitutes the TTL day count into the DDL.
func renderCreateTracesTable(ttlDays int) string {
	return fmt.Sprintf(createTracesTableTmpl, ttlDays)
}
