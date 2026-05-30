package invocation

// Database is the ClickHouse schema name shared with internal/chwriter
// (otel_logs / otel_traces). The embedded engine starts "default"
// pre-created so DDL is idempotent across controller-manager restarts.
const Database = "default"

// Table is the append-only Invocation event-stream table. One row per
// observed lifecycle transition (Dispatched, Running, Succeeded, ...),
// materialized from the JetStream INVOCATIONS stream by the in-process
// consumer. The current state of an Invocation is reconstructed at read
// time as argMax(object, stream_seq) GROUP BY invocation_id — the row
// with the highest stream sequence wins, which matches lifecycle order
// because publishers emit in order and the JetStream sequence is
// monotonic. stream_seq doubles as the apiserver resourceVersion, so
// List can bound a read to `stream_seq <= rv` and a Watch can resume
// from `rv+1` against the same global order.
const Table = "invocation_events"

// createTableTmpl renders the DDL. %d is the TTL in days.
//
// Each row stores the complete Invocation JSON snapshot at the moment
// of the event in `object`; all indexed columns are MATERIALIZED
// projections derived from that JSON at write time, so Spec/Status
// field additions to the Go type need zero schema migration. The table
// is a plain MergeTree (not Replacing): every event is retained so the
// read-side aggregation can reconstruct any historical state and a
// Watch can replay the exact transition sequence.
//
// ORDER BY clusters every event for one invocation contiguously and in
// ascending stream_seq, so the per-id argMax the read path runs is a
// single ordered range scan. PARTITION BY day keeps an invocation's
// (short-lived) event set inside one partition, which also makes the
// (date, last_seen_id) continue token a cheap partition-local cursor.
const createTableTmpl = `
CREATE TABLE IF NOT EXISTS ` + Database + `.` + Table + ` (
  object        String                 CODEC(ZSTD(1)),
  stream_seq    UInt64                 CODEC(Delta, ZSTD(1)),
  event_time    DateTime64(3, 'UTC')   CODEC(Delta, ZSTD(1)),
  namespace     LowCardinality(String) MATERIALIZED JSONExtractString(object, 'metadata', 'namespace'),
  parent_kind   LowCardinality(String) MATERIALIZED JSONExtractString(object, 'spec', 'parentRef', 'kind'),
  parent_name   String                 MATERIALIZED JSONExtractString(object, 'spec', 'parentRef', 'name'),
  invocation_id String                 MATERIALIZED JSONExtractString(object, 'metadata', 'name'),
  created_at    DateTime64(3, 'UTC')   MATERIALIZED parseDateTime64BestEffort(JSONExtractString(object, 'metadata', 'creationTimestamp')),
  phase         LowCardinality(String) MATERIALIZED JSONExtractString(object, 'status', 'phase'),
  INDEX idx_seq stream_seq TYPE minmax GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (namespace, parent_kind, parent_name, invocation_id, stream_seq)
TTL toDateTime(event_time) + INTERVAL %d DAY
SETTINGS index_granularity = 8192
`
