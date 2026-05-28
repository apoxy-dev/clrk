package invocation

// Database is the ClickHouse schema name shared with internal/chwriter
// (otel_logs / otel_traces). The embedded engine starts "default"
// pre-created so DDL is idempotent across controller-manager restarts.
const Database = "default"

// Table is the canonical Invocation storage table. One row per
// observed object version; ReplacingMergeTree(stream_seq) keeps the
// row with the higher seq after merge so updates Just Work via INSERT.
const Table = "invocations"

// createTableTmpl renders the DDL. %d is the TTL in days. The object
// column holds the full Invocation as JSON; all indexed columns are
// MATERIALIZED projections derived from the JSON at write time, which
// means Spec/Status field additions to the Go type require zero
// schema migration — they show up in `object` next insert and are
// promotable to a hot column later via ALTER ... ADD COLUMN ...
// MATERIALIZED JSONExtract(...).
const createTableTmpl = `
CREATE TABLE IF NOT EXISTS ` + Database + `.` + Table + ` (
  object       String                 CODEC(ZSTD(1)),
  namespace    LowCardinality(String) MATERIALIZED JSONExtractString(object, 'metadata', 'namespace'),
  parent_kind  LowCardinality(String) MATERIALIZED JSONExtractString(object, 'spec', 'parentRef', 'kind'),
  parent_name  String                 MATERIALIZED JSONExtractString(object, 'spec', 'parentRef', 'name'),
  id           String                 MATERIALIZED JSONExtractString(object, 'metadata', 'name'),
  created_at   DateTime64(3, 'UTC')   MATERIALIZED parseDateTime64BestEffort(JSONExtractString(object, 'metadata', 'creationTimestamp')),
  phase        LowCardinality(String) MATERIALIZED JSONExtractString(object, 'status', 'phase'),
  stream_seq   UInt64                 CODEC(Delta, ZSTD(1)),
  INDEX idx_id id TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(stream_seq)
PARTITION BY toYYYYMM(created_at)
ORDER BY (namespace, parent_kind, parent_name, created_at, id)
TTL toDateTime(created_at) + INTERVAL %d DAY
SETTINGS index_granularity = 8192
`
