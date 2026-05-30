package invocation

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// listLimit bounds. defaultListLimit applies when the client sends no
// (or zero) limit — we never treat "unset" as "unbounded", which is the
// footgun the design called out; well-behaved clients page via the
// continue token. maxListLimit caps an over-eager explicit limit.
const (
	defaultListLimit = 100
	maxListLimit     = 1000
)

// listCursor is the decoded continue token. It pins the snapshot
// resourceVersion so a multi-page list is consistent (every page reads
// stream_seq <= RV), plus the (min_created_at, invocation_id) of the
// last row returned. created_at is NOT invariant across an invocation's
// events (ingress and the worker stamp independent creationTimestamps),
// so TS holds the per-invocation min(created_at) and the cursor predicate
// runs as a post-aggregation HAVING on that same aggregate — never on raw
// event rows.
type listCursor struct {
	RV uint64 `json:"rv"`
	TS int64  `json:"ts"` // min(created_at), unix millis
	ID string `json:"id"` // invocation_id
}

func encodeCursor(c listCursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (listCursor, error) {
	var c listCursor
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return c, fmt.Errorf("decode continue token: %w", err)
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return c, fmt.Errorf("parse continue token: %w", err)
	}
	return c, nil
}
