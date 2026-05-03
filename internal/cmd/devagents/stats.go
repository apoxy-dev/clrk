package devagents

import (
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/apoxy-dev/clrk/internal/cmd/devotel"
)

// statsWindow is the rolling time window every per-agent counter respects.
// Sized to one minute so the row sparkline shows a recognisable shape
// without needing the user to wait.
const statsWindow = time.Minute

// sparkBuckets is the size of the per-second request-count ring. Sized
// to the 60s rolling window so reqs/m equals the sum of the ring.
const sparkBuckets = 60

// reqLogCap and reqSpanCap bound the per-agent ring buffers used by the
// detail view. Sized to roughly five minutes at 1 req/sec.
const (
	logRingCap  = 256
	spanRingCap = 256
)

// agentStats holds rolling telemetry for one agent. All fields are
// guarded by mu — readers (the TUI) snapshot under mu so they never see
// torn updates.
type agentStats struct {
	mu sync.Mutex

	// reqBuckets is a circular buffer of per-second request counts.
	// bucketAt indexes the most recent bucket; older buckets walk
	// backwards (mod sparkBuckets). Each bucket also carries the wall
	// clock second it represents so stale buckets get zeroed on read.
	reqBuckets [sparkBuckets]int
	bucketTime [sparkBuckets]int64
	bucketAt   int

	tokensIn1m  int64
	tokensOut1m int64
	tokensInTot int64
	tokensOutT  int64

	// latencyMs holds the last N request durations (ms). Bounded so a
	// long-running dev session doesn't grow unboundedly. The newest
	// element wraps the oldest.
	latencyMs   [sparkBuckets * 2]int
	latencyHead int
	latencyN    int

	lastStatus int
	lastSeen   time.Time

	logs  ringLog
	spans ringSpan
}

// addLog records one OTLP log record for an agent. It updates rolling
// counters and appends to the per-agent log ring.
func (s *agentStats) addLog(rec devotel.LogRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastSeen = rec.Time
	if v, ok := rec.Attributes["http.response.status_code"]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			s.lastStatus = n
		}
	}
	if v := rec.Attributes["clrk.duration_ms"]; v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			s.recordLatency(n)
		}
	}
	if v := rec.Attributes["gen_ai.usage.input_tokens"]; v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			s.tokensInTot += n
		}
	}
	if v := rec.Attributes["gen_ai.usage.output_tokens"]; v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			s.tokensOutT += n
		}
	}
	s.bumpRequest(rec.Time)

	s.logs.push(LogEvent{
		Time:       rec.Time,
		Body:       rec.Body,
		Severity:   rec.Severity,
		Attributes: copyAttrs(rec.Attributes),
	})
}

// addSpan records one OTLP span. We treat spans as the authoritative
// per-request event for latency (extproc clamps span start/end to the
// HTTP wall clock) and for the per-agent span ring.
func (s *agentStats) addSpan(sp devotel.Span) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if sp.Time.After(s.lastSeen) {
		s.lastSeen = sp.Time
	}
	if sp.Duration > 0 {
		s.recordLatency(int(sp.Duration / time.Millisecond))
	}
	s.spans.push(SpanEvent{
		Time:       sp.Time,
		Duration:   sp.Duration,
		Name:       sp.Name,
		Status:     sp.Status,
		TraceID:    sp.TraceID,
		SpanID:     sp.SpanID,
		Attributes: copyAttrs(sp.Attributes),
	})
}

// bumpRequest credits the bucket aligned to t. Caller holds mu.
func (s *agentStats) bumpRequest(t time.Time) {
	if t.IsZero() {
		t = time.Now()
	}
	sec := t.Unix()
	cur := s.bucketTime[s.bucketAt]
	if sec == cur {
		s.reqBuckets[s.bucketAt]++
		return
	}
	// Walk forward, zeroing skipped buckets so an idle gap shows as
	// flatline rather than carrying old counts.
	gap := sec - cur
	if cur == 0 || gap >= sparkBuckets {
		for i := range s.reqBuckets {
			s.reqBuckets[i] = 0
			s.bucketTime[i] = 0
		}
		s.bucketAt = 0
		s.reqBuckets[0] = 1
		s.bucketTime[0] = sec
		return
	}
	for i := int64(1); i <= gap; i++ {
		s.bucketAt = (s.bucketAt + 1) % sparkBuckets
		s.reqBuckets[s.bucketAt] = 0
		s.bucketTime[s.bucketAt] = cur + i
	}
	s.reqBuckets[s.bucketAt] = 1
}

func (s *agentStats) recordLatency(ms int) {
	idx := s.latencyHead % len(s.latencyMs)
	s.latencyMs[idx] = ms
	s.latencyHead++
	if s.latencyN < len(s.latencyMs) {
		s.latencyN++
	}
}

// snapshotInto copies rolling stats into dst. Caller-supplied dst lets
// the store reuse a buffer per render. We zero-fill stale buckets so an
// agent that stopped serving traffic decays smoothly to flatline.
func (s *agentStats) snapshotInto(dst *Snapshot, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Tokens & last status apply unchanged. The rolling 1m token
	// counters also walk the bucket ring so they decay, but extproc
	// publishes per-request totals (not per-second deltas) — for now
	// we use cumulative totals which is what the user wants to see
	// in the "tokens in/out" column.
	dst.TokensInTotal = s.tokensInTot
	dst.TokensOutTotal = s.tokensOutT
	dst.LastStatus = s.lastStatus
	dst.LastSeen = s.lastSeen

	// 1m request count from the bucket ring.
	cutoff := now.Unix() - sparkBuckets + 1
	reqs := 0
	for i := 0; i < sparkBuckets; i++ {
		idx := (s.bucketAt + 1 + i) % sparkBuckets
		ts := s.bucketTime[idx]
		if ts == 0 || ts < cutoff {
			continue
		}
		reqs += s.reqBuckets[idx]
	}
	dst.Reqs1m = reqs

	// Token-per-minute estimates: we don't yet track a per-second
	// token bucket, so derive 1m totals as a fraction of cumulative
	// scaled by the 1m request share. Cheap heuristic; replace with a
	// real bucket if it ever misleads.
	if reqs > 0 && s.tokensInTot > 0 {
		// Approximate: assume rolling reqs1m share of total requests
		// reflects token share. We don't have totalReqs handy without
		// another counter, so just publish cumulative for now.
	}
	dst.TokensIn1m = s.tokensInTot
	dst.TokensOut1m = s.tokensOutT

	dst.P50, dst.P95 = s.latencyPercentiles()
}

// latencyPercentiles returns p50/p95 over the buffered window.
// Caller holds mu. Empty buffer → both zero.
func (s *agentStats) latencyPercentiles() (time.Duration, time.Duration) {
	if s.latencyN == 0 {
		return 0, 0
	}
	tmp := make([]int, s.latencyN)
	if s.latencyHead < len(s.latencyMs) {
		copy(tmp, s.latencyMs[:s.latencyN])
	} else {
		// Buffer is full — newest at (head-1)%len, oldest at head%len.
		start := s.latencyHead % len(s.latencyMs)
		n := copy(tmp, s.latencyMs[start:])
		copy(tmp[n:], s.latencyMs[:start])
	}
	sort.Ints(tmp)
	p50 := tmp[(len(tmp)*50)/100]
	p95Idx := (len(tmp) * 95) / 100
	if p95Idx >= len(tmp) {
		p95Idx = len(tmp) - 1
	}
	return time.Duration(p50) * time.Millisecond, time.Duration(tmp[p95Idx]) * time.Millisecond
}

// copyLogs returns a chronological snapshot of buffered log events.
// Returned slice is the caller's to keep; we copy under mu so the TUI
// renders stable data.
func (s *agentStats) copyLogs() []LogEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logs.snapshot()
}

func (s *agentStats) copySpans() []SpanEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spans.snapshot()
}

func copyAttrs(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// ringLog is a fixed-capacity FIFO of LogEvent. push overwrites the
// oldest entry once full. snapshot returns events in chronological
// order.
type ringLog struct {
	buf  [logRingCap]LogEvent
	head int
	n    int
}

func (r *ringLog) push(e LogEvent) {
	idx := (r.head + r.n) % logRingCap
	if r.n < logRingCap {
		r.buf[idx] = e
		r.n++
		return
	}
	r.buf[r.head] = e
	r.head = (r.head + 1) % logRingCap
}

func (r *ringLog) snapshot() []LogEvent {
	if r.n == 0 {
		return nil
	}
	out := make([]LogEvent, r.n)
	for i := 0; i < r.n; i++ {
		out[i] = r.buf[(r.head+i)%logRingCap]
	}
	return out
}

type ringSpan struct {
	buf  [spanRingCap]SpanEvent
	head int
	n    int
}

func (r *ringSpan) push(e SpanEvent) {
	idx := (r.head + r.n) % spanRingCap
	if r.n < spanRingCap {
		r.buf[idx] = e
		r.n++
		return
	}
	r.buf[r.head] = e
	r.head = (r.head + 1) % spanRingCap
}

func (r *ringSpan) snapshot() []SpanEvent {
	if r.n == 0 {
		return nil
	}
	out := make([]SpanEvent, r.n)
	for i := 0; i < r.n; i++ {
		out[i] = r.buf[(r.head+i)%spanRingCap]
	}
	return out
}
