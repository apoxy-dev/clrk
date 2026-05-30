package invocation

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/apoxy-dev/clrk/internal/invevent"
)

const (
	// consumerDurable names the single durable JetStream consumer that
	// materializes the stream into ClickHouse. Durable so a cm restart
	// resumes from the last ack rather than re-materializing the whole
	// stream. Distinct from the ephemeral ordered consumers the Watch
	// path creates per request.
	consumerDurable = "clrk-ch-materializer"

	// defaultBatchSize / defaultFlushWait bound how many events a single
	// INSERT carries and how long Fetch blocks waiting to fill a batch.
	// Mirrors the conservative thresholds in internal/chwriter.
	defaultBatchSize = 256
	defaultFlushWait = time.Second
)

// Consumer materializes the JetStream INVOCATIONS stream into the
// ClickHouse invocation_events table. It is the single durable reader
// of the stream (the apiserver Watch uses ephemeral ordered consumers
// instead). The high-water mark it publishes — the highest stream_seq
// committed to ClickHouse — is the resourceVersion Storage.List
// reports, so a client that Lists then Watches from rv+1 never crosses
// the materialization gap.
type Consumer struct {
	pool      Doer
	js        jetstream.JetStream
	highWater *atomic.Uint64
	batchSize int
	flushWait time.Duration
}

// NewConsumer wires a materializer over the given ClickHouse pool and
// JetStream. highWater is shared with the Storage instances so List can
// report the committed sequence as the list resourceVersion.
func NewConsumer(pool Doer, js jetstream.JetStream, highWater *atomic.Uint64) *Consumer {
	return &Consumer{
		pool:      pool,
		js:        js,
		highWater: highWater,
		batchSize: defaultBatchSize,
		flushWait: defaultFlushWait,
	}
}

// Run creates (or resumes) the durable consumer and materializes events
// until ctx is cancelled. Blocks; intended to run on its own goroutine.
func (c *Consumer) Run(ctx context.Context) error {
	// Seed the high-water mark from what is already materialized before
	// serving. The durable consumer resumes from its last ack and does
	// not re-deliver already-materialized events, so without this a cm
	// restart leaves highWater at 0 and List would bound every read to
	// `stream_seq <= 0` (empty) until a brand-new event re-advanced it.
	c.seedHighWater(ctx)

	cons, err := c.js.CreateOrUpdateConsumer(ctx, invevent.StreamName, jetstream.ConsumerConfig{
		Durable:       consumerDurable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		// No FilterSubject: materialize the whole stream.
	})
	if err != nil {
		return fmt.Errorf("create materializer consumer: %w", err)
	}
	slog.Info("Invocation materializer started",
		"stream", invevent.StreamName, "durable", consumerDurable,
		"high_water", c.highWater.Load())

	for {
		if ctx.Err() != nil {
			return nil
		}
		batch, err := cons.Fetch(c.batchSize, jetstream.FetchMaxWait(c.flushWait))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Warn("Invocation materializer fetch", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		if err := c.drain(ctx, batch); err != nil {
			// drain retries ClickHouse inserts in place (in stream order),
			// so a non-nil return means only that ctx was cancelled
			// mid-retry — shut down. The batch is left unacked and
			// redelivers on the next process start.
			return nil
		}
	}
}

// drain inserts one fetched batch into ClickHouse and acks it. Messages
// are acked only after the INSERT succeeds, so a crash mid-batch
// re-delivers the unmaterialized events (at-least-once; duplicate
// same-seq rows are idempotent under the read-side argMax). The
// high-water mark advances only after a successful insert+ack.
func (c *Consumer) drain(ctx context.Context, batch jetstream.MessageBatch) error {
	objects := new(proto.ColStr)
	seqs := new(proto.ColUInt64)
	times := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC)

	var (
		acks   []jetstream.Msg
		maxSeq uint64
	)
	for msg := range batch.Messages() {
		meta, err := msg.Metadata()
		if err != nil {
			// Not a JetStream message we can sequence; drop it so it
			// doesn't wedge the consumer.
			_ = msg.Term()
			continue
		}
		seq := meta.Sequence.Stream
		objects.Append(string(msg.Data()))
		seqs.Append(seq)
		times.Append(meta.Timestamp.UTC())
		acks = append(acks, msg)
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	if err := batch.Error(); err != nil && ctx.Err() == nil {
		slog.Warn("Invocation materializer batch error", "err", err)
	}
	if len(acks) == 0 {
		return nil
	}

	// Insert with in-place retry. A transient ClickHouse error (restart,
	// merge backpressure, a network blip) must neither kill the
	// materializer for the process lifetime nor let a later batch jump
	// ahead of this one: we hold this batch in memory and retry the
	// INSERT until it commits or the process shuts down. Materializing
	// strictly in stream order keeps the high-water mark contiguous, so a
	// List(rv)-then-Watch(rv+1) handoff never advertises a sequence whose
	// events are not yet in ClickHouse. ch-go's Input columns are read,
	// not consumed, by Do, so retrying re-sends the same block.
	body := fmt.Sprintf("INSERT INTO %s.%s (object, stream_seq, event_time) VALUES", Database, Table)
	q := ch.Query{
		Body: body,
		Input: proto.Input{
			{Name: "object", Data: objects},
			{Name: "stream_seq", Data: seqs},
			{Name: "event_time", Data: times},
		},
	}
	backoff := 250 * time.Millisecond
	for {
		if err := c.pool.Do(ctx, q); err == nil {
			break
		} else if ctx.Err() != nil {
			// Shutting down: leave the batch unacked for redelivery.
			return ctx.Err()
		} else {
			slog.Warn("Invocation materializer insert; retrying", "err", err, "events", len(acks))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
	}

	for _, m := range acks {
		_ = m.Ack()
	}
	c.advanceHighWater(maxSeq)
	return nil
}

// advanceHighWater raises the high-water mark to seq, never lowering it.
// CAS guards against a concurrent advance (seedHighWater vs drain),
// though Run is single-goroutine today.
func (c *Consumer) advanceHighWater(seq uint64) {
	for {
		cur := c.highWater.Load()
		if seq <= cur || c.highWater.CompareAndSwap(cur, seq) {
			return
		}
	}
}

// seedHighWater initialises the high-water mark from the maximum
// stream_seq already materialized in ClickHouse, so List reports a
// correct resourceVersion immediately after a cm restart instead of 0.
// Best-effort: a query failure leaves the mark unchanged and it
// self-heals as the durable consumer re-materializes new events.
func (c *Consumer) seedHighWater(ctx context.Context) {
	var m proto.ColUInt64
	if err := c.pool.Do(ctx, ch.Query{
		Body:   fmt.Sprintf("SELECT max(stream_seq) AS m FROM %s.%s", Database, Table),
		Result: proto.Results{{Name: "m", Data: &m}},
	}); err != nil {
		slog.Warn("Invocation materializer seed high-water", "err", err)
		return
	}
	if m.Rows() > 0 {
		c.advanceHighWater(m.Row(0))
	}
}
