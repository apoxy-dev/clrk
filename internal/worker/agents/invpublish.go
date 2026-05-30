package agents

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/invevent"
)

const (
	// invQueueSize bounds the in-memory outbox. Worker-emitted events are
	// tiny and bounded by in-flight concurrency, so this absorbs a
	// realistic controller-manager bounce without dropping a terminal
	// event. A prolonged cm outage will eventually drop events as their
	// retry budgets expire (see invRetryBudget); a disk-backed outbox to
	// also survive a worker restart is a future hardening.
	invQueueSize = 2048

	// invAttemptTimeout caps one JetStream publish round-trip.
	invAttemptTimeout = 5 * time.Second

	// invRetryInitial / invRetryMax bound the exponential backoff between
	// publish attempts while the cm is unreachable.
	invRetryInitial = 250 * time.Millisecond
	invRetryMax     = 5 * time.Second

	// invRetryBudget is how long a single event is retried before being
	// dropped. Sized to span a controller-manager rollout (the durability
	// requirement: a terminal billing event must survive a cm bounce
	// while the worker stays up). Matches hardTimeoutCap so an event is
	// retried at least as long as the execution that produced it could
	// have run.
	invRetryBudget = 5 * time.Minute

	// invReconnectWait paces nats.go's transparent reconnect attempts.
	invReconnectWait = 2 * time.Second
)

// invPublisher ships worker-side Invocation lifecycle events (Running and
// the terminal Succeeded / Failed / Timeout) to the controller-manager's
// JetStream over TCP. The dispatcher enqueues a complete snapshot per
// transition; a single drain goroutine publishes them in FIFO order,
// which preserves per-invocation ordering (Running before its terminal).
//
// Durability: the underlying nats.go connection auto-reconnects, and each
// event is retried from the in-memory outbox until JetStream acks it, so
// a controller-manager bounce mid-execution does not drop the
// billing-relevant terminal event (the worker stays up across the bounce
// and replays on reconnect). The full-snapshot wire contract plus the
// MsgIDHeader dedup window make a retried publish idempotent.
type invPublisher struct {
	addr  string
	name  string
	queue chan *clrkv1alpha1.Invocation
}

// newInvPublisher constructs a publisher targeting the cm NATS address.
// name labels the connection (cm-side logs); use the worker pod name.
func newInvPublisher(addr, name string) *invPublisher {
	return &invPublisher{
		addr:  addr,
		name:  name,
		queue: make(chan *clrkv1alpha1.Invocation, invQueueSize),
	}
}

// enqueue hands a snapshot to the drain goroutine without blocking the
// dispatch hot path. On a full outbox (a sustained cm outage with more
// terminal events than buffer) it drops and warns rather than stalling
// dispatch.
func (p *invPublisher) enqueue(inv *clrkv1alpha1.Invocation) {
	select {
	case p.queue <- inv:
	default:
		slog.Warn("worker: invocation event outbox full, dropping",
			"id", inv.Name, "agent", inv.Spec.ParentRef.Name, "phase", inv.Status.Phase)
	}
}

// Run dials the cm JetStream and drains the outbox until ctx is
// cancelled. RetryOnFailedConnect means a cm that isn't up yet at worker
// start is not fatal — the connection enters reconnect and publishes
// flush once it lands. Returns when ctx is done.
func (p *invPublisher) Run(ctx context.Context) error {
	nc, err := nats.Connect(p.addr,
		nats.Name(p.name),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(invReconnectWait),
	)
	if err != nil {
		return fmt.Errorf("connect cm NATS %q: %w", p.addr, err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}
	pub := invevent.NewPublisher(js)
	for {
		select {
		case <-ctx.Done():
			return nil
		case inv := <-p.queue:
			p.publishWithRetry(ctx, pub, inv)
		}
	}
}

// publishWithRetry publishes one snapshot, retrying with capped
// exponential backoff until JetStream acks it, ctx is cancelled, or the
// retry budget elapses. first is always false: the worker never emits an
// invocation's birth event (that's ingress Dispatched / cron Pending), so
// the Watch maps these to Modified.
func (p *invPublisher) publishWithRetry(ctx context.Context, pub *invevent.Publisher, inv *clrkv1alpha1.Invocation) {
	deadline := time.Now().Add(invRetryBudget)
	backoff := invRetryInitial
	for {
		actx, cancel := context.WithTimeout(ctx, invAttemptTimeout)
		_, err := pub.Publish(actx, inv, false)
		cancel()
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			return // worker shutting down
		}
		if time.Now().After(deadline) {
			slog.Warn("worker: giving up on invocation event after retry budget",
				"id", inv.Name, "agent", inv.Spec.ParentRef.Name, "phase", inv.Status.Phase, "err", err)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > invRetryMax {
			backoff = invRetryMax
		}
	}
}
