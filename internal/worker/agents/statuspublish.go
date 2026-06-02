//go:build linux

package agents

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/apoxy-dev/clrk/internal/invevent"
	"github.com/apoxy-dev/clrk/internal/worker/sandbox"
)

const (
	// statusFloorInterval is the floor cadence at which the worker re-Puts
	// its status when the activeCounter/warm-pool notifier hasn't fired.
	// The controller-manager's dead-after window is 3x this (15s); the KV
	// bucket TTL (20s) sits above that for hygiene. Doubles as the
	// liveness heartbeat — the cm advances lastSeen on every received Put.
	statusFloorInterval = 5 * time.Second

	// statusBindRetry paces re-binding the KV bucket when it isn't created
	// yet (a worker can start ahead of the cm that owns the bucket).
	statusBindRetry = 2 * time.Second

	// statusOpTimeout caps one KV Put/Delete round-trip.
	statusOpTimeout = 5 * time.Second
)

// StatusPublisher publishes this worker's routing state (warm sandboxes,
// in-flight dispatches, cached images) as the value of its key in the
// controller-manager's WORKER_STATUS JetStream KV bucket. The value is
// last-writer-wins: a complete snapshot is Put on every activeCounter /
// warm-pool change and on a 5s floor, and the key is Deleted on graceful
// shutdown so the cm drops this worker from routing immediately
// (ungraceful death falls through to the cm's lastSeen staleness + the
// bucket TTL).
//
// Unlike invPublisher there is no retry-budget outbox: status is
// idempotent latest-wins state, so a dropped Put is superseded by the next
// one within one floor interval. It opens its own NATS connection — the
// invocation publisher owns a separate one; consolidating the two onto a
// single shared connection is a future cleanup.
type StatusPublisher struct {
	addr       string
	key        string
	sandboxMgr *sandbox.Manager
	imageStore *sandbox.ImageStore
	active     *activeCounter
}

// NewStatusPublisher constructs a StatusPublisher. key inputs (namespace,
// pool, podName) must match what the controller-manager reconstructs from
// the WorkerPool + the pool Service's EndpointSlice targetRef, so the cm
// can join this worker's status to its routable pod IP.
func NewStatusPublisher(addr, namespace, pool, podName string, sandboxMgr *sandbox.Manager, imageStore *sandbox.ImageStore, active *activeCounter) *StatusPublisher {
	return &StatusPublisher{
		addr:       addr,
		key:        invevent.WorkerStatusKey(namespace, pool, podName),
		sandboxMgr: sandboxMgr,
		imageStore: imageStore,
		active:     active,
	}
}

// Run connects to the cm NATS, binds the WORKER_STATUS bucket, and Puts a
// fresh snapshot on every notifier fire plus a 5s floor until ctx is
// cancelled, then Deletes the worker's key. Returns when ctx is done.
func (p *StatusPublisher) Run(ctx context.Context) error {
	nc, err := nats.Connect(p.addr,
		nats.Name("clrk-worker-status-"+p.key),
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

	kv, err := p.bindKV(ctx, js)
	if err != nil {
		return nil // ctx cancelled while waiting for the bucket
	}

	notify := p.active.Notifier().Subscribe()
	defer p.active.Notifier().Unsubscribe(notify)

	tick := time.NewTicker(statusFloorInterval)
	defer tick.Stop()

	p.put(ctx, kv)
	for {
		select {
		case <-ctx.Done():
			p.delete(kv)
			return nil
		case <-notify:
			tick.Reset(statusFloorInterval)
		case <-tick.C:
		}
		p.put(ctx, kv)
	}
}

// bindKV binds the WORKER_STATUS bucket, retrying until it exists (the cm
// creates it on startup; a worker may race ahead of a cm bounce) or ctx is
// cancelled. The underlying nats.go connection auto-reconnects underneath
// this loop, so only the bind itself needs retrying.
func (p *StatusPublisher) bindKV(ctx context.Context, js jetstream.JetStream) (jetstream.KeyValue, error) {
	for {
		kv, err := js.KeyValue(ctx, invevent.WorkerStatusBucket)
		if err == nil {
			return kv, nil
		}
		slog.Warn("worker: WORKER_STATUS KV not ready, retrying", "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(statusBindRetry):
		}
	}
}

func (p *StatusPublisher) put(ctx context.Context, kv jetstream.KeyValue) {
	data, err := proto.Marshal(buildWorkerStatus(p.sandboxMgr, p.imageStore, p.active))
	if err != nil {
		slog.Error("worker: marshal worker status", "err", err)
		return
	}
	pctx, cancel := context.WithTimeout(ctx, statusOpTimeout)
	defer cancel()
	if _, err := kv.Put(pctx, p.key, data); err != nil && ctx.Err() == nil {
		// Latest-wins: the next Put (<=5s) supersedes a dropped one, so no
		// retry/outbox is warranted here.
		slog.Warn("worker: status KV Put failed", "key", p.key, "err", err)
	}
}

func (p *StatusPublisher) delete(kv jetstream.KeyValue) {
	// ctx is already cancelled here (graceful shutdown); use a fresh
	// bounded context so the dereg still reaches the cm before nc.Close.
	dctx, cancel := context.WithTimeout(context.Background(), statusOpTimeout)
	defer cancel()
	if err := kv.Delete(dctx, p.key); err != nil {
		slog.Warn("worker: status KV Delete failed", "key", p.key, "err", err)
	}
}
