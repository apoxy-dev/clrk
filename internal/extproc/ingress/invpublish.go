package ingress

import (
	"context"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/invevent"
)

// natsProvider is the subset of the embedded NATS server the ingress
// invocation publisher needs. *internal/nats.Server satisfies it
// structurally; declaring it here keeps the ingress package off the
// embedded nats-server import.
type natsProvider interface {
	Ready(ctx context.Context) error
	Connect(name string) (*nats.Conn, error)
}

const (
	// invQueueSize bounds the in-flight Invocation event queue. Events
	// are tiny JSON snapshots; the buffer is generous so a brief
	// JetStream stall (file fsync, reconnect) never reaches back into
	// the dispatch hot path. On overflow we drop and warn rather than
	// block — see emitInvocation.
	invQueueSize = 1024

	// invPublishTimeout bounds a single JetStream publish so a wedged
	// server can't pin the drain goroutine forever.
	invPublishTimeout = 5 * time.Second
)

// emitInvocation enqueues a complete Invocation snapshot for async
// publication to the JetStream INVOCATIONS stream. It is the ingress
// server's only write into the Invocation lifecycle.
//
// Two reasons it is best-effort (enqueue-or-drop, never block):
//   - The dispatch response must not wait on a JetStream ack; coupling
//     request admission to the durability layer would turn a NATS hiccup
//     into a dispatch outage.
//   - Every event ingress emits is a birth event (Dispatched on accept,
//     Rejected on a capacity/readiness denial). Dropping a Dispatched
//     only delays the materialized first-seen state until the worker's
//     Running event (which informers fold in as an upsert); dropping a
//     Rejected loses a record of a request that never ran. Neither is a
//     billing-critical terminal event — those originate on the worker
//     (APO-618) and carry a durable outbox.
//
// No-op until the controller-manager wires a publisher via
// RunInvocationPublisher (NATS disabled, or before it is ready), so
// tests and the pre-ready window never panic.
func (s *Server) emitInvocation(namespace, agent, id string, trigger clrkv1alpha1.InvocationTriggerType, phase clrkv1alpha1.InvocationPhase) {
	if s.invPub.Load() == nil {
		return
	}
	inv := &clrkv1alpha1.Invocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:              id,
			Namespace:         namespace,
			CreationTimestamp: metav1.Now(),
		},
		Spec: clrkv1alpha1.InvocationSpec{
			ParentRef: clrkv1alpha1.InvocationParentRef{
				Kind: clrkv1alpha1.InvocationParentTaskAgent,
				Name: agent,
			},
			Trigger: clrkv1alpha1.InvocationTrigger{
				Type: trigger,
			},
		},
		Status: clrkv1alpha1.InvocationStatus{Phase: phase},
	}
	select {
	case s.invQueue <- inv:
	default:
		slog.Warn("ingress ext_proc: invocation event queue full, dropping",
			"id", id, "agent", agent, "phase", phase)
	}
}

// RunInvocationPublisher waits for the embedded NATS server to be ready,
// opens an in-process JetStream connection, and drains the invocation
// event queue until ctx is cancelled. The controller-manager calls it in
// a goroutine after constructing the NATS server; blocking on Ready
// internally keeps startup sequencing out of main. Connection failures
// are logged and leave the publisher unset (emitInvocation stays a
// no-op), so a NATS problem degrades lifecycle recording without
// touching dispatch.
func (s *Server) RunInvocationPublisher(ctx context.Context, provider natsProvider) {
	if err := provider.Ready(ctx); err != nil {
		if ctx.Err() == nil {
			slog.Error("ingress ext_proc: NATS not ready for invocation publisher", "err", err)
		}
		return
	}
	nc, err := provider.Connect("clrk-ingress-invocations")
	if err != nil {
		slog.Error("ingress ext_proc: NATS connect for invocation publisher", "err", err)
		return
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		slog.Error("ingress ext_proc: jetstream for invocation publisher", "err", err)
		return
	}
	s.drainInvocations(ctx, invevent.NewPublisher(js))
}

// drainInvocations publishes queued snapshots until ctx is cancelled.
// Storing the publisher before the loop starts means emitInvocation
// begins enqueuing as soon as the publisher is live; the bounded queue
// absorbs the brief gap before the loop's first receive.
func (s *Server) drainInvocations(ctx context.Context, pub *invevent.Publisher) {
	s.invPub.Store(pub)
	defer s.invPub.Store(nil)
	for {
		select {
		case <-ctx.Done():
			return
		case inv := <-s.invQueue:
			// Ingress only emits birth events, so first is always true:
			// the Watch maps it to a Kubernetes Added.
			pctx, cancel := context.WithTimeout(ctx, invPublishTimeout)
			if _, err := pub.Publish(pctx, inv, true); err != nil {
				slog.Warn("ingress ext_proc: publish invocation event failed",
					"id", inv.Name, "agent", inv.Spec.ParentRef.Name, "phase", inv.Status.Phase, "err", err)
			}
			cancel()
		}
	}
}
