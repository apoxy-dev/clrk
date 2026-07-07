package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/invevent"
)

// invocationDurable names the notifications watcher's durable JetStream
// consumer -- distinct from the ClickHouse materializer (clrk-ch-materializer)
// and the apiserver's ephemeral watch consumers.
const invocationDurable = "clrk-notify-invocations"

// InvocationWatcher records a Warning Event when an Invocation reaches a
// terminal failure phase (Failed / Timeout / Rejected). It reads the INVOCATIONS
// JetStream -- where every phase transition is published as a full snapshot --
// rather than polling ClickHouse or standing up an informer over the custom
// Invocation storage. DeliverNew so a cm restart does not replay historical
// failures into fresh notifications.
type InvocationWatcher struct {
	js  jetstream.JetStream
	rec *Recorder
}

// NewInvocationWatcher returns a watcher over js recording into rec.
func NewInvocationWatcher(js jetstream.JetStream, rec *Recorder) *InvocationWatcher {
	return &InvocationWatcher{js: js, rec: rec}
}

// Run creates the durable consumer and records failure Events until ctx is
// cancelled. Blocks; intended for its own goroutine (go notify.RunInvocationWatcher).
func (w *InvocationWatcher) Run(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("notify-invocations")
	cons, err := w.js.CreateOrUpdateConsumer(ctx, invevent.StreamName, jetstream.ConsumerConfig{
		Durable:       invocationDurable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		FilterSubject: invevent.StreamWildcard,
	})
	if err != nil {
		return fmt.Errorf("create notify invocation consumer: %w", err)
	}
	logger.Info("Invocation notification watcher started", "durable", invocationDurable)
	for {
		if ctx.Err() != nil {
			return nil
		}
		batch, err := cons.Fetch(64, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		for msg := range batch.Messages() {
			w.handle(msg)
			_ = msg.Ack()
		}
	}
}

func (w *InvocationWatcher) handle(msg jetstream.Msg) {
	var inv clrkv1alpha1.Invocation
	if err := json.Unmarshal(msg.Data(), &inv); err != nil {
		return
	}
	reason, ok := terminalFailureReason(inv.Status.Phase)
	if !ok {
		return
	}
	// The parent kind strings ("TaskAgent"/"DaemonAgent") match the agent-kind
	// constants, so regardingAgent resolves the owning agent. The Invocation is
	// the related object.
	regarding := regardingAgent(string(inv.Spec.ParentRef.Kind), inv.Namespace, inv.Spec.ParentRef.Name, "")
	if regarding == nil {
		return
	}
	related := &clrkv1alpha1.Invocation{
		TypeMeta:   metav1.TypeMeta{Kind: "Invocation", APIVersion: clrkv1alpha1.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{Namespace: inv.Namespace, Name: inv.Name, UID: inv.UID},
	}
	w.rec.Eventf(regarding, related, TypeWarning, reason, ActionRun, invocationNote(&inv))
}

// terminalFailureReason maps a terminal failure phase to its Event reason. Only
// failures notify; Succeeded is silent.
func terminalFailureReason(phase clrkv1alpha1.InvocationPhase) (string, bool) {
	switch phase {
	case clrkv1alpha1.InvocationPhaseFailed:
		return ReasonInvocationFailed, true
	case clrkv1alpha1.InvocationPhaseTimeout:
		return ReasonInvocationTimeout, true
	case clrkv1alpha1.InvocationPhaseRejected:
		return ReasonInvocationRejected, true
	default:
		return "", false
	}
}

// invocationNote renders a short human note, preferring the latest status
// condition message.
func invocationNote(inv *clrkv1alpha1.Invocation) string {
	if len(inv.Status.Conditions) > 0 {
		last := inv.Status.Conditions[len(inv.Status.Conditions)-1]
		if last.Message != "" {
			return fmt.Sprintf("Invocation %s %s: %s", inv.Name, inv.Status.Phase, last.Message)
		}
	}
	return fmt.Sprintf("Invocation %s %s", inv.Name, inv.Status.Phase)
}
