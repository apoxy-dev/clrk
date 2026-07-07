package notify

import (
	"context"
	"sync/atomic"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/otelemit"
)

// Egress span names the worker bridge emits
// (internal/worker/sandbox/egress_bridge.go). Matched here rather than shared as
// constants because the worker is a linux-only build; keep in sync.
const (
	spanEgressDenied = "egress.dial.denied"
	spanEgressFailed = "egress.dial.failed"
)

// queueDepth bounds the denial-signal queue. Denials recur, so a bounded loss
// under a burst is acceptable; the drop counter surfaces the rate.
const queueDepth = 256

// EgressSecurityBridge turns worker egress-denial OTLP spans into
// events.k8s.io/v1 Events. Workers stay credential-free: they emit OTLP, and the
// control plane (which holds identity and an apiserver client) records the
// Event. It implements both SecurityObserver (the receiver-side, non-blocking
// enqueue) and manager.Runnable (the queue drain that writes Events).
type EgressSecurityBridge struct {
	rec     *Recorder
	ch      chan denialSignal
	dropped atomic.Uint64
}

// NewEgressSecurityBridge returns a bridge recording into rec.
func NewEgressSecurityBridge(rec *Recorder) *EgressSecurityBridge {
	return &EgressSecurityBridge{rec: rec, ch: make(chan denialSignal, queueDepth)}
}

type denialSignal struct {
	spanName   string
	agentKind  string
	namespace  string
	name       string
	uid        string
	sandboxID  string
	dstName    string
	dstAddr    string
	denyReason string
	failReason string
}

// ObserveTraces scans spans for egress denials/failures and enqueues them. It
// never blocks: a full queue drops and counts.
func (b *EgressSecurityBridge) ObserveTraces(rs []*tracepb.ResourceSpans) {
	if b == nil {
		return
	}
	for _, r := range rs {
		for _, ss := range r.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				name := sp.GetName()
				if name != spanEgressDenied && name != spanEgressFailed {
					continue
				}
				sig := signalFromSpan(name, sp.GetAttributes())
				// Unattributable signals (e.g. a denied orphan with no parsed
				// identity, or malformed frames) carry no agent to point the
				// Event at -- skip rather than emit an orphaned Warning.
				if sig.namespace == "" || sig.name == "" {
					continue
				}
				select {
				case b.ch <- sig:
				default:
					b.dropped.Add(1)
				}
			}
		}
	}
}

// Start drains the queue and records Events until ctx is cancelled. Implements
// manager.Runnable.
func (b *EgressSecurityBridge) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("notify-egress")
	for {
		select {
		case <-ctx.Done():
			if d := b.dropped.Load(); d > 0 {
				logger.Info("Egress security bridge dropped signals under load", "dropped", d)
			}
			return nil
		case sig := <-b.ch:
			b.record(sig)
		}
	}
}

// NeedLeaderElection returns false so the drain runs on EVERY replica, not just
// the leader. The queue is fed by ObserveTraces from the OTLP receiver, which
// runs on every replica; workers load-balance their OTLP export across replicas,
// so each denial span lands on exactly one replica and must be drained THERE.
// A leader-only drain would strand (and eventually drop) every denial that the
// non-leader replicas received -- an in-memory queue can only be drained in the
// same process that fills it. The recorder is likewise per-replica, so each
// replica records its own intake with no cross-replica duplication of a denial.
func (b *EgressSecurityBridge) NeedLeaderElection() bool { return false }

func (b *EgressSecurityBridge) record(sig denialSignal) {
	regarding := regardingAgent(sig.agentKind, sig.namespace, sig.name, sig.uid)
	if regarding == nil {
		return
	}
	dst := sig.dstName
	if dst == "" {
		dst = sig.dstAddr
	}
	switch {
	case sig.spanName == spanEgressFailed:
		b.rec.Eventf(regarding, nil, TypeWarning, ReasonEgressUpstreamFailed, ActionDial,
			"Egress to %s failed after admission: %s", dst, sig.failReason)
	case sig.denyReason == string(otelemit.DenyReasonOrphanSandbox):
		b.rec.Eventf(regarding, nil, TypeWarning, ReasonOrphanSandbox, ActionDeny,
			"Egress denied for orphan sandbox %s to %s", sig.sandboxID, dst)
	default:
		b.rec.Eventf(regarding, nil, TypeWarning, ReasonEgressDenied, ActionDeny,
			"Egress to %s denied: %s", dst, sig.denyReason)
	}
}

func signalFromSpan(spanName string, attrs []*commonpb.KeyValue) denialSignal {
	sig := denialSignal{spanName: spanName}
	for _, kv := range attrs {
		v := otelemit.AnyValueString(kv.GetValue())
		switch kv.GetKey() {
		case otelemit.AttrAgentKind:
			sig.agentKind = v
		case otelemit.AttrAgentNamespace:
			sig.namespace = v
		case otelemit.AttrAgentName:
			sig.name = v
		case otelemit.AttrAgentUID:
			sig.uid = v
		case otelemit.AttrSandboxID:
			sig.sandboxID = v
		case otelemit.AttrL4DstName:
			sig.dstName = v
		case otelemit.AttrEgressDenyReason:
			sig.denyReason = v
		case otelemit.AttrEgressFailureReason:
			sig.failReason = v
		case "network.peer.address": // semconv.NetworkPeerAddress
			sig.dstAddr = v
		}
	}
	return sig
}

// regardingAgent builds the typed parent-agent object the denial Event points
// at. The TypeMeta is set explicitly so the events recorder's reference
// resolution uses the v1alpha1 GVK directly rather than scheme.ObjectKinds
// (which also returns the __internal alias for clrk storage-version types).
func regardingAgent(kind, namespace, name, uid string) runtime.Object {
	om := metav1.ObjectMeta{Namespace: namespace, Name: name, UID: types.UID(uid)}
	switch kind {
	case clrkv1alpha1.AgentKindTask:
		return &clrkv1alpha1.TaskAgent{
			TypeMeta:   metav1.TypeMeta{Kind: "TaskAgent", APIVersion: clrkv1alpha1.SchemeGroupVersion.String()},
			ObjectMeta: om,
		}
	case clrkv1alpha1.AgentKindDaemon:
		return &clrkv1alpha1.DaemonAgent{
			TypeMeta:   metav1.TypeMeta{Kind: "DaemonAgent", APIVersion: clrkv1alpha1.SchemeGroupVersion.String()},
			ObjectMeta: om,
		}
	default:
		return nil
	}
}
