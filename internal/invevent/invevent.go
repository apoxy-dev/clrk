// Package invevent is the shared JetStream pub/sub layer for Invocation
// lifecycle events. It is intentionally dependency-light — the nats.go
// client + jetstream plus the clrk API types, with no embedded
// nats-server and no ClickHouse — so every producer and consumer can
// import it without bloat: the cm-side apiserver storage + consumer,
// the ingress ext_proc publisher (APO-617), and the worker dispatcher
// (APO-618, a separate pod connecting over TCP).
//
// Wire contract: each event is the COMPLETE Invocation JSON snapshot at
// the moment of a lifecycle transition, published to a per-invocation
// subject. The JetStream stream sequence assigned on publish is the
// apiserver resourceVersion. Because the payload is a full object (not
// a delta), the Watch is stateless (decode + stamp RV) and the
// JetStream->ClickHouse consumer extracts its columns from the same
// snapshot.
package invevent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

const (
	// SubjectPrefix roots every Invocation event subject. The full
	// shape is clrk.invocations.<ns>.<kind>.<agent>.<id>; the hierarchy
	// lets per-parent and namespaced consumers filter by subject with
	// no server-side work.
	SubjectPrefix = "clrk.invocations"

	// StreamName is the single global JetStream stream carrying every
	// Invocation lifecycle event. Defined here (the wire-contract leaf)
	// rather than in internal/nats so the materializing consumer and
	// the worker publisher can name it without importing the embedded
	// nats-server.
	StreamName = "INVOCATIONS"

	// StreamWildcard is the subject space the stream binds.
	StreamWildcard = SubjectPrefix + ".>"

	// HeaderFirst marks the birth event of an invocation (its first
	// lifecycle transition). The Watch maps a set HeaderFirst to a
	// Kubernetes Added event and everything else to Modified.
	HeaderFirst = "Clrk-First"

	// CMNATSAddrEnv names the env var carrying the controller-manager's
	// NATS/JetStream client address (host:port). The WorkerPool
	// controller stamps it onto worker pods so the dispatcher's
	// invocation publisher can dial the cm over TCP. Empty disables
	// worker-side publishing (the worker still runs; lifecycle recording
	// for worker-emitted phases is simply unavailable). Mirrors the OTLP
	// endpoint env (otelemit.CMOTLPEndpointEnv) so the two cm-target
	// addresses are wired the same way.
	CMNATSAddrEnv = "CLRK_CM_NATS_ADDR"
)

// tok sanitises a subject token. NATS reserves '.', '*', '>' and
// whitespace; Kubernetes object names are normally dot-free DNS labels
// but CRD names can technically be DNS subdomains (with dots), so we
// fold any reserved/odd byte to '_'. Sanitisation is lossy and can
// collide, so consumers/watchers that scope by subject MUST re-check
// the real namespace/parent fields from the decoded object — the
// subject is a coarse routing filter, never parsed back into identity.
func tok(s string) string {
	if s == "" {
		return "_"
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, s)
}

// Subject returns the per-event subject for one invocation.
func Subject(namespace string, kind clrkv1alpha1.InvocationParentKind, agent, id string) string {
	return fmt.Sprintf("%s.%s.%s.%s.%s",
		SubjectPrefix, tok(namespace), tok(string(kind)), tok(agent), tok(id))
}

// FilterSubject returns the subject filter for a consumer or watch
// scoped to a namespace and optional parent:
//
//   - cluster-wide (namespace ""):                 clrk.invocations.>
//   - namespaced top-level (kind ""):              clrk.invocations.<ns>.>
//   - per-parent subresource (kind+parent set):    clrk.invocations.<ns>.<kind>.<parent>.>
//
// Because tok is lossy, the caller still post-filters on the decoded
// object's real namespace/parent.
func FilterSubject(namespace string, kind clrkv1alpha1.InvocationParentKind, parent string) string {
	if namespace == "" {
		return SubjectPrefix + ".>"
	}
	if kind == "" {
		return fmt.Sprintf("%s.%s.>", SubjectPrefix, tok(namespace))
	}
	if parent == "" {
		return fmt.Sprintf("%s.%s.%s.>", SubjectPrefix, tok(namespace), tok(string(kind)))
	}
	return fmt.Sprintf("%s.%s.%s.%s.>", SubjectPrefix, tok(namespace), tok(string(kind)), tok(parent))
}

// Publisher publishes Invocation lifecycle events to JetStream. It is
// transport-agnostic: cm-local producers pass a JetStream over an
// in-process connection, the worker passes one over a TCP connection.
type Publisher struct {
	js jetstream.JetStream
}

// NewPublisher wraps a JetStream context.
func NewPublisher(js jetstream.JetStream) *Publisher { return &Publisher{js: js} }

// Publish writes the complete Invocation snapshot as one event and
// returns the assigned JetStream stream sequence (the resourceVersion
// the apiserver will surface for this version of the object). first
// marks the invocation's birth event. A Nats-Msg-Id of
// "<id>.<phase>" makes a retried publish of the same transition
// idempotent within the stream's duplicate window — harmless if it
// slips past the window, since the read-side argMax and a stateless
// Watch both tolerate a duplicate same-phase row.
func (p *Publisher) Publish(ctx context.Context, inv *clrkv1alpha1.Invocation, first bool) (uint64, error) {
	data, err := json.Marshal(inv)
	if err != nil {
		return 0, fmt.Errorf("marshal invocation: %w", err)
	}
	msg := &nats.Msg{
		Subject: Subject(inv.Namespace, inv.Spec.ParentRef.Kind, inv.Spec.ParentRef.Name, inv.Name),
		Data:    data,
		Header:  nats.Header{},
	}
	msg.Header.Set(jetstream.MsgIDHeader, inv.Name+"."+string(inv.Status.Phase))
	if first {
		msg.Header.Set(HeaderFirst, "true")
	}
	ack, err := p.js.PublishMsg(ctx, msg)
	if err != nil {
		return 0, fmt.Errorf("publish invocation event: %w", err)
	}
	return ack.Sequence, nil
}
