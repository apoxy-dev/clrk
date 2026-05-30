package invocation

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/nats-io/nats.go/jetstream"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/endpoints/request"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/invevent"
)

// testWriteCtxKey marks a request that carried the test-write header.
// The apiserver handler chain (manager.go) stamps it; Storage's write
// path checks it via testWriteRequested.
type testWriteCtxKey struct{}

// WithTestWrite returns ctx marked as a permitted test-write request.
// Called by the header-chain middleware when the gate header is present.
func WithTestWrite(ctx context.Context) context.Context {
	return context.WithValue(ctx, testWriteCtxKey{}, true)
}

func testWriteRequested(ctx context.Context) bool {
	v, _ := ctx.Value(testWriteCtxKey{}).(bool)
	return v
}

// watchBuffer bounds the watch result channel. When full, the single
// producer goroutine blocks on the send, which backpressures the
// JetStream iterator — JetStream won't deliver faster than the
// apiserver drains.
const watchBuffer = 64

// Watch tails the JetStream INVOCATIONS stream via an ephemeral ordered
// consumer scoped to this GVR's subject filter, translating each event
// into a watch.Event whose object carries the stream sequence as its
// resourceVersion. resourceVersion mapping:
//
//   - "" (unset)  -> deliver new (from now)
//   - "0"         -> deliver all retained
//   - "<seq>"     -> deliver from seq+1 (the List->Watch handoff)
//
// When NATS is not wired the Watch degrades to an immediately-closed
// watch so kubectl --watch exits cleanly and informers don't depend on
// it.
// busJS returns the JetStream handle if the Bus is wired and resolved,
// else nil (so Watch degrades to an empty watch).
func (s *Storage) busJS() jetstream.JetStream {
	if s.deps.Bus == nil {
		return nil
	}
	return s.deps.Bus.JS()
}

// streamFirstSeq returns the lowest stream sequence still retained by
// the INVOCATIONS stream (events below it have aged out via MaxAge).
func streamFirstSeq(ctx context.Context, js jetstream.JetStream) (uint64, error) {
	st, err := js.Stream(ctx, invevent.StreamName)
	if err != nil {
		return 0, err
	}
	info, err := st.Info(ctx)
	if err != nil {
		return 0, err
	}
	return info.State.FirstSeq, nil
}

func (s *Storage) Watch(ctx context.Context, opts *internalversion.ListOptions) (watch.Interface, error) {
	js := s.busJS()
	if js == nil {
		return watch.NewEmptyWatch(), nil
	}

	ns, _ := request.NamespaceFrom(ctx)
	parent := ""
	if s.parentKind != "" {
		if info, ok := request.RequestInfoFrom(ctx); ok {
			parent = info.Name
		}
	}

	cfg := jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{invevent.FilterSubject(ns, s.parentKind, parent)},
	}
	switch {
	case opts == nil || opts.ResourceVersion == "":
		cfg.DeliverPolicy = jetstream.DeliverNewPolicy
	case opts.ResourceVersion == "0":
		cfg.DeliverPolicy = jetstream.DeliverAllPolicy
	default:
		rv, err := strconv.ParseUint(opts.ResourceVersion, 10, 64)
		if err != nil {
			return nil, apierrors.NewBadRequest(fmt.Sprintf("invalid resourceVersion %q", opts.ResourceVersion))
		}
		// 410 Gone if the requested resume point has aged out of the
		// stream (MaxAge eviction). An ordered consumer started below
		// FirstSeq silently begins at the oldest surviving message,
		// dropping events between rv+1 and FirstSeq; the k8s watch
		// contract requires StatusReasonExpired here so informers re-List
		// rather than believe they have unbroken continuity.
		if first, ferr := streamFirstSeq(ctx, js); ferr == nil && first > 0 && rv+1 < first {
			return nil, apierrors.NewResourceExpired(fmt.Sprintf("resourceVersion %d is too old (stream starts at %d)", rv, first-1))
		}
		cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		cfg.OptStartSeq = rv + 1
	}

	cons, err := js.OrderedConsumer(ctx, invevent.StreamName, cfg)
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("watch consumer: %w", err))
	}
	iter, err := cons.Messages()
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("watch messages: %w", err))
	}

	w := &jsWatcher{
		result:     make(chan watch.Event, watchBuffer),
		done:       make(chan struct{}),
		iter:       iter,
		namespace:  ns,
		parentKind: s.parentKind,
		parentName: parent,
	}
	go w.run()
	// Tie the watch to the request context: a client disconnect stops it.
	go func() {
		select {
		case <-ctx.Done():
			w.Stop()
		case <-w.done:
		}
	}()
	return w, nil
}

// jsWatcher adapts a JetStream message iterator to watch.Interface. A
// single goroutine (run) owns the result channel — it is the only
// sender and the only closer, so Stop never races a send onto a closed
// channel.
type jsWatcher struct {
	result chan watch.Event
	done   chan struct{}
	iter   jetstream.MessagesContext

	stopOnce sync.Once

	namespace  string
	parentKind clrkv1alpha1.InvocationParentKind
	parentName string
}

func (w *jsWatcher) ResultChan() <-chan watch.Event { return w.result }

func (w *jsWatcher) Stop() {
	w.stopOnce.Do(func() {
		close(w.done)
		w.iter.Stop() // unblocks iter.Next with ErrMsgIteratorClosed
	})
}

func (w *jsWatcher) run() {
	defer close(w.result)
	for {
		msg, err := w.iter.Next()
		if err != nil {
			// Stopped/drained, or a fatal iterator error — either way the
			// watch is over.
			return
		}
		ev, ok := w.event(msg)
		if !ok {
			continue
		}
		select {
		case w.result <- ev:
		case <-w.done:
			return
		}
	}
}

// event decodes a message into a watch.Event, post-filtering on the
// decoded object's real namespace/parent (the subject token is lossy)
// and classifying Added vs Modified by the birth-event header. Returns
// ok=false for messages to skip.
func (w *jsWatcher) event(msg jetstream.Msg) (watch.Event, bool) {
	var inv clrkv1alpha1.Invocation
	if err := json.Unmarshal(msg.Data(), &inv); err != nil {
		return watch.Event{}, false
	}
	if w.namespace != "" && inv.Namespace != w.namespace {
		return watch.Event{}, false
	}
	if w.parentKind != "" {
		if inv.Spec.ParentRef.Kind != w.parentKind {
			return watch.Event{}, false
		}
		if w.parentName != "" && inv.Spec.ParentRef.Name != w.parentName {
			return watch.Event{}, false
		}
	}
	if meta, err := msg.Metadata(); err == nil {
		inv.ResourceVersion = strconv.FormatUint(meta.Sequence.Stream, 10)
	}
	evType := watch.Modified
	if msg.Headers().Get(invevent.HeaderFirst) == "true" {
		evType = watch.Added
	}
	return watch.Event{Type: evType, Object: &inv}, true
}
