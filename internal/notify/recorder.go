package notify

import (
	"context"
	"fmt"

	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	eventsv1client "k8s.io/client-go/kubernetes/typed/events/v1"
	"k8s.io/client-go/tools/events"
)

// ReportingController is the reportingController stamped on every Event clrk
// records; it doubles as the broadcaster's recorder name.
const ReportingController = "clrk.apoxy.dev/controller-manager"

// Recorder records events.k8s.io/v1 Events into the embedded apiserver through a
// client-go events broadcaster. The broadcaster aggregates repeats into
// series.count, so a looping agent hammering one blocked endpoint yields a
// single rising-count Warning rather than thousands of rows. A nil *Recorder is
// a safe no-op (tests / builds without an apiserver), and every emit site holds
// the recorder as a *Recorder so it can pass nil.
type Recorder struct {
	rec events.EventRecorderLogger
}

// NewRecorder starts a broadcaster recording to the embedded apiserver's
// events.k8s.io/v1 surface. ev must be the loopback events client (never the
// customer cluster's -- an APIService can't override the built-in Events group).
// scheme must know the regarding object kinds (TaskAgent/DaemonAgent/WorkerPool/
// AgentSandboxRevision) so ObjectReferences resolve; pass apiserver.Scheme. The
// broadcaster runs until ctx is cancelled.
func NewRecorder(ctx context.Context, ev eventsv1client.EventsV1Interface, scheme *runtime.Scheme) (*Recorder, error) {
	bc := events.NewBroadcaster(&mergePatchEventSink{inner: ev})
	if err := bc.StartRecordingToSinkWithContext(ctx); err != nil {
		return nil, fmt.Errorf("starting event broadcaster: %w", err)
	}
	return &Recorder{rec: bc.NewRecorder(scheme, ReportingController)}, nil
}

// Eventf records one Event. regarding is the object the notification is about
// (its GVK resolves through the recorder's scheme); related is optional. The
// note may be a format string with args. Nil-safe on the receiver.
func (r *Recorder) Eventf(regarding, related runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
	if r == nil || r.rec == nil {
		return
	}
	r.rec.Eventf(regarding, related, eventtype, reason, action, note, args...)
}

// mergePatchEventSink is the client-go EventSink pointed at the embedded
// apiserver, with one change from events.EventSinkImpl: aggregation Patches are
// sent as application/merge-patch+json (types.MergePatchType) instead of
// StrategicMergePatch. The builder-registered generic store that serves Events
// is a CRD-style store and does not advertise strategic-merge-patch, so a
// StrategicMergePatch aggregation update would be rejected. Event aggregation
// only touches scalar/struct fields (series.count, series.lastObservedTime) with
// no list-merge directives, so the broadcaster's two-way merge patch bytes are
// byte-identical to a JSON merge patch -- re-labeling the content type is safe
// and lossless. Without this, aggregation would fail and each occurrence would
// Create a fresh singleton (still correct, just noisier).
type mergePatchEventSink struct {
	inner eventsv1client.EventsV1Interface
}

var _ events.EventSink = (*mergePatchEventSink)(nil)

func (s *mergePatchEventSink) Create(ctx context.Context, event *eventsv1.Event) (*eventsv1.Event, error) {
	if event.Namespace == "" {
		return nil, fmt.Errorf("cannot create an Event with an empty namespace")
	}
	return s.inner.Events(event.Namespace).Create(ctx, event, metav1.CreateOptions{})
}

func (s *mergePatchEventSink) Update(ctx context.Context, event *eventsv1.Event) (*eventsv1.Event, error) {
	if event.Namespace == "" {
		return nil, fmt.Errorf("cannot update an Event with an empty namespace")
	}
	return s.inner.Events(event.Namespace).Update(ctx, event, metav1.UpdateOptions{})
}

func (s *mergePatchEventSink) Patch(ctx context.Context, event *eventsv1.Event, data []byte) (*eventsv1.Event, error) {
	if event.Namespace == "" {
		return nil, fmt.Errorf("cannot patch an Event with an empty namespace")
	}
	return s.inner.Events(event.Namespace).Patch(ctx, event.Name, types.MergePatchType, data, metav1.PatchOptions{})
}
