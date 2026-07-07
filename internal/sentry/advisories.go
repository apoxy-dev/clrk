package sentry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	eventsv1client "k8s.io/client-go/kubernetes/typed/events/v1"

	"github.com/apoxy-dev/clrk/internal/notify"
)

const (
	// AdvisoryURLAnnotation / AdvisorySeverityAnnotation / AdvisoryIssuedAtAnnotation
	// carry advisory metadata that events.k8s.io/v1 has no dedicated field for.
	AdvisoryURLAnnotation      = "clrk.apoxy.dev/advisory-url"
	AdvisorySeverityAnnotation = "clrk.apoxy.dev/advisory-severity"
	AdvisoryIssuedAtAnnotation = "clrk.apoxy.dev/advisory-issued-at"

	// advisoryNoteMax bounds the Event note length.
	advisoryNoteMax = 1024
)

// AdvisoryPoller pulls security advisories from api.apoxy.dev and materializes
// each as an events.k8s.io/v1 Event (reason=SecurityAdvisory) in the embedded
// apiserver, so advisories land in the same Notification Center as everything
// else. It is a manager.Runnable. Idempotent: the Event name is derived from the
// advisory id, so re-polling AlreadyExists-es instead of duplicating.
//
// It fetches the full active advisory set every cycle rather than tracking a
// "since" watermark. A watermark is unsafe here because the notify.Pruner
// deletes aged Events (including advisories): once the watermark passed an
// advisory's issue time, a since-filtered fetch would never return it again, so
// a pruned-but-still-active advisory would silently vanish. Full-set + idempotent
// create is self-healing -- a pruned active advisory reappears next cycle, and a
// withdrawn one (no longer returned by the server) correctly ages out for good.
type AdvisoryPoller struct {
	Client    *Client
	Events    eventsv1client.EventsV1Interface
	Auth      AuthFunc
	Namespace string        // clrk system namespace advisory Events are written to.
	Interval  time.Duration // <=0 => DefaultAdvisoryPoll. The fallback cadence when the server does not specify one.
}

// NewAdvisoryPoller builds a poller. Returns nil when any dependency is missing.
func NewAdvisoryPoller(client *Client, events eventsv1client.EventsV1Interface, auth AuthFunc, namespace string, interval time.Duration) *AdvisoryPoller {
	if client == nil || events == nil || auth == nil || namespace == "" {
		return nil
	}
	if interval <= 0 {
		interval = DefaultAdvisoryPoll
	}
	return &AdvisoryPoller{
		Client:    client,
		Events:    events,
		Auth:      auth,
		Namespace: namespace,
		Interval:  interval,
	}
}

// NeedLeaderElection keeps a single poller cluster-wide.
func (p *AdvisoryPoller) NeedLeaderElection() bool { return true }

// Start implements manager.Runnable: poll on a ticker until ctx is done. Polls
// once promptly on startup so a freshly-registered deployment sees advisories
// without waiting a full interval. The ticker adopts the server-driven cadence
// (CLRKConfig.status.notifications, surfaced via AuthState) as it changes, so
// api.apoxy.dev can speed up or slow down advisory polling.
func (p *AdvisoryPoller) Start(ctx context.Context) error {
	interval := p.pollOnce(ctx)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if next := p.pollOnce(ctx); next != interval {
				interval = next
				t.Reset(interval)
			}
		}
	}
}

// pollOnce fetches and materializes the current advisory set, returning the
// effective poll cadence to run the next tick at.
func (p *AdvisoryPoller) pollOnce(ctx context.Context) time.Duration {
	auth, err := p.Auth(ctx)
	if err != nil {
		slog.Debug("phone-home: auth unavailable; skipping advisory poll", "err", err)
		return p.resolveInterval(AuthState{})
	}
	if !auth.Authorized() || !auth.AdviseEnabled {
		return p.resolveInterval(auth)
	}
	// Full active set (no since-watermark) so pruned-but-active advisories
	// re-materialize; the deterministic Event name makes re-creates idempotent.
	advisories, err := p.Client.FetchAdvisories(ctx, auth.Token)
	if err != nil {
		slog.Warn("phone-home: advisory fetch failed", "err", err)
		return p.resolveInterval(auth)
	}
	for i := range advisories {
		if err := p.materialize(ctx, &advisories[i]); err != nil {
			slog.Warn("phone-home: advisory materialize failed", "id", advisories[i].ID, "err", err)
		}
	}
	return p.resolveInterval(auth)
}

// resolveInterval prefers the server-driven cadence from AuthState, falling back
// to the poller's configured default. It is always floored to a positive value:
// Start feeds the result straight into time.NewTicker/Reset, which panic on a
// non-positive interval, and p.Interval can be zero when the poller struct is
// built directly rather than via NewAdvisoryPoller.
func (p *AdvisoryPoller) resolveInterval(auth AuthState) time.Duration {
	iv := p.Interval
	if auth.AdvisoryPollInterval > 0 {
		iv = auth.AdvisoryPollInterval
	}
	if iv <= 0 {
		iv = DefaultAdvisoryPoll
	}
	return iv
}

// materialize creates one advisory Event, treating AlreadyExists as success.
func (p *AdvisoryPoller) materialize(ctx context.Context, a *Advisory) error {
	note := a.Title
	if a.Summary != "" {
		note += ": " + a.Summary
	}
	note = truncateUTF8(note, advisoryNoteMax)
	ann := map[string]string{}
	if a.URL != "" {
		ann[AdvisoryURLAnnotation] = a.URL
	}
	if a.Severity != "" {
		ann[AdvisorySeverityAnnotation] = a.Severity
	}
	// EventTime is when the advisory was ISSUED, not when we first polled it, so
	// the Notification Center (which sorts/labels by EventTime) shows the real
	// issue time. Fall back to now only if the server sent no/invalid timestamp.
	eventTime := metav1.NowMicro()
	if a.IssuedAt != "" {
		if t, perr := time.Parse(time.RFC3339, a.IssuedAt); perr == nil {
			eventTime = metav1.NewMicroTime(t)
			ann[AdvisoryIssuedAtAnnotation] = a.IssuedAt
		}
	}
	ev := &eventsv1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:        advisoryEventName(a.ID),
			Namespace:   p.Namespace,
			Annotations: ann,
		},
		EventTime:           eventTime,
		ReportingController: notify.ReportingController,
		ReportingInstance:   "advisory-poller",
		Action:              notify.ActionAdvise,
		Reason:              notify.ReasonSecurityAdvisory,
		Type:                notify.TypeWarning,
		Note:                note,
		// Advisories are not about a specific workload; anchor them to the
		// CLRKConfig singleton so the required Regarding ref is satisfied and the
		// console can link back to the notifications settings.
		Regarding: corev1.ObjectReference{
			APIVersion: "clrk.apoxy.dev/v1alpha1",
			Kind:       "CLRKConfig",
			Namespace:  p.Namespace,
			Name:       "default",
		},
	}
	_, err := p.Events.Events(p.Namespace).Create(ctx, ev, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

// advisoryEventName derives a deterministic, DNS-1123-safe Event name from an
// advisory id (hashed so arbitrary ids always yield a valid, stable name).
func advisoryEventName(id string) string {
	sum := sha256.Sum256([]byte(id))
	return "advisory-" + hex.EncodeToString(sum[:])[:32]
}

// truncateUTF8 caps s to at most max bytes without splitting a multibyte rune.
// A raw s[:max] can bisect a UTF-8 sequence, and the apiserver stores Events as
// protobuf whose string fields reject invalid UTF-8, so an advisory whose note
// straddled the cut would fail to materialize on every poll. Backing off to the
// preceding rune boundary keeps the Note valid.
func truncateUTF8(s string, max int) string {
	if len(s) <= max {
		return s
	}
	for max > 0 && !utf8.RuneStart(s[max]) {
		max--
	}
	return s[:max]
}
