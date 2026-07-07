package notify

import (
	"context"
	"time"

	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	eventsv1client "k8s.io/client-go/kubernetes/typed/events/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// DefaultEventRetention is the fallback Event TTL when none is configured. It
// matches the OTLP / NATS max-age so notifications and their underlying
// telemetry age out together.
const DefaultEventRetention = 72 * time.Hour

// defaultPruneInterval is how often the pruner sweeps when Interval is unset.
const defaultPruneInterval = 10 * time.Minute

// Pruner deletes events.k8s.io/v1 Events older than a retention window from the
// embedded apiserver. The builder-registered generic store over kine has no
// event TTL (unlike kube-apiserver's dedicated, TTL-backed Event storage), so
// without this Events accumulate in SQLite indefinitely and inflate the watch
// cache. It implements manager.Runnable.
type Pruner struct {
	// Events is the embedded apiserver's events client (loopback).
	Events eventsv1client.EventsV1Interface
	// Interval is how often to sweep; defaults to defaultPruneInterval.
	Interval time.Duration
	// Retention returns the maximum Event age to keep. It is read once per
	// sweep so a live CLRKConfig.spec.notifications.eventRetention change takes
	// effect without a restart. A non-positive return disables pruning for that
	// sweep. Must be non-nil.
	Retention func() time.Duration
}

// Start runs the sweep loop until ctx is cancelled.
func (p *Pruner) Start(ctx context.Context) error {
	interval := p.Interval
	if interval <= 0 {
		interval = defaultPruneInterval
	}
	logger := log.FromContext(ctx).WithName("notify-pruner")
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if n, err := p.sweep(ctx); err != nil {
				logger.Error(err, "Event prune sweep failed")
			} else if n > 0 {
				logger.Info("Pruned expired Events", "count", n)
			}
		}
	}
}

// NeedLeaderElection ensures only the elected manager prunes.
func (p *Pruner) NeedLeaderElection() bool { return true }

func (p *Pruner) sweep(ctx context.Context) (int, error) {
	retention := p.Retention()
	if retention <= 0 {
		return 0, nil
	}
	cutoff := time.Now().Add(-retention)
	list, err := p.Events.Events(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, err
	}
	deleted := 0
	for i := range list.Items {
		e := &list.Items[i]
		if lastObserved(e).After(cutoff) {
			continue
		}
		if err := p.Events.Events(e.Namespace).Delete(ctx, e.Name, metav1.DeleteOptions{}); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

// lastObserved returns the most recent time an Event was seen, preferring the
// aggregation series' lastObservedTime and falling back through the modern and
// deprecated timestamp fields to creationTimestamp.
func lastObserved(e *eventsv1.Event) time.Time {
	if e.Series != nil && !e.Series.LastObservedTime.IsZero() {
		return e.Series.LastObservedTime.Time
	}
	if !e.EventTime.IsZero() {
		return e.EventTime.Time
	}
	if !e.DeprecatedLastTimestamp.IsZero() {
		return e.DeprecatedLastTimestamp.Time
	}
	return e.CreationTimestamp.Time
}
