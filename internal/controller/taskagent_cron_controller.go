package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// TaskAgentInvoker fires one execution of ta. Implementations route to the
// same path an HTTP-triggered TaskAgent would take. body is the verbatim
// scheduleInput (default empty JSON object when nil).
type TaskAgentInvoker interface {
	Invoke(ctx context.Context, ta *clrkv1alpha1.TaskAgent, body []byte) error
}

const (
	// tickInterval is the cron sweep cadence. Cron expressions are
	// minute-resolution so 1 Hz is well under user-expressible granularity.
	tickInterval = time.Second
	// invokeTimeoutCap upper-bounds the per-fire HTTP call so a wedged
	// backend can't pin a fire goroutine forever. spec.timeoutSeconds caps
	// from below.
	invokeTimeoutCap = 5 * time.Minute
)

type cronEntry struct {
	specSchedule string
	schedule     cron.Schedule
	nextFire     time.Time
	lastFire     time.Time
}

// TaskAgentCronReconciler watches TaskAgents with a non-empty spec.schedule
// and fires them on a cron clock via Invoker. The fire loop runs as a
// separate Runnable that opts into leader election.
type TaskAgentCronReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	Invoker TaskAgentInvoker
	// Now is overridable so tests can advance the clock without sleeping.
	Now func() time.Time

	mu      sync.Mutex
	entries map[types.NamespacedName]*cronEntry
}

// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=taskagents,verbs=get;list;watch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=taskagents/status,verbs=get;update;patch

func (r *TaskAgentCronReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var ta clrkv1alpha1.TaskAgent
	if err := r.Get(ctx, req.NamespacedName, &ta); err != nil {
		if apierrors.IsNotFound(err) {
			r.dropEntry(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if ta.Spec.Schedule == nil || *ta.Spec.Schedule == "" {
		r.dropEntry(req.NamespacedName)
		return ctrl.Result{}, r.patchScheduled(ctx, &ta, metav1.ConditionFalse, reasonNotScheduled, "spec.schedule is empty")
	}

	specSched := *ta.Spec.Schedule
	if r.entryMatches(req.NamespacedName, specSched) {
		// No spec change — skip the parse and reuse the existing entry's
		// timing. Status patch still runs to refresh ObservedGeneration.
		return ctrl.Result{}, r.patchScheduled(ctx, &ta, metav1.ConditionTrue, reasonScheduleRegistered,
			fmt.Sprintf("Cron entry registered for %q", specSched))
	}

	sched, err := cron.ParseStandard(specSched)
	if err != nil {
		r.dropEntry(req.NamespacedName)
		// Admission already rejects bad expressions; defense in depth.
		logger.Error(err, "Schedule failed to parse despite admission validation", "schedule", specSched)
		return ctrl.Result{}, r.patchScheduled(ctx, &ta, metav1.ConditionFalse, reasonParseError, err.Error())
	}

	r.upsertEntry(req.NamespacedName, specSched, sched)
	return ctrl.Result{}, r.patchScheduled(ctx, &ta, metav1.ConditionTrue, reasonScheduleRegistered,
		fmt.Sprintf("Cron entry registered for %q", specSched))
}

func (r *TaskAgentCronReconciler) entryMatches(key types.NamespacedName, specSched string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[key]
	return ok && e.specSchedule == specSched
}

func (r *TaskAgentCronReconciler) upsertEntry(key types.NamespacedName, specSched string, sched cron.Schedule) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries = map[types.NamespacedName]*cronEntry{}
	}
	r.entries[key] = &cronEntry{
		specSchedule: specSched,
		schedule:     sched,
		nextFire:     sched.Next(r.now()),
	}
}

func (r *TaskAgentCronReconciler) dropEntry(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, key)
}

func (r *TaskAgentCronReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// timing returns the entry's last/next fire times for status projection.
// Returns (nil, nil) when no entry exists. Caller writes the values into
// status; this helper only reads.
func (r *TaskAgentCronReconciler) timing(key types.NamespacedName) (last, next *metav1.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[key]
	if !ok {
		return nil, nil
	}
	if !e.lastFire.IsZero() {
		t := metav1.NewTime(e.lastFire)
		last = &t
	}
	if !e.nextFire.IsZero() {
		t := metav1.NewTime(e.nextFire)
		next = &t
	}
	return last, next
}

// patchScheduled sets the Scheduled condition + LastScheduleTime/
// NextScheduleTime on ta and patches the status subresource. The cron
// reconciler owns this disjoint slice of TaskAgentStatus; other
// reconcilers (revision, ingress) own the rest.
func (r *TaskAgentCronReconciler) patchScheduled(
	ctx context.Context,
	ta *clrkv1alpha1.TaskAgent,
	status metav1.ConditionStatus,
	reason, message string,
) error {
	base := ta.DeepCopy()
	meta.SetStatusCondition(&ta.Status.Conditions, metav1.Condition{
		Type:               condScheduled,
		Status:             status,
		ObservedGeneration: ta.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	})
	last, next := r.timing(types.NamespacedName{Namespace: ta.Namespace, Name: ta.Name})
	ta.Status.LastScheduleTime = last
	ta.Status.NextScheduleTime = next
	if err := r.Status().Patch(ctx, ta, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("patching Scheduled condition: %w", err)
	}
	return nil
}

// SetupWithManager registers the reconciler and the tick runnable on mgr.
func (r *TaskAgentCronReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Now == nil {
		r.Now = time.Now
	}
	if r.Invoker == nil {
		return fmt.Errorf("TaskAgentCronReconciler.Invoker is required")
	}
	if err := mgr.Add(&cronTicker{r: r, interval: tickInterval}); err != nil {
		return fmt.Errorf("registering cron ticker: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("taskagent-cron").
		For(&clrkv1alpha1.TaskAgent{}).
		Complete(r)
}

// cronTicker drives the fire loop on a fixed interval. Registered as a
// Runnable that opts into leader election so only the leader fires.
type cronTicker struct {
	r        *TaskAgentCronReconciler
	interval time.Duration
}

func (t *cronTicker) NeedLeaderElection() bool { return true }

func (t *cronTicker) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("taskagent-cron-ticker")
	logger.Info("Starting cron ticker", "interval", t.interval)
	tk := time.NewTicker(t.interval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Info("Cron ticker stopped")
			return nil
		case <-tk.C:
			t.r.Tick(ctx)
		}
	}
}

// Tick walks all registered entries and fires those whose nextFire time
// has passed. Fires within a single tick run in parallel goroutines so
// one slow Invoker can't starve sibling agents; Tick returns once all
// in-flight fires complete (so the next ticker tick is paced to the
// slowest fire — good enough for minute-resolution cron).
//
// Exported because tests drive it directly to avoid sleeping.
func (r *TaskAgentCronReconciler) Tick(ctx context.Context) {
	now := r.now()
	type due struct {
		key      types.NamespacedName
		fireTime time.Time
	}
	var ready []due
	r.mu.Lock()
	for key, e := range r.entries {
		if !e.nextFire.IsZero() && !e.nextFire.After(now) {
			ready = append(ready, due{key: key, fireTime: e.nextFire})
			// Roll forward immediately so a slow Invoker can't double-fire on the next tick.
			e.nextFire = e.schedule.Next(now)
		}
	}
	r.mu.Unlock()

	if len(ready) == 0 {
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(ready))
	for _, d := range ready {
		go func(d due) {
			defer wg.Done()
			r.fire(ctx, d.key, d.fireTime)
		}(d)
	}
	wg.Wait()
}

func (r *TaskAgentCronReconciler) fire(parent context.Context, key types.NamespacedName, fireTime time.Time) {
	logger := log.FromContext(parent).WithValues("taskagent", key, "fireTime", fireTime)

	var ta clrkv1alpha1.TaskAgent
	if err := r.Get(parent, key, &ta); err != nil {
		logger.Error(err, "Get TaskAgent before fire")
		r.recordFire(key, fireTime)
		return
	}

	body := []byte("{}")
	if ta.Spec.ScheduleInput != nil && len(ta.Spec.ScheduleInput.Raw) > 0 {
		body = ta.Spec.ScheduleInput.Raw
	}

	timeout := invokeTimeoutCap
	if ta.Spec.TimeoutSeconds != nil && *ta.Spec.TimeoutSeconds > 0 {
		if d := time.Duration(*ta.Spec.TimeoutSeconds) * time.Second; d < timeout {
			timeout = d
		}
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	invokeErr := r.Invoker.Invoke(ctx, &ta, body)
	if invokeErr != nil {
		logger.Error(invokeErr, "Cron fire failed")
	}
	// Record lastFire on both success and failure: K8s CronJob's
	// status.lastScheduleTime tracks the schedule clock, not the
	// invocation outcome.
	r.recordFire(key, fireTime)

	status := metav1.ConditionTrue
	reason := reasonScheduleRegistered
	msg := fmt.Sprintf("Last fire at %s succeeded", fireTime.UTC().Format(time.RFC3339))
	if invokeErr != nil {
		status = metav1.ConditionFalse
		reason = reasonLastFireFailed
		msg = fmt.Sprintf("Last fire at %s failed: %s", fireTime.UTC().Format(time.RFC3339), invokeErr)
	}
	if err := r.patchScheduled(parent, &ta, status, reason, msg); err != nil {
		logger.Error(err, "Patching Scheduled condition after fire")
	}
}

func (r *TaskAgentCronReconciler) recordFire(key types.NamespacedName, fireTime time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.entries[key]; ok {
		e.lastFire = fireTime
	}
}

var (
	_ manager.LeaderElectionRunnable = (*cronTicker)(nil)
	_ manager.Runnable               = (*cronTicker)(nil)
)
