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

// tickInterval bounds the worst-case skew between a cron entry's nextFire
// time and the actual fire. Cron expressions are minute-resolution, so a
// 1-Hz sweep is well below the granularity of anything a user can express.
const tickInterval = time.Second

// invokeTimeoutCap upper-bounds the per-fire HTTP call so a wedged backend
// can't pin the ticker goroutine. spec.timeoutSeconds caps from below.
const invokeTimeoutCap = 5 * time.Minute

// cronEntry is the per-TaskAgent in-memory state owned by
// TaskAgentCronReconciler. Mutated under the reconciler's mutex.
type cronEntry struct {
	specSchedule string
	schedule     cron.Schedule
	nextFire     time.Time
	lastFire     time.Time
	lastErr      string
}

// TaskAgentCronReconciler watches TaskAgents with a non-empty spec.schedule
// and fires them on a cron clock. The actual sandbox-invocation path is
// pluggable via Invoker — production uses an HTTP POST to the TaskAgent's
// gateway URL; tests inject a fake.
//
// The fire loop runs on a separate goroutine registered as a Runnable so
// controller-runtime gates start-up on the leader-election lease.
type TaskAgentCronReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	Invoker TaskAgentInvoker
	// Now is overridable so tests can advance the clock without sleeping.
	// Defaults to time.Now in SetupWithManager.
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
		return r.patchScheduledCondition(ctx, &ta, metav1.ConditionFalse, "NotScheduled", "spec.schedule is empty")
	}

	sched, err := cron.ParseStandard(*ta.Spec.Schedule)
	if err != nil {
		r.dropEntry(req.NamespacedName)
		// Admission already rejects bad expressions; defense in depth.
		logger.Error(err, "Schedule failed to parse despite admission validation", "schedule", *ta.Spec.Schedule)
		return r.patchScheduledCondition(ctx, &ta, metav1.ConditionFalse, "ParseError", err.Error())
	}

	r.upsertEntry(req.NamespacedName, *ta.Spec.Schedule, sched)

	return r.patchScheduledCondition(ctx, &ta, metav1.ConditionTrue, "ScheduleRegistered",
		fmt.Sprintf("Cron entry registered for %q", *ta.Spec.Schedule))
}

// upsertEntry installs (or refreshes) a cron entry for key. Preserves
// lastFire/nextFire when the schedule string is unchanged so a no-op
// reconcile doesn't reset the cadence.
func (r *TaskAgentCronReconciler) upsertEntry(key types.NamespacedName, specSched string, sched cron.Schedule) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries = map[types.NamespacedName]*cronEntry{}
	}
	if existing, ok := r.entries[key]; ok && existing.specSchedule == specSched {
		// Same schedule; replace the parsed Schedule (cheap) but keep timing state.
		existing.schedule = sched
		return
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

// patchScheduledCondition updates the Scheduled condition and patches the
// status subresource. Disjoint from TaskAgentRevisionReconciler's writes
// (it touches Conditions[type=WorkerPoolReady|EgressConfigured|RevisionReady|Accepted]
// and the *Revision* + ObservedGeneration fields; we touch Conditions[type=Scheduled]
// + LastScheduleTime + NextScheduleTime).
func (r *TaskAgentCronReconciler) patchScheduledCondition(
	ctx context.Context,
	ta *clrkv1alpha1.TaskAgent,
	status metav1.ConditionStatus,
	reason, message string,
) (ctrl.Result, error) {
	base := ta.DeepCopy()
	cond := metav1.Condition{
		Type:               condScheduled,
		Status:             status,
		ObservedGeneration: ta.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
	meta.SetStatusCondition(&ta.Status.Conditions, cond)
	r.copyTimingToStatus(types.NamespacedName{Namespace: ta.Namespace, Name: ta.Name}, &ta.Status)
	if err := r.Status().Patch(ctx, ta, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, fmt.Errorf("patching Scheduled condition: %w", err)
	}
	return ctrl.Result{}, nil
}

// copyTimingToStatus copies the in-memory entry's last/next fire times onto
// status (or clears them when no entry exists).
func (r *TaskAgentCronReconciler) copyTimingToStatus(key types.NamespacedName, st *clrkv1alpha1.TaskAgentStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[key]
	if !ok {
		st.LastScheduleTime = nil
		st.NextScheduleTime = nil
		return
	}
	if !e.lastFire.IsZero() {
		t := metav1.NewTime(e.lastFire)
		st.LastScheduleTime = &t
	}
	if !e.nextFire.IsZero() {
		t := metav1.NewTime(e.nextFire)
		st.NextScheduleTime = &t
	}
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

// NeedLeaderElection makes controller-runtime gate Start on the lease.
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

// Tick walks all registered entries and synchronously fires those whose
// nextFire time has passed (using r.Now). Exported so tests can drive the
// loop without spawning the production ticker goroutine; production code
// invokes it from cronTicker.Start.
//
// Synchronous fires keep the test ordering deterministic. Production
// callers tolerate this because the per-fire HTTP timeout
// (invokeTimeoutCap or spec.timeoutSeconds, whichever is smaller) caps
// the wall-clock cost.
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
			// Roll forward immediately so a slow Invoker doesn't double-fire on
			// the next tick. Errors update lastErr but don't reset nextFire.
			e.nextFire = e.schedule.Next(now)
		}
	}
	r.mu.Unlock()

	for _, d := range ready {
		r.fire(ctx, d.key, d.fireTime)
	}
}

// fire invokes the TaskAgent and updates entry + status with the outcome.
func (r *TaskAgentCronReconciler) fire(parent context.Context, key types.NamespacedName, fireTime time.Time) {
	logger := log.FromContext(parent).WithValues("taskagent", key, "fireTime", fireTime)

	var ta clrkv1alpha1.TaskAgent
	if err := r.Get(parent, key, &ta); err != nil {
		logger.Error(err, "Get TaskAgent before fire")
		r.recordFire(key, fireTime, err)
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
	// status.lastScheduleTime tracks when the schedule fired, not when
	// the downstream invocation succeeded. lastErr separately carries
	// the error string for the next reconcile's condition message.
	r.recordFire(key, fireTime, invokeErr)
	r.patchFireOutcome(parent, &ta, fireTime, invokeErr)
}

func (r *TaskAgentCronReconciler) recordFire(key types.NamespacedName, fireTime time.Time, fireErr error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[key]
	if !ok {
		return
	}
	e.lastFire = fireTime
	if fireErr != nil {
		e.lastErr = fireErr.Error()
	} else {
		e.lastErr = ""
	}
}

// patchFireOutcome updates the Scheduled condition with the last fire's
// outcome and stamps lastScheduleTime / nextScheduleTime onto status.
func (r *TaskAgentCronReconciler) patchFireOutcome(ctx context.Context, ta *clrkv1alpha1.TaskAgent, fireTime time.Time, fireErr error) {
	base := ta.DeepCopy()
	reason, msg := "ScheduleRegistered", fmt.Sprintf("Last fire at %s succeeded", fireTime.UTC().Format(time.RFC3339))
	st := metav1.ConditionTrue
	if fireErr != nil {
		st = metav1.ConditionFalse
		reason = "LastFireFailed"
		msg = fmt.Sprintf("Last fire at %s failed: %s", fireTime.UTC().Format(time.RFC3339), fireErr)
	}
	meta.SetStatusCondition(&ta.Status.Conditions, metav1.Condition{
		Type:               condScheduled,
		Status:             st,
		ObservedGeneration: ta.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            msg,
	})
	r.copyTimingToStatus(types.NamespacedName{Namespace: ta.Namespace, Name: ta.Name}, &ta.Status)
	if err := r.Status().Patch(ctx, ta, client.MergeFrom(base)); err != nil {
		log.FromContext(ctx).Error(err, "Patching Scheduled condition after fire", "taskagent", ta.Name)
	}
}

// Compile-time assertion that cronTicker satisfies the manager.Runnable +
// manager.LeaderElectionRunnable contract controller-runtime expects.
var _ manager.LeaderElectionRunnable = (*cronTicker)(nil)
var _ manager.Runnable = (*cronTicker)(nil)
