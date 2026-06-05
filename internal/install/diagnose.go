package install

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// A Deployment that never reaches DeploymentAvailable=True (an un-pullable
// image, a missing PVC provisioner, a crash loop) otherwise waits out the whole
// --timeout in silence: the Available condition alone says nothing about WHY.
// The helpers here read the pod-level reasons the kubelet records and surface
// them so the operator sees "ImagePullBackOff: Back-off pulling image ..."
// immediately instead of an opaque hang.

const (
	// diagInterval is how often a diagnostic waiter re-lists a Deployment's
	// pods to report why it isn't Available yet.
	diagInterval = 2 * time.Second
	// stuckGrace is how long a container that can't start (a bad image, a config
	// error, a crash loop) is tolerated before a waiter fails fast. A transient
	// ErrImagePull (a registry hiccup, a rate limit) or a CreateContainerConfigError
	// racing an async-minted Secret often clears on the next try, so we don't
	// abort on first sight; once it stays stuck past this window it won't
	// self-heal and waiting out the full --timeout only wastes the operator's
	// time. Pod-scheduling problems (Unschedulable) are deliberately excluded —
	// they can clear as the cluster scales — so they're surfaced live but left to
	// the full --timeout.
	stuckGrace = 20 * time.Second
)

// blockingWaitReasons are container .state.waiting.reason values that mean the
// container can't start — as opposed to the benign "ContainerCreating" /
// "PodInitializing" the kubelet reports while a pod is simply coming up.
var blockingWaitReasons = map[string]bool{
	"ImagePullBackOff":           true,
	"ErrImagePull":               true,
	"InvalidImageName":           true,
	"ErrImageNeverPull":          true,
	"RegistryUnavailable":        true,
	"CreateContainerConfigError": true,
	"CreateContainerError":       true,
	"RunContainerError":          true,
	"CrashLoopBackOff":           true,
}

// terminalWaitReasons never self-resolve — the spec is wrong (a malformed image
// ref, a never-pull policy with no local image). A waiter aborts on these
// immediately rather than retrying to the timeout.
var terminalWaitReasons = map[string]bool{
	"InvalidImageName":  true,
	"ErrImageNeverPull": true,
}

// podIssue is one pod/container in a state that blocks a rollout: an image-pull
// failure, a container-config error, a crash loop, or an unschedulable pod.
type podIssue struct {
	pod      string
	reason   string // e.g. "ImagePullBackOff", "Unschedulable"
	detail   string // the kubelet's message, when set
	terminal bool   // reason will never self-resolve (fail fast immediately)
	failFast bool   // a stuck container — fail fast once it persists past stuckGrace
}

// String renders the issue pod-first ("pod: reason — detail"), for aggregated
// error messages where the full terminal width is available.
func (i podIssue) String() string {
	if i.detail != "" {
		return fmt.Sprintf("%s: %s — %s", i.pod, i.reason, i.detail)
	}
	return fmt.Sprintf("%s: %s", i.pod, i.reason)
}

// reasonLine renders the issue reason-first ("reason — detail [pod]"), for the
// live status line and the fail-fast error: the reason (ImagePullBackOff, ...)
// leads so it survives truncation to the terminal width — the one word the
// operator most needs to see.
func (i podIssue) reasonLine() string {
	if i.detail != "" {
		return fmt.Sprintf("%s — %s [%s]", i.reason, i.detail, i.pod)
	}
	return fmt.Sprintf("%s [%s]", i.reason, i.pod)
}

// joinIssues renders a set of issues as a single "; "-separated line for an
// error message.
func joinIssues(issues []podIssue) string {
	parts := make([]string, 0, len(issues))
	for _, i := range issues {
		parts = append(parts, i.String())
	}
	return strings.Join(parts, "; ")
}

// diagnoseDeploymentPods lists the pods backing ns/name and returns the issues
// blocking the rollout. Returns nil when no pod is in a blocking state — e.g.
// the pods are merely pulling/creating, which is not yet an error. Any client
// error (the Deployment or its pods not existing yet) yields nil: the absence
// of a pod is not itself a diagnosis.
func diagnoseDeploymentPods(ctx context.Context, core kubernetes.Interface, ns, name string) []podIssue {
	dep, err := core.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil
	}
	return podIssuesForDeployment(ctx, core, dep)
}

// podIssuesForDeployment is diagnoseDeploymentPods given an already-fetched
// Deployment, so a caller that has just GET'd it (the readiness gate) doesn't
// pay for a second round-trip.
func podIssuesForDeployment(ctx context.Context, core kubernetes.Interface, dep *appsv1.Deployment) []podIssue {
	sel, err := metav1.LabelSelectorAsSelector(dep.Spec.Selector)
	if err != nil || sel.Empty() {
		return nil
	}
	pods, err := core.CoreV1().Pods(dep.Namespace).List(ctx, metav1.ListOptions{LabelSelector: sel.String()})
	if err != nil {
		return nil
	}
	var issues []podIssue
	for i := range pods.Items {
		issues = append(issues, diagnosePod(&pods.Items[i])...)
	}
	// Stable order so streamed lines don't reshuffle round-to-round.
	sort.Slice(issues, func(a, b int) bool {
		if issues[a].pod != issues[b].pod {
			return issues[a].pod < issues[b].pod
		}
		return issues[a].reason < issues[b].reason
	})
	return issues
}

// diagnosePod inspects one pod's init/main container waiting states and its
// scheduling condition. Running/Succeeded pods, and pods merely creating
// containers, contribute nothing.
func diagnosePod(p *corev1.Pod) []podIssue {
	var issues []podIssue

	// Unschedulable / unbound-PVC pods sit Pending with PodScheduled=False and
	// no container statuses at all. The cm mounts 3 RWO PVCs, so a missing
	// provisioner shows up here, not as a container waiting reason.
	if p.Status.Phase == corev1.PodPending {
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse && c.Reason != "" {
				issues = append(issues, podIssue{
					pod:    p.Name,
					reason: c.Reason, // e.g. "Unschedulable"
					detail: strings.TrimSpace(c.Message),
				})
			}
		}
	}

	add := func(statuses []corev1.ContainerStatus) {
		for _, cs := range statuses {
			w := cs.State.Waiting
			if w == nil || !blockingWaitReasons[w.Reason] {
				continue
			}
			issues = append(issues, podIssue{
				pod:      p.Name,
				reason:   w.Reason,
				detail:   strings.TrimSpace(w.Message),
				failFast: true, // a container that can't start; grace-gated in firstFailFast
				terminal: terminalWaitReasons[w.Reason],
			})
		}
	}
	add(p.Status.InitContainerStatuses)
	add(p.Status.ContainerStatuses)
	return issues
}

// runWithPodDiagnostics runs the blocking wait fn (a deployment-Available or
// rollout wait) while a background goroutine lists ns/name's pods every
// diagInterval and (a) streams each newly-observed blocking reason via log and
// (b) aborts fn early — by cancelling its context — when a pod hits a terminal
// reason or stays in an image-pull/crash backoff past backoffGrace. Without
// this, an un-pullable image makes fn wait out the entire --timeout in silence.
//
// On a diagnoser-triggered abort the diagnostic (fail-fast) error is returned.
// Otherwise fn's own error is returned, annotated with the last-seen pod issues
// when fn failed for an undiagnosed reason (e.g. a plain timeout while a pod was
// still pulling) so the failure is never bare.
func runWithPodDiagnostics(
	ctx context.Context,
	core kubernetes.Interface,
	ns, name string,
	log func(string),
	fn func(context.Context) error,
) error {
	diagCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		diagErr    error
		lastIssues []podIssue
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		reported := make(map[string]bool)
		firstSeen := make(map[string]time.Time)
		t := time.NewTicker(diagInterval)
		defer t.Stop()
		for {
			select {
			case <-diagCtx.Done():
				return
			case <-t.C:
			}
			issues := diagnoseDeploymentPods(diagCtx, core, ns, name)
			// Keep the last non-empty snapshot: a List that returns nil because
			// diagCtx was cancelled (main just saw fn return) must not wipe the
			// diagnosis used to annotate fn's error.
			if len(issues) > 0 {
				lastIssues = issues
			}
			for _, is := range issues {
				k := is.pod + "|" + is.reason
				if firstSeen[k].IsZero() {
					firstSeen[k] = time.Now()
				}
				if log != nil && !reported[k] {
					reported[k] = true
					log(is.reasonLine())
				}
			}
			if is, ok := firstFailFast(issues, firstSeen); ok {
				diagErr = fmt.Errorf(
					"%s/%s cannot start: %s (it will not recover on its own — check the image ref, pull secret, and node capacity)",
					ns, name, is.reasonLine())
				cancel()
				return
			}
		}
	}()

	err := fn(diagCtx)
	cancel()
	<-done // establishes happens-before for diagErr / lastIssues

	// fn succeeding is authoritative: a diagErr set by the diagnoser in the same
	// instant fn observed the Deployment go Available (a recovery landing right
	// at the grace boundary) must not turn a real success into a failure.
	if err == nil {
		return nil
	}
	if diagErr != nil {
		return diagErr
	}
	if len(lastIssues) > 0 {
		return fmt.Errorf("%w; pod status: %s", err, joinIssues(lastIssues))
	}
	return err
}

// firstFailFast returns the first issue worth aborting for: a terminal reason
// (immediately), or a stuck container that has persisted past stuckGrace.
// Pod-scheduling issues (failFast=false, e.g. Unschedulable) are surfaced live
// but never fail fast — they can clear as the cluster scales.
func firstFailFast(issues []podIssue, firstSeen map[string]time.Time) (podIssue, bool) {
	for _, is := range issues {
		if is.terminal {
			return is, true
		}
		if is.failFast {
			if t, ok := firstSeen[is.pod+"|"+is.reason]; ok && time.Since(t) >= stuckGrace {
				return is, true
			}
		}
	}
	return podIssue{}, false
}
