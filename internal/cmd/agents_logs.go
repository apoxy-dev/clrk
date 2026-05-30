package cmd

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/apoxy-dev/clrk/internal/workerlog"
)

var agentSandboxRevisionGVR = schema.GroupVersionResource{Group: "clrk.apoxy.dev", Version: "v1alpha1", Resource: "agentsandboxrevisions"}

// newAgentsLogsCmd is `clrk agents logs <name>[/<invocation>]` — streams
// sandbox stdio from whichever worker Pod holds the agent's log via SPDY
// `kubectl exec` (one transport, dev and prod alike — workers are real
// Pods in both).
//
//   - DaemonAgent <name>: the elected worker's per-agent log.
//   - TaskAgent <name>: the latest invocation (the per-agent symlink the
//     worker repoints on every dispatch).
//   - TaskAgent <name>/<invocation>: that specific invocation's log
//     (APO-619 / APO-621). The worker that ran it isn't known up front,
//     so ready workers are tried in turn until one has the file.
func newAgentsLogsCmd() *cobra.Command {
	var (
		namespace  string
		local      bool
		kubeconfig string
		follow     bool
		tailLines  int
	)
	cmd := &cobra.Command{
		Use:   "logs NAME[/INVOCATION]",
		Short: "Stream sandbox stdio for a DaemonAgent or TaskAgent (optionally a specific invocation)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			kc, err := resolveKubeconfig(kubeconfig, local)
			if err != nil {
				return err
			}
			cfg, err := clientcmd.BuildConfigFromFlags("", kc)
			if err != nil {
				return fmt.Errorf("loading kubeconfig %s: %w", kc, err)
			}
			dyn, err := dynamic.NewForConfig(cfg)
			if err != nil {
				return fmt.Errorf("dynamic client: %w", err)
			}
			ns := namespace
			if ns == "" {
				if ns, err = contextNamespace(kc); err != nil {
					return err
				}
			}
			return streamAgentLogs(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(),
				dyn, cfg, kc, ns, args[0], follow, tailLines)
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Target namespace (default: kubeconfig context).")
	cmd.Flags().BoolVar(&local, "local", false, "Target the kubeconfig of the running 'clrk dev' session (~/.clrk/kubeconfig.host).")
	cmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "Explicit kubeconfig path (takes precedence over --local and $KUBECONFIG).")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Follow the log stream (kubectl-style; default false).")
	cmd.Flags().IntVar(&tailLines, "tail", 0, "Number of trailing lines to print (0 = entire log).")
	return cmd
}

func streamAgentLogs(ctx context.Context, stdout, stderr io.Writer, dyn dynamic.Interface, cfg *rest.Config, kubeconfig, ns, arg string, follow bool, tailLines int) error {
	name, invID, _ := strings.Cut(arg, "/")

	// DaemonAgent: per-agent log on the elected worker. No per-invocation
	// notion (a daemon is one long-lived process).
	if da, err := dyn.Resource(daemonAgentGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
		if invID != "" {
			return fmt.Errorf("%s/%s is a DaemonAgent; per-invocation logs apply only to TaskAgents", ns, name)
		}
		rev, err := agentRevision(ctx, dyn, ns, da)
		if err != nil {
			return err
		}
		pod, err := pickWorkerPod(rev)
		if err != nil {
			return err
		}
		logPath := workerlog.AgentPath(workerlog.Dir, ns, name)
		return execKube(ctx, stdout, stderr, cfg, ns, pod, buildTailArgs(logPath, follow, tailLines))
	}

	// TaskAgent: <name> tails the latest invocation (the per-agent symlink
	// the worker repoints on each dispatch); <name>/<id> tails one
	// invocation's file. The worker that ran a given invocation isn't
	// known here, so try every ready worker until one has the file.
	ta, err := dyn.Resource(taskAgentGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("getting agent %s/%s: %w", ns, name, err)
	}
	rev, err := agentRevision(ctx, dyn, ns, ta)
	if err != nil {
		return err
	}
	pods := readyWorkerPods(rev)
	if len(pods) == 0 {
		return fmt.Errorf("no worker has a ready sandbox for %s/%s yet (status.workers is empty); is the agent scheduled?", ns, name)
	}
	logPath := workerlog.AgentPath(workerlog.Dir, ns, name)
	if invID != "" {
		logPath = workerlog.InvocationPath(workerlog.Dir, ns, name, invID)
	}
	return streamFromAnyWorker(ctx, stdout, stderr, cfg, ns, pods, logPath, follow, tailLines)
}

// agentRevision resolves an agent's current revision object from its
// status (preferring the ready revision, falling back to the created one).
func agentRevision(ctx context.Context, dyn dynamic.Interface, ns string, agent *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	revName, _, _ := unstructured.NestedString(agent.Object, "status", "latestReadyRevisionName")
	if revName == "" {
		revName, _, _ = unstructured.NestedString(agent.Object, "status", "latestCreatedRevisionName")
	}
	if revName == "" {
		return nil, fmt.Errorf("%s/%s has no revision yet; agent has not been scheduled", ns, agent.GetName())
	}
	rev, err := dyn.Resource(agentSandboxRevisionGVR).Namespace(ns).Get(ctx, revName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting AgentSandboxRevision %s/%s: %w", ns, revName, err)
	}
	return rev, nil
}

// readyWorkerPods returns every worker Pod that has pulled the image
// (so the log dir exists), newest-listed first.
func readyWorkerPods(rev *unstructured.Unstructured) []string {
	workers, _, _ := unstructured.NestedSlice(rev.Object, "status", "workers")
	var pods []string
	for _, raw := range workers {
		m, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		if pulled, _, _ := unstructured.NestedBool(m, "imagePulled"); !pulled {
			continue
		}
		if pod, _, _ := unstructured.NestedString(m, "podName"); pod != "" {
			pods = append(pods, pod)
		}
	}
	return pods
}

// streamFromAnyWorker tails logPath on each candidate worker in turn,
// returning as soon as one streams. A `test -f` guard makes a worker that
// lacks the file fail fast (sh exits non-zero) so we move to the next one
// rather than blocking on `tail -F` of a path that will never appear.
func streamFromAnyWorker(ctx context.Context, stdout, stderr io.Writer, cfg *rest.Config, ns string, pods []string, logPath string, follow bool, tailLines int) error {
	var lastErr error
	for _, pod := range pods {
		err := execKube(ctx, stdout, stderr, cfg, ns, pod, buildGuardedTailScript(logPath, follow, tailLines))
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		lastErr = err
	}
	return fmt.Errorf("no worker holds log %s (tried %d pod(s)); last error: %w", logPath, len(pods), lastErr)
}

func pickWorkerPod(rev *unstructured.Unstructured) (string, error) {
	workers, _, _ := unstructured.NestedSlice(rev.Object, "status", "workers")
	for _, raw := range workers {
		m, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		// `imagePulled=true` is the closest analogue to "this worker has
		// the sandbox" — without it `tail` will fail because the file
		// doesn't exist yet.
		if pulled, _, _ := unstructured.NestedBool(m, "imagePulled"); !pulled {
			continue
		}
		if pod, _, _ := unstructured.NestedString(m, "podName"); pod != "" {
			return pod, nil
		}
	}
	return "", fmt.Errorf("no worker has the sandbox ready yet (status.workers is empty); is the agent scheduled?")
}

func buildTailArgs(logPath string, follow bool, tailLines int) []string {
	tailLineFlag := "+1"
	if tailLines > 0 {
		tailLineFlag = fmt.Sprintf("%d", tailLines)
	}
	args := []string{"tail", "-n", tailLineFlag}
	if follow {
		// -F (capital) handles file replacement across agent restarts —
		// `-f` would silently stop following after a rotation.
		args = append(args, "-F")
	}
	args = append(args, logPath)
	return args
}

// buildGuardedTailScript wraps buildTailArgs in `test -f <path> && exec
// <tail>` so a worker that lacks the file exits non-zero immediately,
// letting streamFromAnyWorker try the next pod instead of blocking on
// `tail -F`. The path is built from DNS-label names + a UUID, so it is
// shell-safe; single-quoting guards against future format changes.
func buildGuardedTailScript(logPath string, follow bool, tailLines int) []string {
	tail := strings.Join(buildTailArgs(logPath, follow, tailLines), " ")
	return []string{"sh", "-c", fmt.Sprintf("test -f '%s' && exec %s", logPath, tail)}
}

func execKube(ctx context.Context, stdout, stderr io.Writer, cfg *rest.Config, namespace, pod string, command []string) error {
	cfg = rest.CopyConfig(cfg)
	cfg.GroupVersion = &corev1.SchemeGroupVersion
	cfg.APIPath = "/api"
	cfg.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	restClient, err := rest.RESTClientFor(cfg)
	if err != nil {
		return fmt.Errorf("rest client: %w", err)
	}
	req := restClient.Post().
		Resource("pods").
		Name(pod).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: workerContainerNameInPod,
			Command:   command,
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(cfg, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("creating SPDY executor: %w", err)
	}
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: stdout,
		Stderr: stderr,
	}); err != nil {
		return fmt.Errorf("kubectl exec %s/%s: %w", namespace, pod, err)
	}
	return nil
}

// workerContainerNameInPod is the container name inside a worker Pod.
// Every WorkerPool's PodTemplate names its container "worker" today; if
// that ever drifts, plumb it from the WorkerPool spec.
const workerContainerNameInPod = "worker"
