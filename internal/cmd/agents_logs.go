package cmd

import (
	"context"
	"fmt"
	"io"

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

// newAgentsLogsCmd is `clrk agents logs <name>` — streams sandbox stdio
// from whichever worker Pod holds the agent's running sandbox via SPDY
// `kubectl exec` (one transport, dev and prod alike — workers are real
// Pods in both).
//
// Today only DaemonAgent is supported; TaskAgent per-execution logs need
// the Execution model from APO-564.
func newAgentsLogsCmd() *cobra.Command {
	var (
		namespace  string
		local      bool
		kubeconfig string
		follow     bool
		tailLines  int
	)
	cmd := &cobra.Command{
		Use:   "logs NAME",
		Short: "Stream sandbox stdio for a DaemonAgent",
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

func streamAgentLogs(ctx context.Context, stdout, stderr io.Writer, dyn dynamic.Interface, cfg *rest.Config, kubeconfig, ns, name string, follow bool, tailLines int) error {
	da, err := dyn.Resource(daemonAgentGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		_, taErr := dyn.Resource(taskAgentGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
		if taErr == nil {
			return fmt.Errorf("%s/%s is a TaskAgent; per-execution logs require APO-564 (not implemented)", ns, name)
		}
		return fmt.Errorf("getting DaemonAgent %s/%s: %w", ns, name, err)
	}
	revName, _, _ := unstructured.NestedString(da.Object, "status", "latestReadyRevisionName")
	if revName == "" {
		revName, _, _ = unstructured.NestedString(da.Object, "status", "latestCreatedRevisionName")
	}
	if revName == "" {
		return fmt.Errorf("%s/%s has no revision yet; agent has not been scheduled", ns, name)
	}
	rev, err := dyn.Resource(agentSandboxRevisionGVR).Namespace(ns).Get(ctx, revName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("getting AgentSandboxRevision %s/%s: %w", ns, revName, err)
	}
	pod, err := pickWorkerPod(rev)
	if err != nil {
		return err
	}
	logPath := workerlog.AgentPath(workerlog.Dir, ns, name)
	return execKube(ctx, stdout, stderr, cfg, ns, pod, buildTailArgs(logPath, follow, tailLines))
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
