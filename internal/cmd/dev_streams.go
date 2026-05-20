package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/apoxy-dev/clrk/internal/cmd/devtui"
)

// pickPodByOrdinal lists pods in ns matching selector, sorts by name
// (stable across reconciliations because Deployments append a random
// hash suffix that lexically orders the replicas), and returns the
// Nth one's name. Polls every 2s until a pod appears.
func pickPodByOrdinal(ctx context.Context, kc kubernetes.Interface, ns, selector string, ordinal int) (string, error) {
	for {
		pods, err := kc.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err == nil {
			names := make([]string, 0, len(pods.Items))
			for _, p := range pods.Items {
				names = append(names, p.Name)
			}
			sort.Strings(names)
			if ordinal < len(names) {
				return names[ordinal], nil
			}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// streamPodLogsPlain follows logs from the ordinal-th pod matching
// selector in ns, prefixing each line with prefix on os.Stdout. Used
// by the non-TUI plain mode.
func streamPodLogsPlain(ctx context.Context, kubeconfig, ns, selector string, ordinal int, prefix string) {
	go func() {
		streamPodLogs(ctx, kubeconfig, ns, selector, ordinal, func(line string) {
			fmt.Fprintf(os.Stdout, "[%s] %s\n", prefix, line)
		})
	}()
}

// streamPodLogsTUI follows logs from the ordinal-th pod matching
// selector in ns into the TUI under the given component name. The
// caller is responsible for spawning this in a goroutine.
func streamPodLogsTUI(ctx context.Context, prog *devtui.Program, kubeconfig, ns, selector string, ordinal int, component string) {
	streamPodLogs(ctx, kubeconfig, ns, selector, ordinal, func(line string) {
		prog.SendLog(component, line, devtui.StreamStdout)
	})
}

// streamPodLogs is the shared body: wait for a matching pod, open a
// follow log stream against it, forward every line to sink. Reconnects
// after the stream ends (pod replaced by a rollout) up to 5 quick
// retries before giving up — long enough to cover a rollout, short
// enough not to spin forever after the user runs `clrk dev down`.
func streamPodLogs(ctx context.Context, kubeconfig, ns, selector string, ordinal int, sink func(line string)) {
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		slog.Warn("Failed to load kubeconfig for pod log stream", "err", err, "ns", ns, "selector", selector)
		return
	}
	kc, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		slog.Warn("Failed to build clientset for pod log stream", "err", err)
		return
	}
	retries := 0
	for ctx.Err() == nil {
		podName, err := pickPodByOrdinal(ctx, kc, ns, selector, ordinal)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			retries++
			if retries > 5 {
				slog.Warn("Giving up on pod log stream", "ns", ns, "selector", selector, "err", err)
				return
			}
			continue
		}
		req := kc.CoreV1().Pods(ns).GetLogs(podName, &corev1.PodLogOptions{Follow: true})
		stream, err := req.Stream(ctx)
		if err != nil {
			retries++
			if retries > 5 {
				slog.Warn("Giving up on pod log stream", "pod", podName, "err", err)
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		retries = 0
		copyLines(stream, sink)
		_ = stream.Close()
		// Stream ended (pod terminated). Loop to pick the replacement.
	}
}

func copyLines(r io.ReadCloser, sink func(line string)) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		sink(scanner.Text())
	}
}
