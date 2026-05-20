package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/apoxy-dev/clrk/internal/drivers"
)

// newDevLogsCmd is `clrk dev logs [-f]`. Streams the k3d server's docker
// logs alongside cm + worker pod logs (via the apiserver), prefixing
// each line with the component name so a single tail covers the whole
// session.
func newDevLogsCmd() *cobra.Command {
	var (
		follow  bool
		tail    string
		workers int
	)

	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Stream the logs of every clrk dev component",
		RunE: func(cmd *cobra.Command, args []string) error {
			dataDir := clrkDir
			kubeconfig := filepath.Join(dataDir, "kubeconfig.host")
			return streamComponentLogs(cmd.Context(), kubeconfig, follow, tail, workers)
		},
	}

	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Follow log output.")
	cmd.Flags().StringVar(&tail, "tail", "100", "Number of recent lines per component before following.")
	cmd.Flags().IntVar(&workers, "workers", 1, "Number of worker replicas to attach to (matches 'clrk dev --workers').")
	return cmd
}

func streamComponentLogs(ctx context.Context, kubeconfig string, follow bool, tail string, workers int) error {
	var wg sync.WaitGroup
	var outMu sync.Mutex

	// k3d server is still a docker container — tail it via `docker logs`.
	wg.Add(1)
	go func() {
		defer wg.Done()
		dockerLogsCmd(ctx, &outMu, drivers.ClusterServerContainerName, follow, tail)
	}()

	// Build clientset once for cm + worker pod streaming.
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return fmt.Errorf("loading kubeconfig %s: %w", kubeconfig, err)
	}
	kc, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("clientset: %w", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		streamPodLogsCLI(ctx, kc, devClrkNamespace, podSelectorControllerManager, 0,
			controllerManagerComponent, follow, tail, &outMu)
	}()
	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			streamPodLogsCLI(ctx, kc, "default", podSelectorWorkers, i,
				workerComponent(i), follow, tail, &outMu)
		}()
	}

	wg.Wait()
	return nil
}

func dockerLogsCmd(ctx context.Context, mu *sync.Mutex, name string, follow bool, tail string) {
	args := []string{"logs", "--tail", tail}
	if follow {
		args = append(args, "-f")
	}
	args = append(args, name)
	c := exec.CommandContext(ctx, "docker", args...)
	stdout, err := c.StdoutPipe()
	if err != nil {
		return
	}
	stderr, err := c.StderrPipe()
	if err != nil {
		return
	}
	if err := c.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "skip %s: %v\n", name, err)
		return
	}
	var pumpWg sync.WaitGroup
	pumpWg.Add(2)
	go pumpDocker(&pumpWg, mu, stdout, name, os.Stdout)
	go pumpDocker(&pumpWg, mu, stderr, name, os.Stderr)
	pumpWg.Wait()
}

func pumpDocker(wg *sync.WaitGroup, mu *sync.Mutex, r io.ReadCloser, prefix string, w *os.File) {
	defer wg.Done()
	defer r.Close()
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		mu.Lock()
		fmt.Fprintf(w, "[%s] %s\n", prefix, sc.Text())
		mu.Unlock()
	}
}

func streamPodLogsCLI(ctx context.Context, kc kubernetes.Interface, ns, selector string, ordinal int,
	prefix string, follow bool, tail string, mu *sync.Mutex,
) {
	pods, err := kc.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		fmt.Fprintf(os.Stderr, "skip %s: list pods: %v\n", prefix, err)
		return
	}
	names := make([]string, 0, len(pods.Items))
	for _, p := range pods.Items {
		names = append(names, p.Name)
	}
	sort.Strings(names)
	if ordinal >= len(names) {
		fmt.Fprintf(os.Stderr, "skip %s: no pod at ordinal %d (%d matching)\n", prefix, ordinal, len(names))
		return
	}
	podName := names[ordinal]
	var tailLines *int64
	if n, err := parseTail(tail); err == nil {
		tailLines = &n
	}
	req := kc.CoreV1().Pods(ns).GetLogs(podName, &corev1.PodLogOptions{
		Follow:    follow,
		TailLines: tailLines,
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "skip %s: stream logs: %v\n", prefix, err)
		return
	}
	defer stream.Close()
	sc := bufio.NewScanner(stream)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		mu.Lock()
		fmt.Fprintf(os.Stdout, "[%s] %s\n", prefix, sc.Text())
		mu.Unlock()
	}
}

func parseTail(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty")
	}
	var n int64
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("not numeric")
		}
		n = n*10 + int64(r-'0')
	}
	return n, nil
}
