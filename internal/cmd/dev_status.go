package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/apoxy-dev/clrk/internal/drivers"
)

// newDevStatusCmd is `clrk dev status`. Reports the per-component state
// of a running clrk dev session: docker for the k3d server, Pod state
// for cm + workers. --json emits structured output keyed by component
// name for until-loops.
func newDevStatusCmd() *cobra.Command {
	var (
		jsonOut bool
		workers int
	)

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Report the health of a running clrk dev session",
		RunE: func(cmd *cobra.Command, args []string) error {
			dataDir := clrkDir
			kubeconfig := filepath.Join(dataDir, "kubeconfig.host")
			states, err := devComponentStates(cmd.Context(), kubeconfig, workers)
			if err != nil {
				return err
			}
			if jsonOut {
				out := map[string]ComponentState{}
				// session may have a registry port that's useful for
				// scripts; expose under a synthetic key.
				if sess, sErr := readDevSession(dataDir); sErr == nil {
					out["registryPort"] = ComponentState{
						Name:   "registryPort",
						Status: fmt.Sprintf("%d", sess.RegistryHostPort),
					}
				}
				for _, s := range states {
					out[s.Name] = s
				}
				return json.NewEncoder(os.Stdout).Encode(out)
			}
			return writeStatusTable(os.Stdout, states)
		},
	}

	cmd.Flags().BoolVar(&jsonOut, "json", false, "Emit JSON keyed by component name.")
	cmd.Flags().IntVar(&workers, "workers", 1, "Number of worker replicas to inspect (matches 'clrk dev --workers').")
	return cmd
}

// ComponentState is one row in `clrk dev status`. Source-of-truth varies
// per component: docker inspect for the k3d server, Pod phase + Ready
// condition for cm + workers.
type ComponentState struct {
	Name      string `json:"name"`
	Status    string `json:"status"`
	Ready     bool   `json:"ready"`
	Image     string `json:"image,omitempty"`
	StartedAt string `json:"started_at,omitempty"`
	Uptime    string `json:"uptime,omitempty"`
}

func devComponentStates(ctx context.Context, kubeconfig string, workers int) ([]ComponentState, error) {
	out := []ComponentState{inspectDockerContainer(ctx, drivers.ClusterServerContainerName)}

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		// If kubeconfig isn't readable, report the cluster row and
		// every other component as missing.
		out = append(out, missingState(controllerManagerComponent))
		for i := 0; i < workers; i++ {
			out = append(out, missingState(workerComponent(i)))
		}
		return out, nil
	}
	kc, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		out = append(out, missingState(controllerManagerComponent))
		for i := 0; i < workers; i++ {
			out = append(out, missingState(workerComponent(i)))
		}
		return out, nil
	}

	out = append(out, inspectPod(ctx, kc, devClrkNamespace, podSelectorControllerManager, 0, controllerManagerComponent))
	for i := 0; i < workers; i++ {
		out = append(out, inspectPod(ctx, kc, "default", podSelectorWorkers, i, workerComponent(i)))
	}
	return out, nil
}

func missingState(name string) ComponentState {
	return ComponentState{Name: name, Status: "missing"}
}

func inspectDockerContainer(ctx context.Context, name string) ComponentState {
	st := ComponentState{Name: name, Status: "missing"}
	type dockerInspect struct {
		State struct {
			Status    string `json:"Status"`
			Running   bool   `json:"Running"`
			StartedAt string `json:"StartedAt"`
		} `json:"State"`
		Config struct {
			Image string `json:"Image"`
		} `json:"Config"`
	}
	out, err := exec.CommandContext(ctx, "docker", "inspect", name).Output()
	if err != nil {
		return st
	}
	var arr []dockerInspect
	if err := json.Unmarshal(out, &arr); err != nil || len(arr) == 0 {
		return st
	}
	d := arr[0]
	st.Status = d.State.Status
	st.Image = d.Config.Image
	st.StartedAt = d.State.StartedAt
	if t, err := time.Parse(time.RFC3339Nano, d.State.StartedAt); err == nil && !t.IsZero() {
		st.Uptime = time.Since(t).Round(time.Second).String()
	}
	st.Ready = d.State.Running
	return st
}

func inspectPod(ctx context.Context, kc kubernetes.Interface, ns, selector string, ordinal int, name string) ComponentState {
	st := ComponentState{Name: name, Status: "missing"}
	pods, err := kc.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return st
	}
	names := make([]string, 0, len(pods.Items))
	idx := make(map[string]corev1.Pod, len(pods.Items))
	for _, p := range pods.Items {
		names = append(names, p.Name)
		idx[p.Name] = p
	}
	sort.Strings(names)
	if ordinal >= len(names) {
		return st
	}
	p := idx[names[ordinal]]
	st.Status = string(p.Status.Phase)
	st.StartedAt = ""
	if p.Status.StartTime != nil {
		st.StartedAt = p.Status.StartTime.Format(time.RFC3339)
		st.Uptime = time.Since(p.Status.StartTime.Time).Round(time.Second).String()
	}
	if len(p.Spec.Containers) > 0 {
		st.Image = p.Spec.Containers[0].Image
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			st.Ready = true
			break
		}
	}
	return st
}

func writeStatusTable(w *os.File, states []ComponentState) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "COMPONENT\tSTATUS\tREADY\tUPTIME\tIMAGE")
	for _, s := range states {
		ready := "no"
		if s.Ready {
			ready = "yes"
		}
		uptime := s.Uptime
		if uptime == "" {
			uptime = "-"
		}
		image := s.Image
		if image == "" {
			image = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", s.Name, s.Status, ready, uptime, image)
	}
	return tw.Flush()
}
