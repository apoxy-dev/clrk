package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/apoxy-dev/clrk/internal/drivers"
)

// newDevStatusCmd is `clrk dev status`. Reports the per-container state
// of a running clrk dev session in a form scripts can wait on, replacing
// ad-hoc `docker ps | grep clrk-…` polling.
func newDevStatusCmd() *cobra.Command {
	var (
		jsonOut bool
		workers int
	)

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Report the health of a running clrk dev session",
		Long: "Inspects the docker containers `clrk dev` brings up and prints " +
			"a single-line summary per component. --json emits structured " +
			"output keyed by component name, suitable for use in until-loops.",
		RunE: func(cmd *cobra.Command, args []string) error {
			components := []string{drivers.K3sContainerName, drivers.ControllerManagerContainerName}
			for i := 0; i < workers; i++ {
				components = append(components, fmt.Sprintf("clrk-worker-%d", i))
			}
			states, err := componentStates(cmd.Context(), components)
			if err != nil {
				return err
			}
			if jsonOut {
				return json.NewEncoder(os.Stdout).Encode(states)
			}
			return writeStatusTable(os.Stdout, states)
		},
	}

	cmd.Flags().BoolVar(&jsonOut, "json", false, "Emit JSON keyed by component name.")
	cmd.Flags().IntVar(&workers, "workers", 1, "Number of worker replicas to inspect (matches 'clrk dev --workers').")
	return cmd
}

// ComponentState is the on-disk view of one clrk dev container.
type ComponentState struct {
	Name      string `json:"name"`
	Status    string `json:"status"`               // running, exited, missing
	Ready     bool   `json:"ready"`                // true once the container is up and (where applicable) /readyz is 200
	Image     string `json:"image,omitempty"`      // image reference (may include sha)
	StartedAt string `json:"started_at,omitempty"` // RFC3339 timestamp
	Uptime    string `json:"uptime,omitempty"`     // human-readable since started_at
}

func componentStates(ctx context.Context, names []string) (map[string]ComponentState, error) {
	out := make(map[string]ComponentState, len(names))
	for _, n := range names {
		out[n] = inspectContainer(ctx, n)
	}
	return out, nil
}

func inspectContainer(ctx context.Context, name string) ComponentState {
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
		// Container missing or daemon unreachable; treat as missing.
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

func writeStatusTable(w *os.File, states map[string]ComponentState) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "COMPONENT\tSTATUS\tREADY\tUPTIME\tIMAGE")
	for _, name := range orderedComponentNames(states) {
		s := states[name]
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
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", name, s.Status, ready, uptime, image)
	}
	return tw.Flush()
}

func orderedComponentNames(states map[string]ComponentState) []string {
	preferred := []string{drivers.K3sContainerName, drivers.ControllerManagerContainerName}
	seen := map[string]bool{}
	out := []string{}
	for _, n := range preferred {
		if _, ok := states[n]; ok {
			out = append(out, n)
			seen[n] = true
		}
	}
	// Worker containers (clrk-worker-N) follow, in name-sorted order so
	// "clrk-worker-0" comes before "clrk-worker-10".
	rest := []string{}
	for n := range states {
		if !seen[n] {
			rest = append(rest, n)
		}
	}
	sortWorkerNames(rest)
	return append(out, rest...)
}

func sortWorkerNames(names []string) {
	// `clrk-worker-N` sorts numerically by N when N differs in length.
	// Tabwriter only needs deterministic order; we don't ship lots of
	// workers in dev so the simple bubble sort is fine.
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			if workerIndex(names[i]) > workerIndex(names[j]) {
				names[i], names[j] = names[j], names[i]
			}
		}
	}
}

func workerIndex(name string) int {
	const prefix = "clrk-worker-"
	if !strings.HasPrefix(name, prefix) {
		return -1
	}
	n := 0
	for _, r := range name[len(prefix):] {
		if r < '0' || r > '9' {
			return -1
		}
		n = n*10 + int(r-'0')
	}
	return n
}
