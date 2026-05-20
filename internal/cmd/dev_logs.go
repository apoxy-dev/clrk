package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"

	"github.com/spf13/cobra"

	"github.com/apoxy-dev/clrk/internal/drivers"
)

// newDevLogsCmd is `clrk dev logs [-f]`. Streams the docker logs of every
// `clrk dev` component container, prefixing each line with the
// container name so a single tail covers the whole session — what users
// who hit the lockfile want to do next ("attach to the running clrk dev").
func newDevLogsCmd() *cobra.Command {
	var (
		follow  bool
		tail    string
		workers int
	)

	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Stream the docker logs of every clrk dev component",
		RunE: func(cmd *cobra.Command, args []string) error {
			components := []string{drivers.K3sContainerName, drivers.ControllerManagerContainerName}
			for i := 0; i < workers; i++ {
				components = append(components, fmt.Sprintf("clrk-worker-%d", i))
			}
			return streamComponentLogs(cmd.Context(), components, follow, tail)
		},
	}

	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Follow log output (docker logs -f).")
	cmd.Flags().StringVar(&tail, "tail", "100", "Number of recent lines per component before following.")
	cmd.Flags().IntVar(&workers, "workers", 1, "Number of worker replicas to attach to (matches 'clrk dev --workers').")
	return cmd
}

func streamComponentLogs(ctx context.Context, names []string, follow bool, tail string) error {
	var wg sync.WaitGroup
	var outMu sync.Mutex

	for _, name := range names {
		args := []string{"logs", "--tail", tail}
		if follow {
			args = append(args, "-f")
		}
		args = append(args, name)
		c := exec.CommandContext(ctx, "docker", args...)
		stdout, err := c.StdoutPipe()
		if err != nil {
			return err
		}
		stderr, err := c.StderrPipe()
		if err != nil {
			return err
		}
		if err := c.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "skip %s: %v\n", name, err)
			continue
		}
		wg.Add(2)
		go pump(&wg, &outMu, stdout, name, os.Stdout)
		go pump(&wg, &outMu, stderr, name, os.Stderr)
	}
	wg.Wait()
	return nil
}

func pump(wg *sync.WaitGroup, mu *sync.Mutex, r io.ReadCloser, prefix string, w *os.File) {
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
