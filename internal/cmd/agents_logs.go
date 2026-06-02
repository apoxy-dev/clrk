package cmd

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	clrkAPIGroup   = "clrk.apoxy.dev"
	clrkAPIVersion = "v1alpha1"

	// componentWorker scopes the /logs read to a sandbox's own stdio (the
	// worker is the producer of agent stdout/stderr; APO-718). Wire-frozen
	// to otelemit.ComponentWorker — hardcoded here so the CLI doesn't pull
	// the otel SDK in through internal/otelemit.
	componentWorker = "worker"

	// logsBacklogLimit is the default ceiling on the one-shot (non --tail)
	// read: the most-recent N agent log lines. The server caps a single
	// page at 1000, so this is also the max a single GET returns.
	logsBacklogLimit = 1000
)

// newAgentsLogsCmd is `clrk agents logs <name>[/<invocation>]` — it reads
// an agent's stdout/stderr from the aggregated apiserver's
// {taskagents,daemonagents}/{name}/logs subresource (APO-719/APO-720),
// which serves the OTLP LogRecords the worker emits for sandbox stdio
// (APO-718). This replaces the old `kubectl exec`/`tail -F` transport:
// no pod-exec rights are required, any worker can be the source (the API
// reads ClickHouse, not a per-worker file), and the same endpoint backs a
// web UI.
//
//   - DaemonAgent <name>: the daemon's stdio.
//   - TaskAgent <name>: every invocation's stdio for the agent.
//   - TaskAgent <name>/<invocation>: one invocation's stdio (?invocation=).
func newAgentsLogsCmd() *cobra.Command {
	var (
		namespace  string
		local      bool
		kubeconfig string
		follow     bool
		tailLines  int
		iostream   string
	)
	cmd := &cobra.Command{
		Use:   "logs NAME[/INVOCATION]",
		Short: "Stream sandbox stdio for a DaemonAgent or TaskAgent (optionally a specific invocation)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if iostream != "" && iostream != "stdout" && iostream != "stderr" {
				return fmt.Errorf("--iostream must be stdout or stderr, got %q", iostream)
			}
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
				dyn, cfg, ns, args[0], follow, tailLines, iostream)
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Target namespace (default: kubeconfig context).")
	cmd.Flags().BoolVar(&local, "local", false, "Target the kubeconfig of the running 'clrk dev' session (~/.clrk/kubeconfig.host).")
	cmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "Explicit kubeconfig path (takes precedence over --local and $KUBECONFIG).")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Follow the log stream (default false).")
	cmd.Flags().IntVar(&tailLines, "tail", 0, "Number of trailing lines to print (0 = up to the most recent 1000).")
	cmd.Flags().StringVar(&iostream, "iostream", "", "Restrict to one stream: stdout or stderr (default: both).")
	return cmd
}

func streamAgentLogs(ctx context.Context, stdout, stderr io.Writer, dyn dynamic.Interface, cfg *rest.Config, ns, arg string, follow bool, tailLines int, iostream string) error {
	name, invID, _ := strings.Cut(arg, "/")

	// The /logs subresource is scoped per parent kind, so resolve which one
	// owns the name. DaemonAgents have no per-invocation notion.
	var resource string
	if _, err := dyn.Resource(daemonAgentGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
		if invID != "" {
			return fmt.Errorf("%s/%s is a DaemonAgent; per-invocation logs apply only to TaskAgents", ns, name)
		}
		resource = "daemonagents"
	} else if _, err := dyn.Resource(taskAgentGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
		resource = "taskagents"
	} else {
		return fmt.Errorf("no TaskAgent or DaemonAgent %s/%s found", ns, name)
	}

	rc, err := clrkLogsRESTClient(cfg)
	if err != nil {
		return err
	}
	basePath := fmt.Sprintf("/apis/%s/%s/namespaces/%s/%s/%s/logs", clrkAPIGroup, clrkAPIVersion, ns, resource, name)

	// Shared filters: an agent's stdio is the worker-component logs; the
	// optional invocation / iostream selectors narrow further.
	base := url.Values{}
	base.Set("component", componentWorker)
	if invID != "" {
		base.Set("invocation", invID)
	}
	if iostream != "" {
		base.Set("iostream", iostream)
	}

	// Print the existing backlog oldest-first and capture the newest
	// timestamp so --follow can resume exactly after it without a gap.
	watermark, err := printLogBacklog(ctx, stdout, stderr, rc, basePath, base, tailLines)
	if err != nil {
		return err
	}
	if !follow {
		return nil
	}
	return followAgentLogs(ctx, stdout, stderr, rc, basePath, base, watermark)
}

// clrkLogsRESTClient builds a RESTClient scoped to the clrk API group.
// DoRaw/Stream bypass typed decoding, so the serializer here only
// satisfies RESTClientFor.
func clrkLogsRESTClient(cfg *rest.Config) (rest.Interface, error) {
	c := rest.CopyConfig(cfg)
	c.GroupVersion = &schema.GroupVersion{Group: clrkAPIGroup, Version: clrkAPIVersion}
	c.APIPath = "/apis"
	c.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	return rest.RESTClientFor(c)
}

// printLogBacklog GETs one page of the newest records (DESC), prints them
// oldest-first, and returns the newest timestamp seen. --tail N caps the
// page to N; otherwise it reads up to logsBacklogLimit. Query params ride
// .Param() (never the path) so client-go URL-encodes them as a real query
// string.
func printLogBacklog(ctx context.Context, stdout, stderr io.Writer, rc rest.Interface, basePath string, base url.Values, tailLines int) (time.Time, error) {
	limit := logsBacklogLimit
	if tailLines > 0 {
		limit = tailLines
	}
	req := rc.Get().AbsPath(basePath)
	for k, vs := range base {
		for _, v := range vs {
			req = req.Param(k, v)
		}
	}
	req = req.Param("limit", strconv.Itoa(limit))
	raw, err := req.DoRaw(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("reading logs: %w", err)
	}
	var ld logspb.LogsData
	if err := protojson.Unmarshal(raw, &ld); err != nil {
		return time.Time{}, fmt.Errorf("decoding logs (%d bytes): %w", len(raw), err)
	}
	records := flattenLogRecords(&ld)
	sortRecordsByTime(records)
	var watermark time.Time
	for _, r := range records {
		printLogRecord(stdout, stderr, r)
		if t := time.Unix(0, int64(r.GetTimeUnixNano())); t.After(watermark) {
			watermark = t
		}
	}
	return watermark, nil
}

// followAgentLogs opens the streaming endpoint (?follow=true) and prints
// each NDJSON LogsData chunk as it arrives. since resumes strictly after
// the backlog's newest record; a zero since lets the server tail from now.
func followAgentLogs(ctx context.Context, stdout, stderr io.Writer, rc rest.Interface, basePath string, base url.Values, since time.Time) error {
	req := rc.Get().AbsPath(basePath)
	for k, vs := range base {
		for _, v := range vs {
			req = req.Param(k, v)
		}
	}
	req = req.Param("follow", "true")
	if !since.IsZero() {
		req = req.Param("since", since.Format(time.RFC3339Nano))
	}
	stream, err := req.Stream(ctx)
	if err != nil {
		return fmt.Errorf("opening follow stream: %w", err)
	}
	defer stream.Close()

	sc := bufio.NewScanner(stream)
	// Allow large lines (a captured stdio line can be up to 64 KiB).
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var ld logspb.LogsData
		if err := protojson.Unmarshal(line, &ld); err != nil {
			// Skip a malformed chunk rather than tearing down the stream.
			continue
		}
		records := flattenLogRecords(&ld)
		sortRecordsByTime(records)
		for _, r := range records {
			printLogRecord(stdout, stderr, r)
		}
	}
	if err := sc.Err(); err != nil && ctx.Err() == nil {
		return fmt.Errorf("follow stream: %w", err)
	}
	return nil
}

func flattenLogRecords(ld *logspb.LogsData) []*logspb.LogRecord {
	var out []*logspb.LogRecord
	for _, rl := range ld.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			out = append(out, sl.GetLogRecords()...)
		}
	}
	return out
}

func sortRecordsByTime(records []*logspb.LogRecord) {
	sort.SliceStable(records, func(i, j int) bool {
		return records[i].GetTimeUnixNano() < records[j].GetTimeUnixNano()
	})
}

// printLogRecord writes a record's body to stdout, or to stderr when the
// record's log.iostream attribute marks it as the stderr stream so a
// caller can still split the two with shell redirection.
func printLogRecord(stdout, stderr io.Writer, r *logspb.LogRecord) {
	w := stdout
	if recordIoStream(r) == "stderr" {
		w = stderr
	}
	fmt.Fprintln(w, r.GetBody().GetStringValue())
}

func recordIoStream(r *logspb.LogRecord) string {
	for _, kv := range r.GetAttributes() {
		if kv.GetKey() == "log.iostream" {
			return kv.GetValue().GetStringValue()
		}
	}
	return ""
}
