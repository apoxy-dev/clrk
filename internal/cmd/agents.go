package cmd

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/duration"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	clrkclient "github.com/apoxy-dev/clrk/client/versioned"
)

// daemonAgentGVR/taskAgentGVR drive the parent-kind resolution and per-parent
// subresource paths in the logs/traces/invocations/run-task commands, which use
// the dynamic and raw REST clients (the typed clientset has no verb for a
// list-subresource). The list/get commands here use the typed clientset.
var (
	daemonAgentGVR = schema.GroupVersionResource{Group: "clrk.apoxy.dev", Version: "v1alpha1", Resource: "daemonagents"}
	taskAgentGVR   = schema.GroupVersionResource{Group: "clrk.apoxy.dev", Version: "v1alpha1", Resource: "taskagents"}
)

// newAgentsCmd is `clrk agents` with `list` (default) and `get` subcommands.
// Read-only; mutation flows through `clrk apply`.
func newAgentsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "agents",
		Aliases: []string{"agent"},
		Short:   "List and inspect DaemonAgents and TaskAgents",
	}
	cmd.AddCommand(newAgentsListCmd())
	cmd.AddCommand(newAgentsGetCmd())
	cmd.AddCommand(newAgentsLogsCmd())
	cmd.AddCommand(newAgentsTracesCmd())
	cmd.AddCommand(newAgentsInvocationsCmd())
	cmd.AddCommand(newAgentsRunTaskCmd())
	return cmd
}

func newAgentsListCmd() *cobra.Command {
	var (
		namespace     string
		allNamespaces bool
	)
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List DaemonAgents and TaskAgents",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cs, ns, err := kube.clrkClient(namespace, allNamespaces)
			if err != nil {
				return err
			}
			return listAgents(cmd.Context(), cmd.OutOrStdout(), cs, ns, allNamespaces)
		},
	}
	addReadFlags(cmd, &namespace, &allNamespaces)
	return cmd
}

func newAgentsGetCmd() *cobra.Command {
	var namespace string
	cmd := &cobra.Command{
		Use:   "get NAME",
		Short: "Show details for a single DaemonAgent or TaskAgent",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cs, ns, err := kube.clrkClient(namespace, false)
			if err != nil {
				return err
			}
			return getAgent(cmd.Context(), cmd.OutOrStdout(), cs, ns, args[0])
		},
	}
	addReadFlags(cmd, &namespace, nil)
	return cmd
}

func listAgents(ctx context.Context, out io.Writer, cs clrkclient.Interface, ns string, allNS bool) error {
	daList, err := cs.ClrkV1alpha1().DaemonAgents(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing DaemonAgents: %w", err)
	}
	taList, err := cs.ClrkV1alpha1().TaskAgents(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing TaskAgents: %w", err)
	}
	sortByNamespaceName(daList.Items)
	sortByNamespaceName(taList.Items)
	if len(daList.Items) == 0 && len(taList.Items) == 0 {
		fmt.Fprintln(out, namespaceMsg("No agents found", ns, allNS))
		return nil
	}
	if len(daList.Items) > 0 {
		fmt.Fprintln(out, "DAEMONAGENTS")
		if err := convertAndPrint(ctx, out, daList, allNS); err != nil {
			return err
		}
	}
	if len(taList.Items) > 0 {
		if len(daList.Items) > 0 {
			fmt.Fprintln(out)
		}
		fmt.Fprintln(out, "TASKAGENTS")
		if err := convertAndPrint(ctx, out, taList, allNS); err != nil {
			return err
		}
	}
	return nil
}

func getAgent(ctx context.Context, out io.Writer, cs clrkclient.Interface, ns, name string) error {
	da, daErr := cs.ClrkV1alpha1().DaemonAgents(ns).Get(ctx, name, metav1.GetOptions{})
	ta, taErr := cs.ClrkV1alpha1().TaskAgents(ns).Get(ctx, name, metav1.GetOptions{})
	switch {
	case daErr == nil && taErr == nil:
		return fmt.Errorf("name %q matches both a DaemonAgent and a TaskAgent in namespace %s; this should not happen", name, ns)
	case daErr == nil:
		return printDaemonAgentDetail(out, da)
	case taErr == nil:
		return printTaskAgentDetail(out, ta)
	default:
		return fmt.Errorf("no DaemonAgent or TaskAgent named %q in namespace %s", name, ns)
	}
}

// tableConverter is implemented by the typed list objects (via the API package's
// resourcestrategy.TableConverter methods), so the renderer is generic over kind.
type tableConverter interface {
	ConvertToTable(ctx context.Context, tableOptions runtime.Object) (*metav1.Table, error)
}

// convertAndPrint asks the typed object for its server-defined columns (the same
// metav1.Table `kubectl get` would render) and prints it.
func convertAndPrint(ctx context.Context, out io.Writer, obj tableConverter, allNS bool) error {
	table, err := obj.ConvertToTable(ctx, &metav1.TableOptions{})
	if err != nil {
		return err
	}
	return printResourceTable(out, table, allNS)
}

// printResourceTable renders a metav1.Table via tabwriter. The column headers
// and cells come from the API package's ConvertToTable, so the layout matches
// `kubectl get`. The NAMESPACE column is the printer's job (it is absent from
// the converter), prepended from each row's object metadata under -A.
func printResourceTable(out io.Writer, table *metav1.Table, allNS bool) error {
	tw := newTableWriter(out)
	headers := make([]string, 0, len(table.ColumnDefinitions)+1)
	if allNS {
		headers = append(headers, "NAMESPACE")
	}
	for _, c := range table.ColumnDefinitions {
		headers = append(headers, strings.ToUpper(c.Name))
	}
	fmt.Fprintln(tw, strings.Join(headers, "\t"))
	for _, row := range table.Rows {
		cells := make([]string, 0, len(row.Cells)+1)
		if allNS {
			ns := ""
			if acc, err := meta.Accessor(row.Object.Object); err == nil {
				ns = acc.GetNamespace()
			}
			cells = append(cells, ns)
		}
		for _, c := range row.Cells {
			cells = append(cells, fmt.Sprintf("%v", c))
		}
		fmt.Fprintln(tw, strings.Join(cells, "\t"))
	}
	return tw.Flush()
}

func printDaemonAgentDetail(out io.Writer, da *clrkv1alpha1.DaemonAgent) error {
	fmt.Fprintf(out, "Kind:         DaemonAgent\n")
	fmt.Fprintf(out, "Name:         %s\n", da.Name)
	fmt.Fprintf(out, "Namespace:    %s\n", da.Namespace)
	fmt.Fprintf(out, "Pool:         %s\n", da.Spec.WorkerPoolRef)
	fmt.Fprintf(out, "Image:        %s\n", da.Spec.Template.Spec.Image)
	if da.Spec.RestartPolicy != "" {
		fmt.Fprintf(out, "Restart:      %s\n", string(da.Spec.RestartPolicy))
	}
	fmt.Fprintf(out, "Age:          %s\n", ageString(da.CreationTimestamp))
	fmt.Fprintln(out, "Status:")
	fmt.Fprintf(out, "  Phase:                       %s\n", defaultDash(string(da.Status.Phase)))
	fmt.Fprintf(out, "  RestartCount:                %d\n", da.Status.RestartCount)
	fmt.Fprintf(out, "  LatestCreatedRevisionName:   %s\n", da.Status.LatestCreatedRevisionName)
	fmt.Fprintf(out, "  LatestReadyRevisionName:     %s\n", da.Status.LatestReadyRevisionName)
	if da.Status.UpSince != nil {
		// UTC to match the old path, which printed the raw JSON timestamp
		// (Kubernetes serializes metav1.Time in UTC with a Z suffix); a bare
		// Format renders in the host's local zone and would drift the output.
		fmt.Fprintf(out, "  UpSince:                     %s\n", da.Status.UpSince.UTC().Format(time.RFC3339))
	}
	printEgressRefs(out, da.Spec.EgressRefs)
	printConditions(out, da.Status.Conditions)
	return nil
}

func printTaskAgentDetail(out io.Writer, ta *clrkv1alpha1.TaskAgent) error {
	fmt.Fprintf(out, "Kind:         TaskAgent\n")
	fmt.Fprintf(out, "Name:         %s\n", ta.Name)
	fmt.Fprintf(out, "Namespace:    %s\n", ta.Namespace)
	fmt.Fprintf(out, "Pool:         %s\n", ta.Spec.WorkerPoolRef)
	fmt.Fprintf(out, "Image:        %s\n", ta.Spec.Template.Spec.Image)
	if ta.Spec.Schedule != nil && *ta.Spec.Schedule != "" {
		fmt.Fprintf(out, "Schedule:     %s\n", *ta.Spec.Schedule)
	}
	fmt.Fprintf(out, "Age:          %s\n", ageString(ta.CreationTimestamp))
	fmt.Fprintln(out, "Status:")
	fmt.Fprintf(out, "  ActiveExecutions:            %d\n", ta.Status.ActiveExecutions)
	fmt.Fprintf(out, "  WarmSandboxes:               %d\n", ta.Status.WarmSandboxes)
	fmt.Fprintf(out, "  LatestCreatedRevisionName:   %s\n", ta.Status.LatestCreatedRevisionName)
	fmt.Fprintf(out, "  LatestReadyRevisionName:     %s\n", ta.Status.LatestReadyRevisionName)
	printEgressRefs(out, ta.Spec.EgressRefs)
	printConditions(out, ta.Status.Conditions)
	return nil
}

func printEgressRefs(out io.Writer, refs []clrkv1alpha1.AgentEgressRef) {
	if len(refs) == 0 {
		return
	}
	fmt.Fprintln(out, "EgressRefs:")
	for _, r := range refs {
		fmt.Fprintf(out, "  - %s\n", r.GatewayRef)
	}
}

func printConditions(out io.Writer, conds []metav1.Condition) {
	if len(conds) == 0 {
		return
	}
	fmt.Fprintln(out, "Conditions:")
	tw := newTableWriter(out)
	fmt.Fprintln(tw, "  TYPE\tSTATUS\tREASON\tMESSAGE")
	for _, c := range conds {
		fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\n", c.Type, string(c.Status), c.Reason, c.Message)
	}
	tw.Flush()
}

// sortByNamespaceName orders typed list items by (namespace, name) for stable
// output, matching the order the dynamic-client list path used to produce. The
// pointer constraint lets it read the promoted ObjectMeta accessors off value
// elements.
func sortByNamespaceName[T any, PT interface {
	*T
	metav1.Object
}](items []T) {
	sort.SliceStable(items, func(i, j int) bool {
		a, b := PT(&items[i]), PT(&items[j])
		if a.GetNamespace() != b.GetNamespace() {
			return a.GetNamespace() < b.GetNamespace()
		}
		return a.GetName() < b.GetName()
	})
}

func newTableWriter(out io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
}

func defaultDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// ageString renders an object's age (human duration since creation) for the
// detail views; the table column comes from the API package's ConvertToTable.
func ageString(t metav1.Time) string {
	if t.IsZero() {
		return "<unknown>"
	}
	return duration.HumanDuration(time.Since(t.Time))
}

// ageOf is the unstructured-input age helper still used by the REST-backed
// invocations command, which reads the per-parent subresource as unstructured.
func ageOf(item unstructured.Unstructured) string {
	return ageString(item.GetCreationTimestamp())
}

func namespaceMsg(prefix, ns string, allNS bool) string {
	if allNS {
		return prefix + " (all namespaces)."
	}
	return fmt.Sprintf("%s in namespace %q.", prefix, ns)
}

func addReadFlags(cmd *cobra.Command, namespace *string, allNamespaces *bool) {
	cmd.Flags().StringVarP(namespace, "namespace", "n", "", "Target namespace (default: kubeconfig context).")
	if allNamespaces != nil {
		cmd.Flags().BoolVarP(allNamespaces, "all-namespaces", "A", false, "List across all namespaces.")
	}
}
