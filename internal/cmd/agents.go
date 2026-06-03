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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/duration"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

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
		local         bool
		kubeconfig    string
	)
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List DaemonAgents and TaskAgents",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			dyn, ns, err := agentsClient(kubeconfig, local, namespace, allNamespaces)
			if err != nil {
				return err
			}
			return listAgents(cmd.Context(), cmd.OutOrStdout(), dyn, ns, allNamespaces)
		},
	}
	addReadFlags(cmd, &namespace, &allNamespaces, &local, &kubeconfig)
	return cmd
}

func newAgentsGetCmd() *cobra.Command {
	var (
		namespace  string
		local      bool
		kubeconfig string
	)
	cmd := &cobra.Command{
		Use:   "get NAME",
		Short: "Show details for a single DaemonAgent or TaskAgent",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dyn, ns, err := agentsClient(kubeconfig, local, namespace, false)
			if err != nil {
				return err
			}
			return getAgent(cmd.Context(), cmd.OutOrStdout(), dyn, ns, args[0])
		},
	}
	addReadFlags(cmd, &namespace, nil, &local, &kubeconfig)
	return cmd
}

func listAgents(ctx context.Context, out io.Writer, dyn dynamic.Interface, ns string, allNS bool) error {
	das, err := listResource(ctx, dyn, daemonAgentGVR, ns, allNS)
	if err != nil {
		return fmt.Errorf("listing DaemonAgents: %w", err)
	}
	tas, err := listResource(ctx, dyn, taskAgentGVR, ns, allNS)
	if err != nil {
		return fmt.Errorf("listing TaskAgents: %w", err)
	}
	if len(das) == 0 && len(tas) == 0 {
		fmt.Fprintln(out, namespaceMsg("No agents found", ns, allNS))
		return nil
	}
	if len(das) > 0 {
		fmt.Fprintln(out, "DAEMONAGENTS")
		printDaemonAgentTable(out, das, allNS)
	}
	if len(tas) > 0 {
		if len(das) > 0 {
			fmt.Fprintln(out)
		}
		fmt.Fprintln(out, "TASKAGENTS")
		printTaskAgentTable(out, tas, allNS)
	}
	return nil
}

func getAgent(ctx context.Context, out io.Writer, dyn dynamic.Interface, ns, name string) error {
	da, daErr := dyn.Resource(daemonAgentGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	ta, taErr := dyn.Resource(taskAgentGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
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

func printDaemonAgentTable(out io.Writer, items []unstructured.Unstructured, allNS bool) {
	tw := newTableWriter(out)
	headers := []string{"NAME", "POOL", "LATEST READY", "PHASE", "RESTARTS", "AGE"}
	if allNS {
		headers = append([]string{"NAMESPACE"}, headers...)
	}
	fmt.Fprintln(tw, strings.Join(headers, "\t"))
	for _, item := range items {
		spec := nestedMap(item.Object, "spec")
		status := nestedMap(item.Object, "status")
		row := []string{
			item.GetName(),
			stringField(spec, "workerPoolRef"),
			stringField(status, "latestReadyRevisionName"),
			defaultDash(stringField(status, "phase")),
			intStr(status, "restartCount"),
			ageOf(item),
		}
		if allNS {
			row = append([]string{item.GetNamespace()}, row...)
		}
		fmt.Fprintln(tw, strings.Join(row, "\t"))
	}
	tw.Flush()
}

func printTaskAgentTable(out io.Writer, items []unstructured.Unstructured, allNS bool) {
	tw := newTableWriter(out)
	headers := []string{"NAME", "POOL", "LATEST READY", "ACTIVE", "AGE"}
	if allNS {
		headers = append([]string{"NAMESPACE"}, headers...)
	}
	fmt.Fprintln(tw, strings.Join(headers, "\t"))
	for _, item := range items {
		spec := nestedMap(item.Object, "spec")
		status := nestedMap(item.Object, "status")
		row := []string{
			item.GetName(),
			stringField(spec, "workerPoolRef"),
			stringField(status, "latestReadyRevisionName"),
			intStr(status, "activeExecutions"),
			ageOf(item),
		}
		if allNS {
			row = append([]string{item.GetNamespace()}, row...)
		}
		fmt.Fprintln(tw, strings.Join(row, "\t"))
	}
	tw.Flush()
}

func printDaemonAgentDetail(out io.Writer, da *unstructured.Unstructured) error {
	spec := nestedMap(da.Object, "spec")
	status := nestedMap(da.Object, "status")
	template := nestedMap(spec, "template", "spec")
	fmt.Fprintf(out, "Kind:         DaemonAgent\n")
	fmt.Fprintf(out, "Name:         %s\n", da.GetName())
	fmt.Fprintf(out, "Namespace:    %s\n", da.GetNamespace())
	fmt.Fprintf(out, "Pool:         %s\n", stringField(spec, "workerPoolRef"))
	fmt.Fprintf(out, "Image:        %s\n", stringField(template, "image"))
	if rp := stringField(spec, "restartPolicy"); rp != "" {
		fmt.Fprintf(out, "Restart:      %s\n", rp)
	}
	fmt.Fprintf(out, "Age:          %s\n", ageOf(*da))
	fmt.Fprintln(out, "Status:")
	fmt.Fprintf(out, "  Phase:                       %s\n", defaultDash(stringField(status, "phase")))
	fmt.Fprintf(out, "  RestartCount:                %s\n", intStr(status, "restartCount"))
	fmt.Fprintf(out, "  LatestCreatedRevisionName:   %s\n", stringField(status, "latestCreatedRevisionName"))
	fmt.Fprintf(out, "  LatestReadyRevisionName:     %s\n", stringField(status, "latestReadyRevisionName"))
	if up := stringField(status, "upSince"); up != "" {
		fmt.Fprintf(out, "  UpSince:                     %s\n", up)
	}
	printEgressRefs(out, spec)
	printConditions(out, status)
	return nil
}

func printTaskAgentDetail(out io.Writer, ta *unstructured.Unstructured) error {
	spec := nestedMap(ta.Object, "spec")
	status := nestedMap(ta.Object, "status")
	template := nestedMap(spec, "template", "spec")
	fmt.Fprintf(out, "Kind:         TaskAgent\n")
	fmt.Fprintf(out, "Name:         %s\n", ta.GetName())
	fmt.Fprintf(out, "Namespace:    %s\n", ta.GetNamespace())
	fmt.Fprintf(out, "Pool:         %s\n", stringField(spec, "workerPoolRef"))
	fmt.Fprintf(out, "Image:        %s\n", stringField(template, "image"))
	if sched := stringField(spec, "schedule"); sched != "" {
		fmt.Fprintf(out, "Schedule:     %s\n", sched)
	}
	fmt.Fprintf(out, "Age:          %s\n", ageOf(*ta))
	fmt.Fprintln(out, "Status:")
	fmt.Fprintf(out, "  ActiveExecutions:            %s\n", intStr(status, "activeExecutions"))
	fmt.Fprintf(out, "  LatestCreatedRevisionName:   %s\n", stringField(status, "latestCreatedRevisionName"))
	fmt.Fprintf(out, "  LatestReadyRevisionName:     %s\n", stringField(status, "latestReadyRevisionName"))
	printEgressRefs(out, spec)
	printConditions(out, status)
	return nil
}

func printEgressRefs(out io.Writer, spec map[string]interface{}) {
	refs, _, _ := unstructured.NestedSlice(spec, "egressRefs")
	if len(refs) == 0 {
		return
	}
	fmt.Fprintln(out, "EgressRefs:")
	for _, raw := range refs {
		m, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		fmt.Fprintf(out, "  - %s\n", stringField(m, "gatewayRef"))
	}
}

func printConditions(out io.Writer, status map[string]interface{}) {
	conds, _, _ := unstructured.NestedSlice(status, "conditions")
	if len(conds) == 0 {
		return
	}
	fmt.Fprintln(out, "Conditions:")
	tw := newTableWriter(out)
	fmt.Fprintln(tw, "  TYPE\tSTATUS\tREASON\tMESSAGE")
	for _, raw := range conds {
		m, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\n",
			stringField(m, "type"),
			stringField(m, "status"),
			stringField(m, "reason"),
			stringField(m, "message"),
		)
	}
	tw.Flush()
}

func agentsClient(kubeconfig string, local bool, namespace string, allNamespaces bool) (dynamic.Interface, string, error) {
	kc, err := resolveKubeconfig(kubeconfig, local)
	if err != nil {
		return nil, "", err
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kc)
	if err != nil {
		return nil, "", fmt.Errorf("loading kubeconfig %s: %w", kc, err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, "", fmt.Errorf("dynamic client: %w", err)
	}
	if allNamespaces {
		return dyn, "", nil
	}
	if namespace != "" {
		return dyn, namespace, nil
	}
	ns, err := contextNamespace(kc)
	if err != nil {
		return nil, "", err
	}
	return dyn, ns, nil
}

func listResource(ctx context.Context, dyn dynamic.Interface, gvr schema.GroupVersionResource, ns string, allNS bool) ([]unstructured.Unstructured, error) {
	var (
		list *unstructured.UnstructuredList
		err  error
	)
	if allNS {
		list, err = dyn.Resource(gvr).List(ctx, metav1.ListOptions{})
	} else {
		list, err = dyn.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
	}
	if err != nil {
		return nil, err
	}
	items := list.Items
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].GetNamespace() != items[j].GetNamespace() {
			return items[i].GetNamespace() < items[j].GetNamespace()
		}
		return items[i].GetName() < items[j].GetName()
	})
	return items, nil
}

func newTableWriter(out io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
}

func nestedMap(obj map[string]interface{}, fields ...string) map[string]interface{} {
	m, _, _ := unstructured.NestedMap(obj, fields...)
	if m == nil {
		return map[string]interface{}{}
	}
	return m
}

func stringField(m map[string]interface{}, key string) string {
	v, _, _ := unstructured.NestedString(m, key)
	return v
}

func intStr(m map[string]interface{}, key string) string {
	v, ok, _ := unstructured.NestedInt64(m, key)
	if !ok {
		return "0"
	}
	return fmt.Sprintf("%d", v)
}

func defaultDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func ageOf(item unstructured.Unstructured) string {
	ct := item.GetCreationTimestamp()
	if ct.IsZero() {
		return "<unknown>"
	}
	return duration.HumanDuration(time.Since(ct.Time))
}

func namespaceMsg(prefix, ns string, allNS bool) string {
	if allNS {
		return prefix + " (all namespaces)."
	}
	return fmt.Sprintf("%s in namespace %q.", prefix, ns)
}

func addReadFlags(cmd *cobra.Command, namespace *string, allNamespaces *bool, local *bool, kubeconfig *string) {
	cmd.Flags().StringVarP(namespace, "namespace", "n", "", "Target namespace (default: kubeconfig context).")
	if allNamespaces != nil {
		cmd.Flags().BoolVarP(allNamespaces, "all-namespaces", "A", false, "List across all namespaces.")
	}
	cmd.Flags().BoolVar(local, "local", false, "Target the kubeconfig of the running 'clrk dev' session (~/.clrk/kubeconfig.host).")
	cmd.Flags().StringVar(kubeconfig, "kubeconfig", "", "Explicit kubeconfig path (takes precedence over --local and $KUBECONFIG).")
}
