package cmd

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// newAgentsInvocationsCmd is `clrk agents invocations <name>` — lists the
// lifecycle records for one TaskAgent, newest first. Reads the per-parent
// subresource (taskagents/<name>/invocations) so scoping happens
// server-side against the ClickHouse-backed read model rather than by
// fetching every invocation in the namespace and filtering client-side.
func newAgentsInvocationsCmd() *cobra.Command {
	var (
		namespace  string
		local      bool
		kubeconfig string
	)
	cmd := &cobra.Command{
		Use:     "invocations NAME",
		Short:   "List Invocation lifecycle records for a TaskAgent",
		Aliases: []string{"invs", "inv"},
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			kc, err := resolveKubeconfig(kubeconfig, local)
			if err != nil {
				return err
			}
			cfg, err := clientcmd.BuildConfigFromFlags("", kc)
			if err != nil {
				return fmt.Errorf("loading kubeconfig %s: %w", kc, err)
			}
			ns := namespace
			if ns == "" {
				if ns, err = contextNamespace(kc); err != nil {
					return err
				}
			}
			return listInvocations(cmd.Context(), cmd.OutOrStdout(), cfg, ns, args[0])
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Target namespace (default: kubeconfig context).")
	cmd.Flags().BoolVar(&local, "local", false, "Target the kubeconfig of the running 'clrk dev' session (~/.clrk/kubeconfig.host).")
	cmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "Explicit kubeconfig path (takes precedence over --local and $KUBECONFIG).")
	return cmd
}

func listInvocations(ctx context.Context, out io.Writer, cfg *rest.Config, ns, name string) error {
	rc, err := clrkRESTClient(cfg)
	if err != nil {
		return err
	}
	// taskagents/<name>/invocations — the per-parent read subresource. The
	// request builder assembles the group/version/namespace/resource path,
	// so the only literal is the subresource name.
	raw, err := rc.Get().
		Namespace(ns).
		Resource(taskAgentGVR.Resource).
		Name(name).
		SubResource("invocations").
		DoRaw(ctx)
	if err != nil {
		return fmt.Errorf("listing invocations for %s/%s: %w", ns, name, err)
	}
	var list unstructured.UnstructuredList
	if err := list.UnmarshalJSON(raw); err != nil {
		return fmt.Errorf("decoding invocation list: %w", err)
	}
	if len(list.Items) == 0 {
		fmt.Fprintf(out, "No invocations for TaskAgent %q in namespace %q.\n", name, ns)
		return nil
	}
	tw := newTableWriter(out)
	fmt.Fprintln(tw, "INVOCATION\tPHASE\tTRIGGER\tAGE")
	for _, item := range list.Items {
		phase, _, _ := unstructured.NestedString(item.Object, "status", "phase")
		trigger, _, _ := unstructured.NestedString(item.Object, "spec", "trigger", "type")
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
			item.GetName(), defaultDash(phase), defaultDash(trigger), ageOf(item))
	}
	return tw.Flush()
}

// clrkRESTClient builds a REST client bound to the clrk API group for raw
// subresource reads the dynamic client can't express (it has no
// list-subresource verb). DoRaw bypasses decoding, so the clientgo
// serializer is only here to satisfy RESTClientFor — it never sees a
// clrk type.
func clrkRESTClient(cfg *rest.Config) (rest.Interface, error) {
	c := rest.CopyConfig(cfg)
	c.GroupVersion = &schema.GroupVersion{Group: "clrk.apoxy.dev", Version: "v1alpha1"}
	c.APIPath = "/apis"
	c.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	rc, err := rest.RESTClientFor(c)
	if err != nil {
		return nil, fmt.Errorf("rest client: %w", err)
	}
	return rc, nil
}
