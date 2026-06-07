package cmd

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var workerPoolGVR = schema.GroupVersionResource{Group: "clrk.apoxy.dev", Version: "v1alpha1", Resource: "workerpools"}

// newPoolsCmd is `clrk pools` with `list` (default) and `get` subcommands.
func newPoolsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "pools",
		Aliases: []string{"pool"},
		Short:   "List and inspect WorkerPools",
	}
	cmd.AddCommand(newPoolsListCmd())
	cmd.AddCommand(newPoolsGetCmd())
	return cmd
}

func newPoolsListCmd() *cobra.Command {
	var (
		namespace     string
		allNamespaces bool
	)
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List WorkerPools",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			_, dyn, ns, err := kube.clients(namespace, allNamespaces)
			if err != nil {
				return err
			}
			items, err := listResource(cmd.Context(), dyn, workerPoolGVR, ns, allNamespaces)
			if err != nil {
				return fmt.Errorf("listing WorkerPools: %w", err)
			}
			if len(items) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), namespaceMsg("No WorkerPools found", ns, allNamespaces))
				return nil
			}
			printWorkerPoolTable(cmd.OutOrStdout(), items, allNamespaces)
			return nil
		},
	}
	addReadFlags(cmd, &namespace, &allNamespaces)
	return cmd
}

func newPoolsGetCmd() *cobra.Command {
	var namespace string
	cmd := &cobra.Command{
		Use:   "get NAME",
		Short: "Show details for a single WorkerPool",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, dyn, ns, err := kube.clients(namespace, false)
			if err != nil {
				return err
			}
			wp, err := dyn.Resource(workerPoolGVR).Namespace(ns).Get(cmd.Context(), args[0], metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("getting WorkerPool %s/%s: %w", ns, args[0], err)
			}
			return printWorkerPoolDetail(cmd.OutOrStdout(), wp)
		},
	}
	addReadFlags(cmd, &namespace, nil)
	return cmd
}

func printWorkerPoolTable(out io.Writer, items []unstructured.Unstructured, allNS bool) {
	tw := newTableWriter(out)
	headers := []string{"NAME", "REPLICAS", "READY", "ACTIVE", "AGE"}
	if allNS {
		headers = append([]string{"NAMESPACE"}, headers...)
	}
	fmt.Fprintln(tw, strings.Join(headers, "\t"))
	for _, item := range items {
		spec := nestedMap(item.Object, "spec")
		status := nestedMap(item.Object, "status")
		row := []string{
			item.GetName(),
			intStr(spec, "replicas"),
			intStr(status, "readyReplicas"),
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

func printWorkerPoolDetail(out io.Writer, wp *unstructured.Unstructured) error {
	spec := nestedMap(wp.Object, "spec")
	status := nestedMap(wp.Object, "status")
	capacity := nestedMap(status, "capacity")
	fmt.Fprintf(out, "Kind:         WorkerPool\n")
	fmt.Fprintf(out, "Name:         %s\n", wp.GetName())
	fmt.Fprintf(out, "Namespace:    %s\n", wp.GetNamespace())
	fmt.Fprintf(out, "Replicas:     %s desired / %s ready\n", intStr(spec, "replicas"), intStr(status, "readyReplicas"))
	if v := intStr(spec, "maxExecutionsPerWorker"); v != "0" {
		fmt.Fprintf(out, "Max/Worker:   %s\n", v)
	}
	if v := intStr(spec, "warmPool"); v != "0" {
		fmt.Fprintf(out, "WarmPool:     %s\n", v)
	}
	fmt.Fprintf(out, "Age:          %s\n", ageOf(*wp))
	fmt.Fprintln(out, "Status:")
	fmt.Fprintf(out, "  ActiveExecutions:    %s\n", intStr(status, "activeExecutions"))
	if len(capacity) > 0 {
		fmt.Fprintf(out, "  Capacity:            %s available / %s max\n",
			intStr(capacity, "availableExecutions"), intStr(capacity, "maxExecutions"))
	}
	printConditions(out, status)
	return nil
}
