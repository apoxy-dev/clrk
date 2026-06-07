package cmd

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

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
			cs, ns, err := kube.clrkClient(namespace, allNamespaces)
			if err != nil {
				return err
			}
			list, err := cs.ClrkV1alpha1().WorkerPools(ns).List(cmd.Context(), metav1.ListOptions{})
			if err != nil {
				return fmt.Errorf("listing WorkerPools: %w", err)
			}
			sortByNamespaceName(list.Items)
			if len(list.Items) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), namespaceMsg("No WorkerPools found", ns, allNamespaces))
				return nil
			}
			return convertAndPrint(cmd.Context(), cmd.OutOrStdout(), list, allNamespaces)
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
			cs, ns, err := kube.clrkClient(namespace, false)
			if err != nil {
				return err
			}
			wp, err := cs.ClrkV1alpha1().WorkerPools(ns).Get(cmd.Context(), args[0], metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("getting WorkerPool %s/%s: %w", ns, args[0], err)
			}
			return printWorkerPoolDetail(cmd.OutOrStdout(), wp)
		},
	}
	addReadFlags(cmd, &namespace, nil)
	return cmd
}

func printWorkerPoolDetail(out io.Writer, wp *clrkv1alpha1.WorkerPool) error {
	fmt.Fprintf(out, "Kind:         WorkerPool\n")
	fmt.Fprintf(out, "Name:         %s\n", wp.Name)
	fmt.Fprintf(out, "Namespace:    %s\n", wp.Namespace)
	fmt.Fprintf(out, "Replicas:     %d desired / %d ready\n", workerPoolReplicas(wp), wp.Status.ReadyReplicas)
	if wp.Spec.MaxExecutionsPerWorker != nil && *wp.Spec.MaxExecutionsPerWorker != 0 {
		fmt.Fprintf(out, "Max/Worker:   %d\n", *wp.Spec.MaxExecutionsPerWorker)
	}
	if wp.Spec.WarmPool != nil && *wp.Spec.WarmPool != 0 {
		fmt.Fprintf(out, "WarmPool:     %d\n", *wp.Spec.WarmPool)
	}
	fmt.Fprintf(out, "Age:          %s\n", ageString(wp.CreationTimestamp))
	fmt.Fprintln(out, "Status:")
	fmt.Fprintf(out, "  ActiveExecutions:    %d\n", wp.Status.ActiveExecutions)
	capacity := wp.Status.Capacity
	if capacity.MaxExecutions != 0 || capacity.AvailableExecutions != 0 {
		fmt.Fprintf(out, "  Capacity:            %d available / %d max\n",
			capacity.AvailableExecutions, capacity.MaxExecutions)
	}
	printConditions(out, wp.Status.Conditions)
	return nil
}

// workerPoolReplicas returns the desired replica count, treating an unset
// (nil) value as 0 to match the historical column output.
func workerPoolReplicas(wp *clrkv1alpha1.WorkerPool) int32 {
	if wp.Spec.Replicas != nil {
		return *wp.Spec.Replicas
	}
	return 0
}
