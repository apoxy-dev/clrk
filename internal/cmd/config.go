package cmd

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

// newConfigCmd is `clrk config` — kubectl-style management of cluster
// contexts under ~/.clrk/config. The current context is the
// fall-through target for every command that resolves a kubeconfig
// (apply, agents, agents logs, pools, secret).
func newConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage clrk cluster contexts at ~/.clrk/config",
	}
	cmd.AddCommand(newConfigGetContextsCmd())
	cmd.AddCommand(newConfigCurrentContextCmd())
	cmd.AddCommand(newConfigUseContextCmd())
	cmd.AddCommand(newConfigSetContextCmd())
	return cmd
}

func newConfigGetContextsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get-contexts",
		Short: "List configured contexts",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			if len(cfg.Contexts) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(),
					"No contexts configured. Try `clrk config set-context dev "+
						"--kubeconfig ~/.clrk/kubeconfig.host` "+
						"(or run `clrk dev`, which auto-creates one).")
				return nil
			}
			printContextsTable(cmd.OutOrStdout(), cfg)
			return nil
		},
	}
}

func newConfigCurrentContextCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "current-context",
		Short: "Print the name of the currently selected context",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			if cfg.CurrentContext == "" {
				return fmt.Errorf("no current context set")
			}
			fmt.Fprintln(cmd.OutOrStdout(), cfg.CurrentContext)
			return nil
		},
	}
}

func newConfigUseContextCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "use-context NAME",
		Short: "Set the current context",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			if cfg.findContext(args[0]) == nil {
				return fmt.Errorf("no context named %q (use `clrk config get-contexts` to list)", args[0])
			}
			cfg.CurrentContext = args[0]
			if err := saveConfig(cfg); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Switched to context %q.\n", args[0])
			return nil
		},
	}
}

func newConfigSetContextCmd() *cobra.Command {
	var kubeconfig string
	cmd := &cobra.Command{
		Use:   "set-context NAME",
		Short: "Add or update a context",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if kubeconfig == "" {
				return fmt.Errorf("nothing to set: pass --kubeconfig")
			}
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			entry := clrkContext{Name: args[0], Kubeconfig: kubeconfig}
			replaced := cfg.upsertContext(entry)
			// First context becomes current automatically — same UX
			// kubectl uses on a brand-new config.
			if cfg.CurrentContext == "" {
				cfg.CurrentContext = entry.Name
			}
			if err := saveConfig(cfg); err != nil {
				return err
			}
			verb := "Added"
			if replaced {
				verb = "Updated"
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s context %q.\n", verb, args[0])
			return nil
		},
	}
	cmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "Kubeconfig file path for this context.")
	return cmd
}

func printContextsTable(out io.Writer, cfg *clrkConfig) {
	tw := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
	fmt.Fprintln(tw, "CURRENT\tNAME\tKUBECONFIG")
	for _, c := range cfg.Contexts {
		marker := ""
		if c.Name == cfg.CurrentContext {
			marker = "*"
		}
		kc := c.Kubeconfig
		if kc == "" {
			kc = "-"
		}
		fmt.Fprintln(tw, strings.Join([]string{marker, c.Name, kc}, "\t"))
	}
	tw.Flush()
}
