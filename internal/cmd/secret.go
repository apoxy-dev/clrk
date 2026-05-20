package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

// newSecretCmd is `clrk secret`. Today exposes one subcommand (`set`).
func newSecretCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "secret",
		Short: "Manage Kubernetes Secrets in dev/prod with the same UX",
	}
	cmd.AddCommand(newSecretSetCmd())
	return cmd
}

// newSecretSetCmd implements `clrk secret set NAME [--from-env=KEY=ENVVAR
// --from-literal=KEY=VALUE --from-file=KEY=PATH]...` Server-side-applies
// an Opaque Secret in the resolved namespace using the same kubeconfig
// resolution chain as `clrk apply`.
func newSecretSetCmd() *cobra.Command {
	var (
		fromEnv     []string
		fromLiteral []string
		fromFile    []string
		namespace   string
		local       bool
		kubeconfig  string
	)

	cmd := &cobra.Command{
		Use:   "set NAME",
		Short: "Apply an Opaque Secret with one or more keys",
		Long: "Server-side-applies an Opaque Secret with the supplied data " +
			"keys. Sources are repeatable and merge into the same Secret " +
			"with field manager `clrk-dev`. --local targets ~/.clrk/" +
			"kubeconfig.host (the running clrk dev session); otherwise " +
			"resolution falls back to --kubeconfig and $KUBECONFIG.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := strings.TrimSpace(args[0])
			if name == "" {
				return fmt.Errorf("empty Secret name")
			}
			specs, err := collectSecretSourceFlags(name, fromEnv, fromLiteral, fromFile)
			if err != nil {
				return err
			}
			if len(specs) == 0 {
				return fmt.Errorf("no sources: pass at least one --from-env, --from-literal, or --from-file")
			}
			kc, err := resolveKubeconfig(kubeconfig, local)
			if err != nil {
				return err
			}
			ns := namespace
			if ns == "" {
				ns = "default"
			}
			return applySecretSpecs(cmd.Context(), kc, specs, ns)
		},
	}

	cmd.Flags().StringArrayVar(&fromEnv, "from-env", nil, "key=ENVVAR — read the value of the named environment variable into the Secret under data[key]. Repeatable.")
	cmd.Flags().StringArrayVar(&fromLiteral, "from-literal", nil, "key=VALUE — verbatim string into data[key]. Repeatable.")
	cmd.Flags().StringArrayVar(&fromFile, "from-file", nil, "key=PATH — file contents into data[key]. Repeatable.")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Target namespace (default: \"default\").")
	cmd.Flags().BoolVar(&local, "local", false, "Target the kubeconfig of the running 'clrk dev' session (~/.clrk/kubeconfig.host).")
	cmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "Explicit kubeconfig path (takes precedence over --local and $KUBECONFIG).")
	return cmd
}

// collectSecretSourceFlags fans the three source flags out into one
// secretSpec per data key. The standalone command never uses the EnvVar
// hint for `--from-literal`/`--from-file`; only `--from-env` carries it
// (used by parseDevSecretFlag too, for diagnostics).
func collectSecretSourceFlags(name string, fromEnv, fromLiteral, fromFile []string) ([]secretSpec, error) {
	var out []secretSpec
	for _, raw := range fromEnv {
		s, err := parseSecretSourceEnv(name, raw)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	for _, raw := range fromLiteral {
		s, err := parseSecretSourceLiteral(name, raw)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	for _, raw := range fromFile {
		s, err := parseSecretSourceFile(name, raw)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func parseSecretSourceEnv(name, raw string) (secretSpec, error) {
	key, rhs, err := splitKVFlag("--from-env", raw)
	if err != nil {
		return secretSpec{}, err
	}
	val, ok := os.LookupEnv(rhs)
	if !ok {
		return secretSpec{}, fmt.Errorf("--from-env %q: env var %s is not set", raw, rhs)
	}
	return secretSpec{Name: name, Key: key, Value: []byte(val), EnvVar: rhs}, nil
}

func parseSecretSourceLiteral(name, raw string) (secretSpec, error) {
	key, val, err := splitKVFlag("--from-literal", raw)
	if err != nil {
		return secretSpec{}, err
	}
	return secretSpec{Name: name, Key: key, Value: []byte(val)}, nil
}

func parseSecretSourceFile(name, raw string) (secretSpec, error) {
	key, path, err := splitKVFlag("--from-file", raw)
	if err != nil {
		return secretSpec{}, err
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return secretSpec{}, fmt.Errorf("--from-file %q: %w", raw, err)
	}
	return secretSpec{Name: name, Key: key, Value: body}, nil
}

func splitKVFlag(flag, raw string) (string, string, error) {
	idx := strings.IndexByte(raw, '=')
	if idx <= 0 {
		return "", "", fmt.Errorf("%s %q: expected KEY=VALUE", flag, raw)
	}
	key := strings.TrimSpace(raw[:idx])
	val := raw[idx+1:]
	if key == "" {
		return "", "", fmt.Errorf("%s %q: empty KEY", flag, raw)
	}
	return key, val, nil
}

