package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	sigsyaml "sigs.k8s.io/yaml"
)

// devContextName is the well-known context auto-managed by `clrk dev`.
// On bring-up we upsert it pointing at the live host kubeconfig and
// promote it to current so subsequent `clrk apply` / `clrk agents`
// invocations "just work" without --local.
const devContextName = "dev"

// clrkConfig is the on-disk shape of ~/.clrk/config — a kubectl-style
// list of named cluster contexts plus a pointer to the current one.
// resolveKubeconfig falls through to the current context's kubeconfig
// when no flag/env is set, replacing the previous "fail with no
// kubeconfig" terminal state.
type clrkConfig struct {
	CurrentContext string        `json:"current-context,omitempty"`
	Contexts       []clrkContext `json:"contexts,omitempty"`
}

// clrkContext binds a name to a kubeconfig path. Future fields
// (apiserver URL, API key env, etc.) will land here as new transports
// get CLI-side support; the file format is intentionally minimal
// today so we don't ship dead validation paths.
type clrkContext struct {
	Name       string `json:"name"`
	Kubeconfig string `json:"kubeconfig,omitempty"`
}

func configPath() string {
	return filepath.Join(clrkDir, "config")
}

// loadConfig reads ~/.clrk/config. A missing file is normal — every
// pre-existing install lacks one — and yields an empty config + nil
// error so the caller can keep walking the resolution chain.
func loadConfig() (*clrkConfig, error) {
	body, err := os.ReadFile(configPath())
	if err != nil {
		if os.IsNotExist(err) {
			return &clrkConfig{}, nil
		}
		return nil, fmt.Errorf("reading %s: %w", configPath(), err)
	}
	var cfg clrkConfig
	if err := sigsyaml.Unmarshal(body, &cfg); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", configPath(), err)
	}
	return &cfg, nil
}

// saveConfig writes the config atomically (tmp + rename) with 0600
// perms — kubeconfig paths can leak cluster topology and an API key
// will live next to them as soon as a hosted-mode field lands.
func saveConfig(cfg *clrkConfig) error {
	body, err := sigsyaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshalling config: %w", err)
	}
	if err := os.MkdirAll(clrkDir, 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", clrkDir, err)
	}
	path := configPath()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o600); err != nil {
		return fmt.Errorf("writing %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("renaming %s -> %s: %w", tmp, path, err)
	}
	return nil
}

func (c *clrkConfig) findContext(name string) *clrkContext {
	for i := range c.Contexts {
		if c.Contexts[i].Name == name {
			return &c.Contexts[i]
		}
	}
	return nil
}

// upsertContext inserts or replaces a context by name. Returns true
// when an existing entry was overwritten, false on insert.
func (c *clrkConfig) upsertContext(ctx clrkContext) bool {
	for i := range c.Contexts {
		if c.Contexts[i].Name == ctx.Name {
			c.Contexts[i] = ctx
			return true
		}
	}
	c.Contexts = append(c.Contexts, ctx)
	return false
}

// currentKubeconfig returns the kubeconfig path of the current context
// with `~` expansion. ("", nil) when no current context is set —
// caller decides whether that's an error.
func (c *clrkConfig) currentKubeconfig() (string, error) {
	if c.CurrentContext == "" {
		return "", nil
	}
	cur := c.findContext(c.CurrentContext)
	if cur == nil {
		return "", fmt.Errorf("current-context %q not found in %s", c.CurrentContext, configPath())
	}
	return expandTilde(cur.Kubeconfig)
}

// expandTilde swaps a leading `~/` for the user's home dir. Other
// shapes (`~user`, mid-string `~`) pass through — keeps semantics
// predictable and matches what kubectl does.
func expandTilde(p string) (string, error) {
	if p == "" || p[0] != '~' {
		return p, nil
	}
	if len(p) > 1 && p[1] != '/' && p[1] != filepath.Separator {
		return p, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, p[1:]), nil
}

// writeDevContext upserts the dev context to point at kubeconfigPath
// and promotes it to current. Called by `clrk dev` after k3s comes
// up so subsequent CLI invocations target the live dev cluster.
// Best-effort by design — bring-up logs a warning on failure rather
// than rolling back.
func writeDevContext(kubeconfigPath string) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	cfg.upsertContext(clrkContext{Name: devContextName, Kubeconfig: kubeconfigPath})
	cfg.CurrentContext = devContextName
	return saveConfig(cfg)
}
