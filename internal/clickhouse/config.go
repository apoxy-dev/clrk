package clickhouse

import (
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"text/template"
)

//go:embed config.yaml
var configYAMLRaw string

//go:embed users.yaml
var usersYAMLRaw string

// configTmpl is the embedded engine's main config template, parsed
// once at startup. Substitutions:
//
//	{{.DataDir}}   -> on-disk path (no trailing slash)
//	{{.HTTPPort}}  -> ClickHouse HTTP port (8123)
//	{{.NativePort}} -> ClickHouse native TCP port (9000)
//
// The listener binds 127.0.0.1 only (hard-coded in config.yaml): the
// embedded engine is private to the cm process. Other pods (worker)
// talk to cm via the existing gRPC surface; cm writes to its local
// CH via ch-go on loopback.
var configTmpl = template.Must(template.New("clickhouse-config").Parse(configYAMLRaw))

// writeConfig renders the embedded engine's config.yaml + users.yaml
// into configDir. dataDir is the on-disk path the engine uses for
// MergeTree parts; threaded into the template so the embedded YAML
// has no hard-coded /var/lib/clickhouse.
func writeConfig(configDir, dataDir string) error {
	cfgPath := filepath.Join(configDir, "config.yaml")
	cfgFile, err := os.OpenFile(cfgPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create %s: %w", cfgPath, err)
	}
	defer cfgFile.Close()
	if err := configTmpl.Execute(cfgFile, struct {
		DataDir    string
		HTTPPort   int
		NativePort int
	}{
		DataDir:    dataDir,
		HTTPPort:   HTTPPort,
		NativePort: NativePort,
	}); err != nil {
		return fmt.Errorf("render %s: %w", cfgPath, err)
	}

	usersPath := filepath.Join(configDir, "users.yaml")
	if err := os.WriteFile(usersPath, []byte(usersYAMLRaw), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", usersPath, err)
	}
	return nil
}
