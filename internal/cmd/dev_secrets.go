package cmd

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
)

// secretSpec describes one Secret to materialize from sources outside
// the manifest tree (env vars today; --from-literal / --from-file for the
// standalone command). Multiple specs targeting the same name+namespace
// are merged into a single Secret with all keys present.
type secretSpec struct {
	Name      string
	Namespace string
	Key       string // data key inside the Secret
	Value     []byte // resolved bytes (base64-encoded into data[key])
	EnvVar    string // for diagnostics; empty when --from-literal/--from-file
}

// parseDevSecretFlag accepts the `clrk dev --secret name=ENVVAR[:key]`
// shape. Defaults the key to envvar lowercased with `_` → `-` so
// `ANTHROPIC_API_KEY` becomes `anthropic-api-key`. Resolves the env var
// at parse time and fails loudly when missing — secrets are intent.
func parseDevSecretFlag(raw string) (secretSpec, error) {
	idx := strings.IndexByte(raw, '=')
	if idx <= 0 {
		return secretSpec{}, fmt.Errorf("--secret %q: expected name=ENVVAR[:key]", raw)
	}
	name := strings.TrimSpace(raw[:idx])
	rest := strings.TrimSpace(raw[idx+1:])
	if name == "" || rest == "" {
		return secretSpec{}, fmt.Errorf("--secret %q: empty name or envvar", raw)
	}
	envVar, key := rest, ""
	if colon := strings.IndexByte(rest, ':'); colon >= 0 {
		envVar = strings.TrimSpace(rest[:colon])
		key = strings.TrimSpace(rest[colon+1:])
	}
	if envVar == "" {
		return secretSpec{}, fmt.Errorf("--secret %q: empty envvar", raw)
	}
	if key == "" {
		key = strings.ToLower(strings.ReplaceAll(envVar, "_", "-"))
	}
	val, ok := os.LookupEnv(envVar)
	if !ok {
		return secretSpec{}, fmt.Errorf("--secret %q: env var %s is not set", raw, envVar)
	}
	return secretSpec{
		Name:   name,
		Key:    key,
		Value:  []byte(val),
		EnvVar: envVar,
	}, nil
}

// applySecretSpecs server-side-applies one corev1.Secret per
// (namespace, name) group. Specs sharing a name+namespace are merged
// into one Secret with multiple data keys. The field manager is
// `clrk-dev` (matches applyManifests) so a later --apply pass with the
// same field manager doesn't fight us for ownership of the data block.
func applySecretSpecs(ctx context.Context, cc clientcmd.ClientConfig, specs []secretSpec, defaultNamespace string) error {
	if len(specs) == 0 {
		return nil
	}
	cfg, err := restFromClientConfig(cc)
	if err != nil {
		return err
	}
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}

	type key struct{ ns, name string }
	grouped := map[key][]secretSpec{}
	order := []key{}
	for _, s := range specs {
		ns := s.Namespace
		if ns == "" {
			ns = defaultNamespace
		}
		if ns == "" {
			ns = "default"
		}
		k := key{ns, s.Name}
		if _, seen := grouped[k]; !seen {
			order = append(order, k)
		}
		grouped[k] = append(grouped[k], s)
	}

	gvr := schema.GroupVersionResource{Version: "v1", Resource: "secrets"}
	for _, k := range order {
		data := map[string]interface{}{}
		for _, s := range grouped[k] {
			data[s.Key] = base64.StdEncoding.EncodeToString(s.Value)
		}
		obj := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Secret",
			"metadata": map[string]interface{}{
				"name":      k.name,
				"namespace": k.ns,
			},
			"type": string(corev1.SecretTypeOpaque),
			"data": data,
		}}
		body, err := obj.MarshalJSON()
		if err != nil {
			return fmt.Errorf("marshalling Secret %s/%s: %w", k.ns, k.name, err)
		}
		if _, err := dynClient.Resource(gvr).Namespace(k.ns).Patch(
			ctx, k.name, types.ApplyPatchType, body,
			metav1.PatchOptions{FieldManager: "clrk-dev", Force: ptr.To(true)},
		); err != nil {
			return fmt.Errorf("applying Secret %s/%s: %w", k.ns, k.name, err)
		}
		keys := make([]string, 0, len(data))
		for kk := range data {
			keys = append(keys, kk)
		}
		slog.Info("Applied Secret", "namespace", k.ns, "name", k.name, "keys", strings.Join(keys, ","))
	}
	return nil
}
