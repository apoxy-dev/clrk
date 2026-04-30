package drivers

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/apoxy-dev/clrk/internal/drivers/dockerutils"
)

const (
	// K3sContainerName is the stable name for the k3s control-plane container.
	K3sContainerName = "clrk-k3s"

	// DefaultK3sImage pins a minor version whose k8s.io/api release matches
	// clrk and apoxy-cloud's module resolution (v0.34.x). Override via
	// WithImage for CI pinning.
	DefaultK3sImage = "rancher/k3s:v1.34.1-k3s1"

	// DefaultK3sAPIPort is the host port the k3s API is optionally
	// published on for kubectl access from the host.
	DefaultK3sAPIPort = 6443

	// KubeconfigFileName is written into the shared data dir so other
	// drivers (and the user) can find the rewritten kubeconfig.
	KubeconfigFileName = "kubeconfig"
)

// K3sDriver runs a single-node k3s control plane in docker so reconcilers
// and workers have a real Kubernetes API to target in `clrk dev`. The
// driver waits for the API to become ready, pulls the in-container
// kubeconfig out, rewrites the server URL to the in-network hostname, and
// writes it to <dataDir>/kubeconfig for other containers to mount.
type K3sDriver struct {
	// DataDir is the host path where the rewritten kubeconfig is written.
	// Must be the same path mounted into the other drivers so they can
	// read it.
	DataDir string
	// HostPort is the host port the k3s API is published on. Zero means
	// no publish (network-internal only).
	HostPort int
}

// NewK3sDriver constructs a driver with sane defaults. dataDir is required
// because the kubeconfig has to land somewhere other drivers can see. The
// API port is NOT published on the host by default — other dev containers
// reach it via the shared docker network, and on developer machines port
// 6443 is commonly held by an unrelated cluster (kind, k3d, etc.). Set
// HostPort explicitly if you want to target clrk's k3s from host kubectl.
func NewK3sDriver(dataDir string) *K3sDriver {
	return &K3sDriver{DataDir: dataDir}
}

// Start launches k3s with extensions disabled (traefik, servicelb,
// metrics-server) to keep the boot tight, waits for the kubeconfig, and
// writes a rewritten copy to <dataDir>/kubeconfig. Returns the container
// name.
func (d *K3sDriver) Start(ctx context.Context, opts ...Option) (string, error) {
	if err := EnsureNetwork(ctx); err != nil {
		return "", err
	}
	if err := dockerutils.RemoveIfExists(ctx, K3sContainerName); err != nil {
		return "", err
	}

	o := Apply(opts...)
	if o.Image == "" {
		o.Image = DefaultK3sImage
	}
	if d.HostPort > 0 {
		if _, ok := o.Ports[d.HostPort]; !ok {
			o.Ports[d.HostPort] = 6443
		}
	}

	// k3s needs a full set of kernel capabilities to manage cgroups and
	// namespaces for workloads, plus the kubelet wants rw access to
	// /sys/fs/cgroup. `--privileged` is the straightforward way; it
	// matches what `k3d` does internally for its loadbalancer-less
	// single-node mode.
	args := []string{
		"run", "--detach",
		"--name", K3sContainerName,
		"--network", NetworkName,
		"--label", ownerLabel,
		"--restart", "on-failure",
		"--privileged",
		// k3s writes /output/kubeconfig if K3S_KUBECONFIG_OUTPUT is set,
		// but we want the standard in-container path so we can docker cp.
		"--env", "K3S_KUBECONFIG_MODE=644",
	}
	for _, k := range sortedStringKeys(o.Labels) {
		args = append(args, "--label", k+"="+o.Labels[k])
	}
	for _, k := range sortedStringKeys(o.Env) {
		args = append(args, "--env", k+"="+o.Env[k])
	}
	for _, k := range sortedStringKeys(o.Volumes) {
		args = append(args, "--volume", k+":"+o.Volumes[k])
	}
	for _, host := range sortedIntKeys(o.Ports) {
		args = append(args, "--publish", fmt.Sprintf("%d:%d", host, o.Ports[host]))
	}

	args = append(args, o.Image,
		"server",
		"--disable=traefik",
		"--disable=servicelb",
		"--disable=metrics-server",
		"--write-kubeconfig-mode=644",
		"--tls-san="+K3sContainerName,
		"--tls-san=localhost",
	)

	if _, err := runDocker(ctx, args...); err != nil {
		return "", fmt.Errorf("starting k3s: %w", err)
	}
	if err := dockerutils.WaitRunning(ctx, K3sContainerName); err != nil {
		return "", err
	}

	if err := d.extractKubeconfig(ctx); err != nil {
		return "", fmt.Errorf("extracting kubeconfig: %w", err)
	}

	return K3sContainerName, nil
}

// Stop removes the k3s container. The kubeconfig file in DataDir is left
// behind so post-hoc `kubectl --kubeconfig=...` still works for
// inspection; clrk dev clears DataDir explicitly on next run if needed.
func (d *K3sDriver) Stop(ctx context.Context) error {
	return dockerutils.RemoveIfExists(ctx, K3sContainerName)
}

// Reload is a no-op — k3s has no bind-mounted binary to hot-swap.
func (d *K3sDriver) Reload(ctx context.Context) error { return nil }

// GetAddr returns the in-network API URL.
func (d *K3sDriver) GetAddr(ctx context.Context) (string, error) {
	return fmt.Sprintf("https://%s:6443", K3sContainerName), nil
}

// KubeconfigPath is the host path to the rewritten kubeconfig.
func (d *K3sDriver) KubeconfigPath() string {
	return filepath.Join(d.DataDir, KubeconfigFileName)
}

// HostKubeconfigPath is the host path to the kubeconfig tests and host
// tools can use to reach k3s via the published port. Only populated when
// HostPort > 0.
func (d *K3sDriver) HostKubeconfigPath() string {
	return filepath.Join(d.DataDir, "kubeconfig.host")
}

// extractKubeconfig polls `docker exec cat /etc/rancher/k3s/k3s.yaml`
// until it succeeds, rewrites the server URL to the in-network hostname,
// and writes the result to <DataDir>/kubeconfig.
func (d *K3sDriver) extractKubeconfig(ctx context.Context) error {
	if err := os.MkdirAll(d.DataDir, 0o755); err != nil {
		return err
	}

	deadline := time.NewTimer(60 * time.Second)
	defer deadline.Stop()

	var raw []byte
	for {
		out, err := exec.CommandContext(ctx, "docker", "exec", K3sContainerName,
			"cat", "/etc/rancher/k3s/k3s.yaml").Output()
		if err == nil && len(bytes.TrimSpace(out)) > 0 && bytes.Contains(out, []byte("server:")) {
			raw = out
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("k3s kubeconfig never appeared")
		case <-time.After(500 * time.Millisecond):
		}
	}

	// k3s writes `server: https://127.0.0.1:6443` assuming kubectl runs on
	// the same host as the server. From other containers that URL is
	// useless; rewrite to the docker-DNS name that resolves to the k3s
	// container on our shared network.
	rewritten := strings.ReplaceAll(
		string(raw),
		"https://127.0.0.1:6443",
		fmt.Sprintf("https://%s:6443", K3sContainerName),
	)

	if err := os.WriteFile(d.KubeconfigPath(), []byte(rewritten), 0o644); err != nil {
		return fmt.Errorf("writing kubeconfig: %w", err)
	}

	// Write a host-side kubeconfig next to the docker-network one so
	// integration tests running on the host (e.g. bazel test
	// //tests/integration/clrk) can reach k3s. Requires HostPort>0 so
	// the published 127.0.0.1:<port> mapping actually exists.
	if d.HostPort > 0 {
		hostKubeconfig := strings.ReplaceAll(
			string(raw),
			"https://127.0.0.1:6443",
			fmt.Sprintf("https://127.0.0.1:%d", d.HostPort),
		)
		if err := os.WriteFile(d.HostKubeconfigPath(), []byte(hostKubeconfig), 0o644); err != nil {
			return fmt.Errorf("writing host kubeconfig: %w", err)
		}
	}
	return nil
}

// WaitAPIReady blocks until the k3s apiserver responds with 200 on
// /readyz. Polls via `docker exec` since Mac Docker's port-forward is
// unreliable for TLS.
func (d *K3sDriver) WaitAPIReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		cmd := exec.CommandContext(ctx, "docker", "exec", K3sContainerName,
			"kubectl", "--kubeconfig=/etc/rancher/k3s/k3s.yaml",
			"get", "--raw=/readyz")
		if err := cmd.Run(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("k3s API never became ready")
		case <-time.After(time.Second):
		}
	}
}

// KubectlApply runs `kubectl apply` inside the k3s container against the
// in-container kubeconfig. source may be a URL or "-" to read YAML from
// stdin (passed as input). Multiple calls are idempotent.
func (d *K3sDriver) KubectlApply(ctx context.Context, source string, stdin []byte) error {
	args := []string{
		"exec", "-i", K3sContainerName,
		"kubectl", "--kubeconfig=/etc/rancher/k3s/k3s.yaml",
		"apply", "-f", source,
	}
	cmd := exec.CommandContext(ctx, "docker", args...)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl apply %s: %w: %s", source, err, bytes.TrimSpace(out))
	}
	return nil
}

// ApplyControllerManagerBridge wires two selectorless Services to the
// clrk-controller-manager docker container so in-cluster pods can dial
// it by DNS name:
//
//  1. clrk-system/clrk-controller-manager:grpcPort — clrk's gRPC
//     services (extension hooks, cert provider, ext_proc). The EG
//     extension programs this DNS into the data-plane Envoy config.
//  2. envoy-gateway-system/envoy-gateway:xdsPort — the EG xDS server.
//     EG hardcodes "envoy-gateway.envoy-gateway-system.svc:18000" into
//     the data-plane bootstrap, so the Service has to live under that
//     exact name.
func (d *K3sDriver) ApplyControllerManagerBridge(ctx context.Context, cmIP string, grpcPort, xdsPort int32) error {
	yaml := serviceBridge("clrk-system", "clrk-controller-manager", cmIP, grpcPort) +
		serviceBridge("envoy-gateway-system", "envoy-gateway", cmIP, xdsPort)
	return d.KubectlApply(ctx, "-", []byte(yaml))
}

// serviceBridge renders a Namespace + selectorless Service +
// hand-managed Endpoints triple that fronts a docker-network IP under
// an in-cluster DNS name. Always uses port==targetPort and a single
// "grpc" port name; the bridge has no other use cases today.
func serviceBridge(ns, name, ip string, port int32) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Namespace
metadata:
  name: %s
---
apiVersion: v1
kind: Service
metadata:
  name: %s
  namespace: %s
spec:
  ports:
    - name: grpc
      port: %d
      protocol: TCP
      targetPort: %d
---
apiVersion: v1
kind: Endpoints
metadata:
  name: %s
  namespace: %s
subsets:
  - addresses:
      - ip: %s
    ports:
      - name: grpc
        port: %d
        protocol: TCP
---
`, ns, name, ns, port, port, name, ns, ip, port)
}
