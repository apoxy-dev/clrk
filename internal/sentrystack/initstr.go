// The InitStr envelope is pure JSON with no gvisor dependencies — leave
// it without a //go:build constraint so cross-platform unit tests can
// exercise Encode/Decode without pulling in linux-only gvisor packages.
package sentrystack

import (
	"encoding/json"
	"fmt"
)

// initStrVersion is the current envelope version. Bump when the InitStr
// shape changes incompatibly so an old Sentry binary refuses to boot
// against a newer worker's payload (or vice versa) instead of silently
// misinterpreting fields.
const initStrVersion = 1

// InitStrEnv is the env var the worker sets on each runsc subprocess
// invocation to communicate per-sandbox InitStr to PreInit. JSON-encoded
// per InitStr.Encode().
//
// Why an env var instead of pid lookup or OCI annotations: PreInit runs
// in the runsc subprocess (same binary as the worker, but a separate
// fork-exec'd process), so in-memory state in the worker doesn't carry
// over. The PreInitStackArgs only exposes the boot child's pid, which
// isn't a stable key the worker can pre-register against. Env vars are
// inherited by the subprocess at exec time, available to PreInit
// immediately, and naturally per-invocation — one Container.Create →
// one runsc subprocess → one env. OCI annotations would also work but
// require the subprocess to reach into the in-progress config struct,
// which the runsc setupNetwork path doesn't expose easily.
//
// Lives in this (un-tagged) file so cross-platform unit tests can
// reference it without pulling linux-only gvisor packages.
const InitStrEnv = "CLRK_SENTRYSTACK_INITSTR"

// InitStr is the serialized payload sent from the worker process to the
// Sentry boot child via urpc NetworkInitPluginStack. Worker fills it in
// PreInit (per-sandbox); Sentry reads it in Init.
//
// Carries everything the Sentry needs to wire its in-process *tcpip.Stack
// for a specific clrk-sandbox: addressing (eth0), MTU, MAC, default
// gateway (cosmetic), and — in later phases — MITM/IMDS endpoints, DNS
// resolvers, and the policy snapshot. Fields that aren't set on a given
// call (zero-valued) cause the corresponding Sentry-side wiring to be
// skipped, so Phase 1 (lo-only) and later phases can share the same
// envelope.
type InitStr struct {
	Version int `json:"v"`

	// SandboxID is the worker's opaque identifier for this sandbox.
	// Echoed back over PROXY-v2 TLVs so the worker can demux IMDS
	// callbacks and egress streams to the right invocation.
	SandboxID string `json:"sandbox_id,omitempty"`

	// Eth0V4 is the per-sandbox IPv4 address (e.g. "10.200.0.6") and
	// PrefixLen is its CIDR prefix. If V4 is empty, no eth0 v4
	// addressing is wired; the sandbox sees only lo's 127.0.0.1.
	Eth0V4         string `json:"eth0_v4,omitempty"`
	Eth0V4PrefixLen int    `json:"eth0_v4_prefix,omitempty"`

	// Eth0V6 is the per-sandbox IPv6 address (e.g. "fd00:ec2::ffff").
	// If empty, no eth0 v6 addressing is wired.
	Eth0V6         string `json:"eth0_v6,omitempty"`
	Eth0V6PrefixLen int    `json:"eth0_v6_prefix,omitempty"`

	// Eth0MAC is the synthesized link-layer address for eth0 in colon
	// form (e.g. "02:ca:fe:00:00:01"). Sandbox userspace tools that
	// read /sys/class/net/eth0/address or call getifaddrs see this
	// value. Empty falls back to a zero MAC, which is legal but ugly.
	Eth0MAC string `json:"eth0_mac,omitempty"`

	// Eth0MTU overrides loopetherMTU when set. Zero keeps the default
	// (1500). Only set this if a specific test scenario needs it —
	// loopback semantics make the on-wire MTU irrelevant.
	Eth0MTU uint32 `json:"eth0_mtu,omitempty"`

	// GatewayV4 / GatewayV6 are the IP addresses written as the
	// default-route gateway. The forwarder never actually delivers
	// frames to them (loopether short-circuits), but presenting a
	// gateway IP keeps tools like `ip route` happy.
	GatewayV4 string `json:"gw_v4,omitempty"`
	GatewayV6 string `json:"gw_v6,omitempty"`
}

// Encode serializes an InitStr. Empty SandboxID is allowed in Phase 1.
func (s *InitStr) Encode() (string, error) {
	s.Version = initStrVersion
	b, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("encoding initstr: %w", err)
	}
	return string(b), nil
}

// DecodeInitStr parses the payload received over urpc. Rejects mismatched
// versions to surface skew loudly instead of misreading fields.
func DecodeInitStr(s string) (*InitStr, error) {
	if s == "" {
		return &InitStr{Version: initStrVersion}, nil
	}
	var out InitStr
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, fmt.Errorf("decoding initstr: %w", err)
	}
	if out.Version != initStrVersion {
		return nil, fmt.Errorf("initstr version mismatch: got %d, want %d", out.Version, initStrVersion)
	}
	return &out, nil
}
