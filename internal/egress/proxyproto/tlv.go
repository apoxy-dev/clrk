// Package proxyproto encodes PROXY protocol v2 frames with clrk-specific
// TLVs that carry agent identity into the egress data plane.
package proxyproto

// PP2 TLV type IDs in the private-use range (0xE0..0xEF per PROXY v2 spec).
// The Envoy listener filter decodes these into filter-state keys that our
// handshaker + ext_proc pick up.
const (
	TLVAgentKind      byte = 0xE0 // 1 byte: 0 = DaemonAgent, 1 = TaskAgent
	TLVAgentNamespace byte = 0xE1 // UTF-8
	TLVAgentName      byte = 0xE2 // UTF-8
	TLVAgentUID       byte = 0xE3 // UTF-8 (k8s UID string)
	TLVAgentRevision  byte = 0xE4 // UTF-8 (AgentSandboxRevision name)
	TLVInvocationID   byte = 0xE5 // UTF-8 (TaskAgent UID; empty for DaemonAgent)
)

// AgentKind encodes the agent type carried in TLVAgentKind.
type AgentKind byte

const (
	AgentKindDaemon AgentKind = 0
	AgentKindTask   AgentKind = 1
)

// AgentIdentity is the per-sandbox identity encoded into PP2 TLVs so the
// Envoy MITM listener can attribute traffic back to a clrk agent without
// a side lookup.
type AgentIdentity struct {
	Kind         AgentKind
	Namespace    string
	Name         string
	UID          string
	Revision     string
	InvocationID string // Empty for DaemonAgent.
}
