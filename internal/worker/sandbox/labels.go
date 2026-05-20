package sandbox

import (
	"fmt"
	"time"

	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
)

// Sandbox container labels persisted into libcontainer state.json so the
// API lineage of a sandbox can be recovered out-of-band (runc state) and
// future GC paths can correlate on-disk containers with apiserver objects
// without relying on in-memory state.
const (
	LabelAgentKind    = "clrk.apoxy.dev/agent-kind"
	LabelAgentName    = "clrk.apoxy.dev/agent-name"
	LabelAgentUID     = "clrk.apoxy.dev/agent-uid"
	LabelNamespace    = "clrk.apoxy.dev/namespace"
	LabelRevisionName = "clrk.apoxy.dev/revision-name"
	LabelInvocationID = "clrk.apoxy.dev/invocation-id"
	LabelAttempt      = "clrk.apoxy.dev/attempt"
	LabelPodName      = "clrk.apoxy.dev/worker-pod"
	LabelCreatedAt    = "clrk.apoxy.dev/created-at"
)

// BuildLabels returns the lineage labels for a sandbox, encoded
// as "k=v" strings. Empty fields are omitted so the set is
// self-describing — a missing invocation-id means "DaemonAgent or
// pre-bind warm sandbox", not "value happens to be empty".
//
// Exported so apoxy-cloud//clrk/worker unit tests can lock down the
// label format without reaching into unexported package internals.
func BuildLabels(identity proxyproto.AgentIdentity, podName string, attempt int32) []string {
	m := buildSandboxLabelMap(identity, podName, attempt)
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	return out
}

// buildSandboxLabelMap is the single source of truth for sandbox
// lineage labels. BuildLabels (libcontainer-style "k=v" slice)
// and buildSandboxAnnotations (OCI map) both project from here.
func buildSandboxLabelMap(identity proxyproto.AgentIdentity, podName string, attempt int32) map[string]string {
	m := map[string]string{
		LabelAgentKind: kindString(identity.Kind),
		LabelAgentName: identity.Name,
		LabelNamespace: identity.Namespace,
		LabelCreatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if identity.UID != "" {
		m[LabelAgentUID] = identity.UID
	}
	if identity.Revision != "" {
		m[LabelRevisionName] = identity.Revision
	}
	if identity.InvocationID != "" {
		m[LabelInvocationID] = identity.InvocationID
	}
	if attempt > 0 {
		m[LabelAttempt] = fmt.Sprintf("%d", attempt)
	}
	if podName != "" {
		m[LabelPodName] = podName
	}
	return m
}

func kindString(k proxyproto.AgentKind) string {
	switch k {
	case proxyproto.AgentKindDaemon:
		return "DaemonAgent"
	case proxyproto.AgentKindTask:
		return "TaskAgent"
	}
	return "Unknown"
}
