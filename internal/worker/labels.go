package worker

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

// BuildSandboxLabels returns the libcontainer Labels slice for a sandbox.
// Empty fields are omitted so the label set is self-describing — a missing
// invocation-id label means "DaemonAgent or pre-bind warm sandbox", not
// "value happens to be empty".
//
// Exported so apoxy-cloud//clrk/worker unit tests can lock down the label
// format without reaching into unexported package internals.
func BuildSandboxLabels(identity proxyproto.AgentIdentity, podName string, attempt int32) []string {
	labels := []string{
		fmt.Sprintf("%s=%s", LabelAgentKind, kindString(identity.Kind)),
		fmt.Sprintf("%s=%s", LabelAgentName, identity.Name),
		fmt.Sprintf("%s=%s", LabelNamespace, identity.Namespace),
		fmt.Sprintf("%s=%s", LabelCreatedAt, time.Now().UTC().Format(time.RFC3339)),
	}
	if identity.UID != "" {
		labels = append(labels, fmt.Sprintf("%s=%s", LabelAgentUID, identity.UID))
	}
	if identity.Revision != "" {
		labels = append(labels, fmt.Sprintf("%s=%s", LabelRevisionName, identity.Revision))
	}
	if identity.InvocationID != "" {
		labels = append(labels, fmt.Sprintf("%s=%s", LabelInvocationID, identity.InvocationID))
	}
	if attempt > 0 {
		labels = append(labels, fmt.Sprintf("%s=%d", LabelAttempt, attempt))
	}
	if podName != "" {
		labels = append(labels, fmt.Sprintf("%s=%s", LabelPodName, podName))
	}
	return labels
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
