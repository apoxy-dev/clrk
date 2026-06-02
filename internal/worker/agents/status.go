//go:build linux

package agents

import (
	"sort"

	workerstatusv1alpha1 "github.com/apoxy-dev/clrk/internal/proto/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/worker/sandbox"
)

// buildWorkerStatus assembles this worker's current routing-state snapshot:
// warm sandboxes ready to accept a dispatch (Phase=Ready), in-flight
// dispatches per (ns, agent), and pulled image refs. It is transport-free
// — statusPublisher marshals the result and Puts it into the WORKER_STATUS
// KV bucket, and the controller-manager's healthchecker joins it to the
// worker's routable pod IP + readiness from the pool's EndpointSlices.
func buildWorkerStatus(sandboxMgr *sandbox.Manager, imageStore *sandbox.ImageStore, active *activeCounter) *workerstatusv1alpha1.WorkerStatus {
	return &workerstatusv1alpha1.WorkerStatus{
		WarmRevisions: warmRevisions(sandboxMgr),
		InFlight:      inFlight(active),
		CachedImages:  cachedImages(imageStore),
	}
}

func warmRevisions(sandboxMgr *sandbox.Manager) []*workerstatusv1alpha1.WarmRevision {
	counts := make(map[WarmKey]uint32)
	for _, sb := range sandboxMgr.List() {
		if sb.Phase != sandbox.SandboxReady {
			continue
		}
		k := WarmKey{Namespace: sb.Identity.Namespace, Agent: sb.AgentRef, Revision: sb.Identity.Revision}
		if k.Agent == "" || k.Revision == "" {
			continue
		}
		counts[k]++
	}
	out := make([]*workerstatusv1alpha1.WarmRevision, 0, len(counts))
	for k, c := range counts {
		out = append(out, &workerstatusv1alpha1.WarmRevision{
			Namespace: k.Namespace,
			Agent:     k.Agent,
			Revision:  k.Revision,
			Count:     c,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		if out[i].Agent != out[j].Agent {
			return out[i].Agent < out[j].Agent
		}
		return out[i].Revision < out[j].Revision
	})
	return out
}

func inFlight(active *activeCounter) []*workerstatusv1alpha1.InFlight {
	snap := active.Snapshot()
	out := make([]*workerstatusv1alpha1.InFlight, 0, len(snap))
	for k, c := range snap {
		out = append(out, &workerstatusv1alpha1.InFlight{
			Namespace: k.Namespace,
			Agent:     k.Name,
			Count:     uint32(c),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Agent < out[j].Agent
	})
	return out
}

func cachedImages(imageStore *sandbox.ImageStore) []*workerstatusv1alpha1.CachedImage {
	refs := imageStore.CachedRefs()
	sort.Strings(refs)
	out := make([]*workerstatusv1alpha1.CachedImage, 0, len(refs))
	for _, r := range refs {
		out = append(out, &workerstatusv1alpha1.CachedImage{ImageRef: r})
	}
	return out
}
