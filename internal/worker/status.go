//go:build linux

package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	ctrl "sigs.k8s.io/controller-runtime"

	workerstatusv1alpha1 "github.com/apoxy-dev/clrk/internal/proto/clrk/v1alpha1"
)

// statusHeartbeat is the floor cadence at which the worker sends a
// snapshot when the dispatcher's activeCounter notifier hasn't fired.
// Doubles as the dead-stream-detect signal on the controller side
// (3× this value).
const statusHeartbeat = 5 * time.Second

// StatusService implements WorkerStatusServiceServer. It is fed by
// the worker's SandboxManager (warm sandboxes), ImageStore (cached
// images), and Dispatcher activeCounter (in-flight executions). One
// instance per worker pod; concurrent Watch streams are supported
// (the controller may reconnect while the prior stream is draining).
type StatusService struct {
	workerstatusv1alpha1.UnimplementedWorkerStatusServiceServer

	sandboxMgr *SandboxManager
	imageStore *ImageStore
	active     *activeCounter

	seq atomic.Uint64
}

// NewStatusService constructs a StatusService. The dispatcher's
// activeCounter (shared with NewDispatcher) is the hot-path
// state-change source for in-flight counts.
func NewStatusService(sandboxMgr *SandboxManager, imageStore *ImageStore, active *activeCounter) *StatusService {
	return &StatusService{
		sandboxMgr: sandboxMgr,
		imageStore: imageStore,
		active:     active,
	}
}

// Watch streams snapshots of this worker's state. First message is a
// snapshot; subsequent messages are sent on activeCounter changes,
// at the fallback-poll cadence for warm/cache changes, and at the
// heartbeat cadence to keep the connection observable when nothing
// has changed.
func (s *StatusService) Watch(req *workerstatusv1alpha1.WatchRequest, stream workerstatusv1alpha1.WorkerStatusService_WatchServer) error {
	ctx := stream.Context()
	log := ctrl.LoggerFrom(ctx).WithName("status.watch")

	notify := s.active.Notifier().Subscribe()
	defer s.active.Notifier().Unsubscribe(notify)

	if err := s.sendSnapshot(stream, true); err != nil {
		return err
	}

	tick := time.NewTicker(statusHeartbeat)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-notify:
			tick.Reset(statusHeartbeat)
		case <-tick.C:
		}
		if err := s.sendSnapshot(stream, false); err != nil {
			log.V(1).Info("Stream send failed", "err", err)
			return err
		}
	}
}

func (s *StatusService) sendSnapshot(stream workerstatusv1alpha1.WorkerStatusService_WatchServer, isFirst bool) error {
	msg := &workerstatusv1alpha1.WorkerStatus{
		UpdateSeq:     s.seq.Add(1),
		Snapshot:      isFirst,
		WarmRevisions: s.warmRevisions(),
		InFlight:      s.inFlight(),
		CachedImages:  s.cachedImages(),
	}
	return stream.Send(msg)
}

func (s *StatusService) warmRevisions() []*workerstatusv1alpha1.WarmRevision {
	type key struct {
		ns, agent, revision string
	}
	counts := make(map[key]uint32)
	for _, sb := range s.sandboxMgr.List() {
		if sb.Phase != SandboxReady {
			continue
		}
		k := key{ns: sb.Identity.Namespace, agent: sb.AgentRef, revision: sb.Identity.Revision}
		if k.agent == "" || k.revision == "" {
			continue
		}
		counts[k]++
	}
	out := make([]*workerstatusv1alpha1.WarmRevision, 0, len(counts))
	for k, c := range counts {
		out = append(out, &workerstatusv1alpha1.WarmRevision{
			Namespace: k.ns,
			Agent:     k.agent,
			Revision:  k.revision,
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

func (s *StatusService) inFlight() []*workerstatusv1alpha1.InFlight {
	snap := s.active.Snapshot()
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

func (s *StatusService) cachedImages() []*workerstatusv1alpha1.CachedImage {
	refs := s.imageStore.CachedRefs()
	sort.Strings(refs)
	out := make([]*workerstatusv1alpha1.CachedImage, 0, len(refs))
	for _, r := range refs {
		out = append(out, &workerstatusv1alpha1.CachedImage{ImageRef: r})
	}
	return out
}

// RunStatusServer starts a gRPC server with the WorkerStatusService
// registered on the given address and blocks until ctx is cancelled
// or Serve returns. Designed to be invoked in its own goroutine from
// the worker runtime startup path.
func RunStatusServer(ctx context.Context, addr string, svc *StatusService) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	srv := grpc.NewServer()
	workerstatusv1alpha1.RegisterWorkerStatusServiceServer(srv, svc)

	errCh := make(chan error, 1)
	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		srv.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}
