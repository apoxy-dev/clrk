package invevent

import "fmt"

// WorkerStatusBucket is the JetStream KV bucket each worker pod publishes
// its routing state (warm sandboxes, in-flight dispatches, cached images)
// into, keyed by WorkerStatusKey. The value is a marshalled
// workerstatusv1alpha1.WorkerStatus snapshot, last-writer-wins.
//
// Defined here (the dependency-light wire-contract leaf) so the worker
// publisher and the controller-manager watcher can name the bucket without
// importing the embedded nats-server. The controller-manager creates and
// owns the bucket config (TTL/History/Replicas); workers only Put and
// Delete their own key.
const WorkerStatusBucket = "WORKER_STATUS"

// WorkerStatusKey returns the KV key a worker publishes its WorkerStatus
// under. The controller-manager reconstructs the same key from a
// WorkerPool's namespace/name and a pod name (the latter sourced from the
// pool Service's EndpointSlice targetRef) to join routing state to the
// worker's routable pod IP + readiness.
//
// Because tok is lossy, the key is never parsed back into identity: both
// sides build it from the same (namespace, pool, pod) inputs, so the tok'd
// keys match. In practice namespace/pool/pod are dot-free DNS labels and
// tok is lossless for them.
func WorkerStatusKey(namespace, pool, pod string) string {
	return fmt.Sprintf("%s.%s.%s", tok(namespace), tok(pool), tok(pod))
}
