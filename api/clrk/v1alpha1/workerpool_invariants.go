/*
Copyright 2026 Apoxy, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

// Fixed gVisor/runsc invariants of a worker pod. These are owned by the
// controller's pod builder (internal/workerpod) and are not expressible on a
// WorkerPool: a user cannot set or break them. They live here, in the
// cross-platform API package, so both the builder and the WorkerPool admission
// validation (validation.go) reference one source of truth. The dispatch port
// *number* stays in internal/ports (a leaf the API package must not import);
// only the port *name* is duplicated here.
const (
	// WorkerServiceAccountName is the ServiceAccount worker pods run under by
	// default. The worker advertises itself into WorkerPool.status via the
	// k8s API, so the SA needs the matching RBAC.
	WorkerServiceAccountName = "clrk-worker"

	// WorkerContainerName is the fixed name of the worker container. The
	// AppArmor annotation key (WorkerAppArmorAnnotation) and the WorkerPool
	// Service selector both key on it.
	WorkerContainerName = "worker"

	// WorkerAppArmorAnnotation disables AppArmor for the worker container so
	// runsc can fork its gofer. K3s on Linux honors it; k3d-on-mac silently
	// ignores it. The key suffix is WorkerContainerName.
	WorkerAppArmorAnnotation = "container.apparmor.security.beta.kubernetes.io/worker"

	// WorkerDispatchPortName names the worker container's dispatch port
	// (internal/ports.DispatchPort, 8090). The per-TaskAgent ingress ext_proc
	// rewrites :authority to <podIP>:<DispatchPort> on each request.
	WorkerDispatchPortName = "dispatch"
)

// WorkerReservedVolume is a scratch volume the worker runtime owns and mounts
// at a fixed path.
type WorkerReservedVolume struct {
	// Name is the pod volume name and the worker container volumeMount name.
	Name string
	// MountPath is where the worker container mounts the volume.
	MountPath string
}

// WorkerReservedVolumes are the EmptyDir scratch volumes the worker runtime
// requires: runsc state/rootfs/image cache plus the per-invocation stdio tee
// (internal/workerlog.Dir = /run/clrk/logs), both under /run/clrk (run); and
// per-agent persistent state (/var/lib/clrk/state, state). The /var/log/clrk
// (varlog) mount is legacy and currently unused by the worker, retained to
// match the pre-overlay pod spec. The mount paths are hardcoded in the worker
// package (internal/worker, internal/workerlog), so they are owned by the pod
// builder and not settable on a WorkerPool; the validator rejects
// ExtraVolumes/ExtraVolumeMounts that collide with these names or paths.
var WorkerReservedVolumes = []WorkerReservedVolume{
	{Name: "state", MountPath: "/var/lib/clrk/state"},
	{Name: "run", MountPath: "/run/clrk"},
	{Name: "varlog", MountPath: "/var/log/clrk"},
}
