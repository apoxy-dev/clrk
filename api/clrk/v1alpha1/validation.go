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

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/robfig/cron"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
	// Provider registration: schema-name validation resolves spellings
	// against the llmcall registry, which the provider plugins populate
	// from init. Without this import a binary serving these types would
	// reject every non-custom schema name.
	_ "github.com/apoxy-dev/clrk/internal/extproc/llmcall/providers/all"
)

// imageRefRegexp is a permissive smoke-test for OCI references. It accepts
// short forms (`nginx`, `nginx:latest`), namespaced forms (`library/nginx`),
// and full registry forms (`ghcr.io/foo/bar:tag@sha256:...`). The intent is
// to reject obvious typos (whitespace, leading/trailing slashes, scheme
// prefixes) at admission time without duplicating the full distribution
// spec — runtime image pulls will surface deeper errors.
var imageRefRegexp = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@/+-]*$`)

func validateImage(image string, fp *field.Path) field.ErrorList {
	var errs field.ErrorList
	if image == "" {
		errs = append(errs, field.Required(fp, "image is required"))
		return errs
	}
	if strings.ContainsAny(image, " \t\n\r") {
		errs = append(errs, field.Invalid(fp, image, "image must not contain whitespace"))
	}
	if strings.Contains(image, "://") {
		errs = append(errs, field.Invalid(fp, image, "image must not contain a scheme"))
	}
	if strings.HasPrefix(image, "/") || strings.HasSuffix(image, "/") {
		errs = append(errs, field.Invalid(fp, image, "image must not have leading or trailing '/'"))
	}
	if strings.Contains(image, "//") {
		errs = append(errs, field.Invalid(fp, image, "image must not contain '//'"))
	}
	if len(errs) > 0 {
		return errs
	}
	if !imageRefRegexp.MatchString(image) {
		errs = append(errs, field.Invalid(fp, image, "image is not a valid OCI reference"))
	}
	return errs
}

func validateAgentSandbox(s AgentSandbox, fp *field.Path) field.ErrorList {
	return validateImage(s.Image, fp.Child("image"))
}

func validateEgressRefs(refs []AgentEgressRef, fp *field.Path) field.ErrorList {
	var errs field.ErrorList
	for i, r := range refs {
		if r.GatewayRef == "" {
			errs = append(errs, field.Required(fp.Index(i).Child("gatewayRef"), "gatewayRef is required"))
		}
	}
	return errs
}

// reservedMountPaths enumerates in-sandbox paths that the runtime owns
// and must not be shadowed by an agent's persistent-state bind mount.
// The state mount is appended last in the OCI spec and is writable, so
// without this guard a TaskAgent could overlay system mounts, the trust
// CA bundle, /etc/resolv.conf, or rootfs binary dirs and persist the
// overlay across executions via the per-(ns,agent) host backing dir.
//
// The list duplicates the sources of truth in the worker package
// (internal/worker/sandbox/oci_spec.go defaultSpecMounts and
// buildResolvMountSpec, internal/worker/sandbox/trust.go
// wellKnownTrustPaths) because this file is cross-platform and the
// worker package is //go:build linux. Keep the two in sync when either
// side changes.
var reservedMountPaths = []string{
	// defaultSpecMounts destinations.
	"/proc",
	"/dev",
	"/sys",
	"/sys/fs/cgroup",
	"/dev/pts",
	"/tmp",
	// buildResolvMountSpec destination.
	"/etc/resolv.conf",
	// wellKnownTrustPaths CA bundle locations.
	"/etc/ssl/certs/ca-certificates.crt",
	"/etc/pki/tls/certs/ca-bundle.crt",
	"/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
	"/etc/ssl/cert.pem",
	// Rootfs binary and system-config dirs. Mounting a writable
	// overlay over any of these lets an agent plant binaries or
	// config that re-applies on the next execution.
	"/etc",
	"/usr",
	"/bin",
	"/sbin",
	"/lib",
	"/lib64",
	"/boot",
	"/root",
}

// pathConflicts reports whether candidate and reserved name the same
// path, or one is an ancestor of the other. Both arguments must be
// clean absolute paths.
func pathConflicts(candidate, reserved string) bool {
	if candidate == reserved {
		return true
	}
	sep := string(filepath.Separator)
	return strings.HasPrefix(candidate, reserved+sep) || strings.HasPrefix(reserved, candidate+sep)
}

// validateAgentState rejects MountPath values that are non-clean,
// non-absolute, root, or that overlap with a reserved system mount.
// An empty MountPath is valid and means "use the worker default".
func validateAgentState(state *AgentState, fp *field.Path) field.ErrorList {
	if state == nil || state.MountPath == "" {
		return nil
	}
	var errs field.ErrorList
	p := state.MountPath
	mp := fp.Child("mountPath")
	if !filepath.IsAbs(p) {
		return append(errs, field.Invalid(mp, p, "mountPath must be an absolute path"))
	}
	if p != filepath.Clean(p) {
		return append(errs, field.Invalid(mp, p, "mountPath must be a clean path (no '.', '..', '//' or trailing '/')"))
	}
	if p == "/" {
		return append(errs, field.Invalid(mp, p, "mountPath must not be the root path"))
	}
	for _, r := range reservedMountPaths {
		if pathConflicts(p, r) {
			return append(errs, field.Invalid(mp, p, fmt.Sprintf("mountPath conflicts with reserved path %q", r)))
		}
	}
	return errs
}

func validateIdentity(id *AgentIdentity, fp *field.Path) field.ErrorList {
	if id == nil {
		return nil
	}
	var errs field.ErrorList
	if len(id.Extractors) == 0 {
		errs = append(errs, field.Required(fp.Child("extractors"), "at least one extractor is required when identity is set"))
	}
	for i, e := range id.Extractors {
		ep := fp.Child("extractors").Index(i)
		switch e.Type {
		case IdentityExtractorTypeHeader:
			if e.Header == nil {
				errs = append(errs, field.Required(ep.Child("header"), "header is required for type=Header"))
			}
		case IdentityExtractorTypeBody:
			if e.Body == nil {
				errs = append(errs, field.Required(ep.Child("body"), "body is required for type=Body"))
			}
		case IdentityExtractorTypeJWT:
			if e.JWT == nil {
				errs = append(errs, field.Required(ep.Child("jwt"), "jwt is required for type=JWT"))
			}
		default:
			errs = append(errs, field.NotSupported(ep.Child("type"), e.Type,
				[]string{string(IdentityExtractorTypeHeader), string(IdentityExtractorTypeBody), string(IdentityExtractorTypeJWT)}))
		}
	}
	return errs
}

func (ta *TaskAgent) Validate(_ context.Context) field.ErrorList {
	var errs field.ErrorList
	specPath := field.NewPath("spec")

	errs = append(errs, validateAgentSandbox(ta.Spec.Template.Spec.AgentSandbox, specPath.Child("template", "spec"))...)

	if ta.Spec.WorkerPoolRef == "" {
		errs = append(errs, field.Required(specPath.Child("workerPoolRef"), "workerPoolRef is required"))
	}

	if ta.Spec.Timeout != nil && ta.Spec.Timeout.Duration <= 0 {
		errs = append(errs, field.Invalid(specPath.Child("timeout"), ta.Spec.Timeout.Duration.String(),
			"timeout must be > 0"))
	}

	if ta.Spec.MaxConcurrent != nil && *ta.Spec.MaxConcurrent < 0 {
		errs = append(errs, field.Invalid(specPath.Child("maxConcurrent"), *ta.Spec.MaxConcurrent,
			"maxConcurrent must be >= 0"))
	}

	if ta.Spec.Schedule != nil {
		if _, err := cron.ParseStandard(*ta.Spec.Schedule); err != nil {
			errs = append(errs, field.Invalid(specPath.Child("schedule"), *ta.Spec.Schedule, err.Error()))
		}
	}

	errs = append(errs, validateEgressRefs(ta.Spec.EgressRefs, specPath.Child("egressRefs"))...)
	errs = append(errs, validateIdentity(ta.Spec.Identity, specPath.Child("identity"))...)
	errs = append(errs, validateAgentState(ta.Spec.State, specPath.Child("state"))...)

	return errs
}

// Cross-object existence (workerPool / egressRef) is not enforced at admission
// — declarative apply chains create the agent before the referenced resource
// lands; reconcilers surface those via status conditions.
func (ta *TaskAgent) ValidateUpdate(ctx context.Context, _ runtime.Object) field.ErrorList {
	return ta.Validate(ctx)
}

func (da *DaemonAgent) Validate(_ context.Context) field.ErrorList {
	var errs field.ErrorList
	specPath := field.NewPath("spec")

	errs = append(errs, validateAgentSandbox(da.Spec.Template.Spec.AgentSandbox, specPath.Child("template", "spec"))...)

	if da.Spec.WorkerPoolRef == "" {
		errs = append(errs, field.Required(specPath.Child("workerPoolRef"), "workerPoolRef is required"))
	}

	switch da.Spec.RestartPolicy {
	case "", RestartPolicyAlways, RestartPolicyOnFailure, RestartPolicyNever:
	default:
		errs = append(errs, field.NotSupported(specPath.Child("restartPolicy"), da.Spec.RestartPolicy,
			[]string{string(RestartPolicyAlways), string(RestartPolicyOnFailure), string(RestartPolicyNever)}))
	}

	if da.Spec.MaxRestarts != nil && *da.Spec.MaxRestarts < 0 {
		errs = append(errs, field.Invalid(specPath.Child("maxRestarts"), *da.Spec.MaxRestarts,
			"maxRestarts must be >= 0"))
	}

	if da.Spec.MaxLifetimeSeconds != nil && *da.Spec.MaxLifetimeSeconds < 0 {
		errs = append(errs, field.Invalid(specPath.Child("maxLifetimeSeconds"), *da.Spec.MaxLifetimeSeconds,
			"maxLifetimeSeconds must be >= 0"))
	}

	errs = append(errs, validateEgressRefs(da.Spec.EgressRefs, specPath.Child("egressRefs"))...)

	return errs
}

func (da *DaemonAgent) ValidateUpdate(ctx context.Context, _ runtime.Object) field.ErrorList {
	return da.Validate(ctx)
}

// validateWorkerExtraMounts rejects ExtraVolumes/ExtraVolumeMounts that would
// collide with a reserved worker volume name, reference an undeclared volume,
// or mount at a path the worker container owns. WorkerReservedVolumes
// (workerpool_invariants.go) is the source of truth for the reserved names and
// scratch paths; reservedMountPaths covers the rootfs/system dirs the worker
// and runsc depend on.
func validateWorkerExtraMounts(t WorkerPodTemplate, fp *field.Path) field.ErrorList {
	var errs field.ErrorList
	reservedNames := make(map[string]bool, len(WorkerReservedVolumes))
	declaredNames := make(map[string]bool)
	for _, rv := range WorkerReservedVolumes {
		reservedNames[rv.Name] = true
		declaredNames[rv.Name] = true
	}
	for i, v := range t.ExtraVolumes {
		if reservedNames[v.Name] {
			errs = append(errs, field.Invalid(fp.Child("extraVolumes").Index(i).Child("name"), v.Name,
				"name conflicts with a reserved worker volume (state/run/varlog)"))
		}
		declaredNames[v.Name] = true
	}
	for i, m := range t.ExtraVolumeMounts {
		mp := fp.Child("extraVolumeMounts").Index(i)
		if reservedNames[m.Name] {
			errs = append(errs, field.Invalid(mp.Child("name"), m.Name,
				"name conflicts with a reserved worker volume (state/run/varlog)"))
		} else if !declaredNames[m.Name] {
			errs = append(errs, field.Invalid(mp.Child("name"), m.Name,
				"no volume with this name is declared in extraVolumes"))
		}
		errs = append(errs, validateWorkerMountPath(m.MountPath, mp.Child("mountPath"))...)
	}
	return errs
}

// validateWorkerMountPath rejects an ExtraVolumeMount path that is empty,
// non-absolute, non-clean, root, or that overlaps a path the worker container
// owns -- the runsc scratch volumes (run/state/varlog) or a rootfs/system path
// from reservedMountPaths. It mirrors validateAgentState's path discipline so a
// bad path fails loudly at admission rather than as a CreateContainerError when
// the kubelet later refuses the pod (relative/non-clean paths in particular
// would otherwise evade pathConflicts, which assumes clean absolute paths).
func validateWorkerMountPath(p string, fp *field.Path) field.ErrorList {
	if p == "" {
		return field.ErrorList{field.Required(fp, "mountPath is required")}
	}
	if !filepath.IsAbs(p) {
		return field.ErrorList{field.Invalid(fp, p, "mountPath must be an absolute path")}
	}
	if p != filepath.Clean(p) {
		return field.ErrorList{field.Invalid(fp, p, "mountPath must be a clean path (no '.', '..', '//' or trailing '/')")}
	}
	if p == "/" {
		return field.ErrorList{field.Invalid(fp, p, "mountPath must not be the root path")}
	}
	var errs field.ErrorList
	for _, rv := range WorkerReservedVolumes {
		if pathConflicts(p, rv.MountPath) {
			errs = append(errs, field.Invalid(fp, p, fmt.Sprintf("mountPath conflicts with reserved worker path %q", rv.MountPath)))
		}
	}
	for _, r := range reservedMountPaths {
		if pathConflicts(p, r) {
			errs = append(errs, field.Invalid(fp, p, fmt.Sprintf("mountPath conflicts with reserved system path %q", r)))
		}
	}
	return errs
}

func (wp *WorkerPool) Validate(_ context.Context) field.ErrorList {
	var errs field.ErrorList
	specPath := field.NewPath("spec")
	tp := specPath.Child("template")

	errs = append(errs, validateImage(wp.Spec.Template.Image, tp.Child("image"))...)

	if wp.Spec.Replicas != nil && *wp.Spec.Replicas < 0 {
		errs = append(errs, field.Invalid(specPath.Child("replicas"), *wp.Spec.Replicas, "replicas must be >= 0"))
	}
	if wp.Spec.MaxExecutionsPerWorker != nil && *wp.Spec.MaxExecutionsPerWorker < 0 {
		errs = append(errs, field.Invalid(specPath.Child("maxExecutionsPerWorker"), *wp.Spec.MaxExecutionsPerWorker,
			"maxExecutionsPerWorker must be >= 0"))
	}
	if wp.Spec.WarmPool != nil && *wp.Spec.WarmPool < 0 {
		errs = append(errs, field.Invalid(specPath.Child("warmPool"), *wp.Spec.WarmPool, "warmPool must be >= 0"))
	}

	errs = append(errs, validateWorkerExtraMounts(wp.Spec.Template, tp)...)

	return errs
}

func (wp *WorkerPool) ValidateUpdate(ctx context.Context, old runtime.Object) field.ErrorList {
	// The aggregated apiserver's status subresource strategy inherits this
	// ValidateUpdate, so a status (or metadata-only) write re-runs it. Skip the
	// spec checks when the spec is unchanged: otherwise a status patch to a pool
	// whose stored spec predates this validator -- e.g. a pre-overlay WorkerPool
	// that decoded with an empty Template.Image -- would be rejected, and the
	// deployment/status reconcilers could never report ReadyReplicas/conditions.
	if prev, ok := old.(*WorkerPool); ok && apiequality.Semantic.DeepEqual(&prev.Spec, &wp.Spec) {
		return nil
	}
	return wp.Validate(ctx)
}

// validateProviderSchemaName checks a CRD-supplied provider/schema
// spelling against the llmcall registry. "custom" is always legal
// (endpoint-only matching, operator-vouched backends). The registry —
// not a hardcoded enum — is the source of truth, so a new provider
// plugin extends what admission accepts without an API change; pending
// aliases (azure-openai, bedrock) stay legal before their plugins
// land. Case-insensitive: the data plane lowercases before matching.
func validateProviderSchemaName(name string, fp *field.Path) field.ErrorList {
	var errs field.ErrorList
	if name == "" {
		errs = append(errs, field.Required(fp, "provider schema name is required"))
		return errs
	}
	n := strings.ToLower(name)
	if n == "custom" || llmcall.KnownName(n) {
		return nil
	}
	errs = append(errs, field.NotSupported(fp, name, append(llmcall.KnownNames(), "custom")))
	return errs
}

func (b *Backend) Validate(_ context.Context) field.ErrorList {
	return validateProviderSchemaName(string(b.Spec.Schema.Name),
		field.NewPath("spec").Child("schema", "name"))
}

func (b *Backend) ValidateUpdate(ctx context.Context, old runtime.Object) field.ErrorList {
	// Same status-write skip as WorkerPool: a stored spec that predates
	// this validator (or whose schema's plugin was unlinked) must not
	// wedge the status controller.
	if prev, ok := old.(*Backend); ok && apiequality.Semantic.DeepEqual(&prev.Spec, &b.Spec) {
		return nil
	}
	return b.Validate(ctx)
}

func (p *FallbackRoutingPolicy) Validate(_ context.Context) field.ErrorList {
	var errs field.ErrorList
	specPath := field.NewPath("spec")

	if len(p.Spec.ParentRefs) == 0 {
		errs = append(errs, field.Required(specPath.Child("parentRefs"), "at least one parentRef is required"))
	}
	for i, ref := range p.Spec.ParentRefs {
		rp := specPath.Child("parentRefs").Index(i)
		// Attachment (llmroute.PolicyAttachesTo) joins on an explicit
		// clrk.apoxy.dev/AIProviderRoute spelling plus a name, so any
		// ref missing one of those is permanently inert. Reject it at
		// admission instead of letting it silently never attach: only
		// AIProviderRoute parents carry backendRef candidate sets, so
		// nothing else can ever gain fallback semantics.
		if ref.Kind == nil || *ref.Kind == "" {
			errs = append(errs, field.Required(rp.Child("kind"), "kind must be AIProviderRoute"))
		} else if string(*ref.Kind) != "AIProviderRoute" {
			errs = append(errs, field.NotSupported(rp.Child("kind"), string(*ref.Kind), []string{"AIProviderRoute"}))
		}
		if ref.Group == nil || *ref.Group == "" {
			errs = append(errs, field.Required(rp.Child("group"), "group must be "+SchemeGroupVersion.Group))
		} else if string(*ref.Group) != SchemeGroupVersion.Group {
			errs = append(errs, field.NotSupported(rp.Child("group"), string(*ref.Group), []string{SchemeGroupVersion.Group}))
		}
		if ref.Name == "" {
			errs = append(errs, field.Required(rp.Child("name"), "name of the parent AIProviderRoute is required"))
		}
	}

	if r := p.Spec.Retry; r != nil {
		retryPath := specPath.Child("retry")
		if r.NumRetries != nil && *r.NumRetries < 0 {
			errs = append(errs, field.Invalid(retryPath.Child("numRetries"), *r.NumRetries, "numRetries must be >= 0"))
		}
		for i, code := range r.RetriableStatusCodes {
			if code < 400 || code > 599 {
				errs = append(errs, field.Invalid(retryPath.Child("retriableStatusCodes").Index(i), code,
					"retriable status codes must be in [400, 599]"))
			}
		}
		if r.PerTryTimeout != nil && r.PerTryTimeout.Duration <= 0 {
			errs = append(errs, field.Invalid(retryPath.Child("perTryTimeout"), r.PerTryTimeout.Duration.String(),
				"perTryTimeout must be > 0"))
		}
	}

	if e := p.Spec.Ejection; e != nil {
		if e.MaxEjectionTime != nil && e.MaxEjectionTime.Duration <= 0 {
			errs = append(errs, field.Invalid(specPath.Child("ejection").Child("maxEjectionTime"),
				e.MaxEjectionTime.Duration.String(), "maxEjectionTime must be > 0"))
		}
	}

	return errs
}

func (p *FallbackRoutingPolicy) ValidateUpdate(ctx context.Context, old runtime.Object) field.ErrorList {
	if prev, ok := old.(*FallbackRoutingPolicy); ok && apiequality.Semantic.DeepEqual(&prev.Spec, &p.Spec) {
		return nil
	}
	return p.Validate(ctx)
}

func (r *AIProviderRoute) Validate(_ context.Context) field.ErrorList {
	var errs field.ErrorList
	rules := field.NewPath("spec").Child("rules")
	for i := range r.Spec.Rules {
		matches := rules.Index(i).Child("matches")
		for j := range r.Spec.Rules[i].Matches {
			errs = append(errs, validateProviderSchemaName(r.Spec.Rules[i].Matches[j].Provider,
				matches.Index(j).Child("provider"))...)
		}
	}
	return errs
}

func (r *AIProviderRoute) ValidateUpdate(ctx context.Context, old runtime.Object) field.ErrorList {
	if prev, ok := old.(*AIProviderRoute); ok && apiequality.Semantic.DeepEqual(&prev.Spec, &r.Spec) {
		return nil
	}
	return r.Validate(ctx)
}
