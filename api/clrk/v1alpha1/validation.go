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
	"slices"
	"strings"
	"time"

	"github.com/robfig/cron"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

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

func (p *CredentialInjectionPolicy) Validate(_ context.Context) field.ErrorList {
	var errs field.ErrorList
	specPath := field.NewPath("spec")

	if len(p.Spec.ParentRefs) == 0 {
		errs = append(errs, field.Required(specPath.Child("parentRefs"), "at least one parentRef is required"))
	}
	supportedKinds := []string{"AIProviderRoute", "MCPRoute", "EgressGateway"}
	for i, ref := range p.Spec.ParentRefs {
		rp := specPath.Child("parentRefs").Index(i)
		// The credential table joins on an explicit clrk.apoxy.dev
		// group + kind + name spelling (buildCredTable); a ref missing
		// one is permanently inert. Reject at admission instead of
		// letting the policy silently never inject.
		if ref.Kind == nil || *ref.Kind == "" {
			errs = append(errs, field.Required(rp.Child("kind"), "kind is required"))
		} else if !slices.Contains(supportedKinds, string(*ref.Kind)) {
			errs = append(errs, field.NotSupported(rp.Child("kind"), string(*ref.Kind), supportedKinds))
		}
		if ref.Group == nil || *ref.Group == "" {
			errs = append(errs, field.Required(rp.Child("group"), "group must be "+SchemeGroupVersion.Group))
		} else if string(*ref.Group) != SchemeGroupVersion.Group {
			errs = append(errs, field.NotSupported(rp.Child("group"), string(*ref.Group), []string{SchemeGroupVersion.Group}))
		}
		if ref.Name == "" {
			errs = append(errs, field.Required(rp.Child("name"), "name of the parent resource is required"))
		}
	}

	if p.Spec.SecretRef.Name == "" {
		errs = append(errs, field.Required(specPath.Child("secretRef", "name"), "the credential Secret name is required"))
	}

	// Target/field pairing. A selector set for a non-matching target is
	// the failure mode worth catching at admission: the policy would
	// resolve and silently not inject what the operator configured.
	hasHeader := p.Spec.HeaderName != nil
	hasQuery := p.Spec.QueryParamName != nil
	hasProviderAuth := p.Spec.ProviderAuth != nil
	switch p.Spec.Target {
	case CredentialTargetHeader:
		if !hasHeader || *p.Spec.HeaderName == "" {
			errs = append(errs, field.Required(specPath.Child("headerName"), "required when target=Header"))
		}
		if hasQuery {
			errs = append(errs, field.Forbidden(specPath.Child("queryParamName"), "only valid when target=QueryParam"))
		}
		if hasProviderAuth {
			errs = append(errs, field.Forbidden(specPath.Child("providerAuth"), "only valid when target=ProviderAuth"))
		}
	case CredentialTargetQueryParam:
		if !hasQuery || *p.Spec.QueryParamName == "" {
			errs = append(errs, field.Required(specPath.Child("queryParamName"), "required when target=QueryParam"))
		}
		if hasHeader {
			errs = append(errs, field.Forbidden(specPath.Child("headerName"), "only valid when target=Header"))
		}
		if hasProviderAuth {
			errs = append(errs, field.Forbidden(specPath.Child("providerAuth"), "only valid when target=ProviderAuth"))
		}
	case CredentialTargetProviderAuth:
		if hasHeader {
			errs = append(errs, field.Forbidden(specPath.Child("headerName"), "only valid when target=Header"))
		}
		if hasQuery {
			errs = append(errs, field.Forbidden(specPath.Child("queryParamName"), "only valid when target=QueryParam"))
		}
		if !hasProviderAuth {
			errs = append(errs, field.Required(specPath.Child("providerAuth"), "required when target=ProviderAuth"))
			break
		}
		pa := p.Spec.ProviderAuth
		paPath := specPath.Child("providerAuth")
		// Only AWSv4 is implemented; GCPServiceAccount is a reserved
		// spelling with no resolver behind it yet — admitting it would
		// produce a policy that never injects.
		if pa.Type != ProviderAuthTypeAWSv4 {
			errs = append(errs, field.NotSupported(paPath.Child("type"), pa.Type, []string{ProviderAuthTypeAWSv4}))
		}
		if pa.Region != nil && *pa.Region == "" {
			errs = append(errs, field.Invalid(paPath.Child("region"), *pa.Region,
				"region must be non-empty when set (omit it to derive from the target host)"))
		}
		if pa.Service != nil && *pa.Service == "" {
			errs = append(errs, field.Invalid(paPath.Child("service"), *pa.Service,
				"service must be non-empty when set (omit it for the bedrock default)"))
		}
	default:
		errs = append(errs, field.NotSupported(specPath.Child("target"), string(p.Spec.Target),
			[]string{string(CredentialTargetHeader), string(CredentialTargetQueryParam), string(CredentialTargetProviderAuth)}))
	}

	return errs
}

func (p *CredentialInjectionPolicy) ValidateUpdate(ctx context.Context, old runtime.Object) field.ErrorList {
	if prev, ok := old.(*CredentialInjectionPolicy); ok && apiequality.Semantic.DeepEqual(&prev.Spec, &p.Spec) {
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
		filters := rules.Index(i).Child("filters")
		for j := range r.Spec.Rules[i].Filters {
			f := r.Spec.Rules[i].Filters[j]
			fp := filters.Index(j)
			switch f.Type {
			case AIProviderFilterExtensionRef:
				// AIProviderRoute's extensionRef is open-kinded: besides the
				// cross-cutting policies it is also the classifier-driven
				// backend-selection seam (APO-480), so accept any clrk kind
				// rather than the RateLimit/Logging-only set the MCP and L4
				// filters use.
				errs = append(errs, validateExtensionRef(f.ExtensionRef, nil, fp.Child("extensionRef"))...)
			case AIProviderFilterTokenBudget:
				if f.TokenBudget == nil {
					errs = append(errs, field.Required(fp.Child("tokenBudget"), "tokenBudget is required when type=TokenBudget"))
				}
			}
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

// extensionRefPolicyKinds enumerates the clrk policy kinds a route filter's
// extensionRef may resolve to on MCPRoute and EgressL4Route. The data plane
// dereferences the ref by group+kind+name to dispatch the right resolver
// (RateLimitPolicy vs LoggingPolicy); the gwapiv1.LocalObjectReference type
// already carries those fields, but nothing populates them by default, so a
// ref that omits group/kind or names an unknown kind is permanently inert.
// Reject it at admission. AIProviderRoute filters pass nil (open-kinded)
// because that extensionRef is also the classifier-driven backend-selection
// seam (APO-480), which accepts kinds outside this set.
var extensionRefPolicyKinds = []string{"RateLimitPolicy", "LoggingPolicy"}

// validateClrkRef applies the attachment/extension reference checks shared by
// every clrk policy and route filter that points at a clrk object: the group
// must be this API group, the name is required, and -- when supportedKinds is
// non-nil -- the kind must be one of them. A nil supportedKinds enforces only
// that kind is non-empty, for refs whose accepted kind set is open-ended.
func validateClrkRef(group, kind, name string, supportedKinds []string, fp *field.Path) field.ErrorList {
	var errs field.ErrorList
	if group == "" {
		errs = append(errs, field.Required(fp.Child("group"), "group must be "+SchemeGroupVersion.Group))
	} else if group != SchemeGroupVersion.Group {
		errs = append(errs, field.NotSupported(fp.Child("group"), group, []string{SchemeGroupVersion.Group}))
	}
	if kind == "" {
		errs = append(errs, field.Required(fp.Child("kind"), "kind is required"))
	} else if supportedKinds != nil && !slices.Contains(supportedKinds, kind) {
		errs = append(errs, field.NotSupported(fp.Child("kind"), kind, supportedKinds))
	}
	if name == "" {
		errs = append(errs, field.Required(fp.Child("name"), "name is required"))
	}
	return errs
}

// validateExtensionRef checks that a route filter's extensionRef carries a
// resolvable clrk policy reference. An ExtensionRef-typed filter with a nil
// or under-specified ref would silently never apply. allowedKinds restricts
// the kind (nil = any clrk kind, for the AIProviderRoute classifier seam).
func validateExtensionRef(ref *gwapiv1.LocalObjectReference, allowedKinds []string, fp *field.Path) field.ErrorList {
	if ref == nil {
		return field.ErrorList{field.Required(fp, "extensionRef is required when type=ExtensionRef")}
	}
	return validateClrkRef(string(ref.Group), string(ref.Kind), string(ref.Name), allowedKinds, fp)
}

// validateRegexList rejects any pattern the data plane could not compile.
// mcproutetable's compileRegexList does regexp.Compile and DROPS the whole
// rule on the first error, so a bad pattern silently disables that rule's
// tool governance -- catch it at admission instead. The engine
// (regexp.Compile) matches the data plane's exactly.
func validateRegexList(patterns []string, fp *field.Path) field.ErrorList {
	var errs field.ErrorList
	for i, p := range patterns {
		if _, err := regexp.Compile(p); err != nil {
			errs = append(errs, field.Invalid(fp.Index(i), p, "must be a valid Go regular expression: "+err.Error()))
		}
	}
	return errs
}

// validateWindow requires a non-empty Go duration string, matching the
// documented "1m"/"1h"/"24h" vocabulary on RateLimitSpec.Window and
// ToolRateLimit.Window.
func validateWindow(window string, fp *field.Path) field.ErrorList {
	if window == "" {
		return field.ErrorList{field.Required(fp, "window is required")}
	}
	if _, err := time.ParseDuration(window); err != nil {
		return field.ErrorList{field.Invalid(fp, window, `window must be a Go duration string (e.g. "1m", "1h", "24h")`)}
	}
	return nil
}

// validateRateLimitScope replaces the (inert, aggregated-apiserver-ignored)
// kubebuilder Enum marker on RateLimitScope. An empty scope is valid: both
// callers default it to PerAgent at decode time (RateLimitPolicy.Default and
// MCPRoute.Default) before validation runs.
func validateRateLimitScope(scope RateLimitScope, fp *field.Path) field.ErrorList {
	switch scope {
	case "", RateLimitScopePerAgent, RateLimitScopePerExecution, RateLimitScopePerRoute:
		return nil
	default:
		return field.ErrorList{field.NotSupported(fp, string(scope), []string{
			string(RateLimitScopePerAgent), string(RateLimitScopePerExecution), string(RateLimitScopePerRoute)})}
	}
}

func (p *RateLimitPolicy) Validate(_ context.Context) field.ErrorList {
	var errs field.ErrorList
	specPath := field.NewPath("spec")
	if p.Spec.Requests <= 0 {
		errs = append(errs, field.Invalid(specPath.Child("requests"), p.Spec.Requests, "requests must be > 0"))
	}
	errs = append(errs, validateWindow(p.Spec.Window, specPath.Child("window"))...)
	errs = append(errs, validateRateLimitScope(p.Spec.Scope, specPath.Child("scope"))...)
	return errs
}

func (p *RateLimitPolicy) ValidateUpdate(ctx context.Context, old runtime.Object) field.ErrorList {
	if prev, ok := old.(*RateLimitPolicy); ok && apiequality.Semantic.DeepEqual(&prev.Spec, &p.Spec) {
		return nil
	}
	return p.Validate(ctx)
}

func (p *EgressDenyPolicy) Validate(_ context.Context) field.ErrorList {
	var errs field.ErrorList
	specPath := field.NewPath("spec")
	if len(p.Spec.TargetRefs) == 0 {
		errs = append(errs, field.Required(specPath.Child("targetRefs"), "at least one targetRef is required"))
	}
	// The deny resolver joins on an explicit clrk.apoxy.dev route spelling;
	// only the route kinds below carry traffic an EgressDenyPolicy can invert.
	supportedKinds := []string{"AIProviderRoute", "MCPRoute", "EgressL4Route"}
	for i, ref := range p.Spec.TargetRefs {
		errs = append(errs, validateClrkRef(string(ref.Group), string(ref.Kind), string(ref.Name),
			supportedKinds, specPath.Child("targetRefs").Index(i))...)
	}
	return errs
}

func (p *EgressDenyPolicy) ValidateUpdate(ctx context.Context, old runtime.Object) field.ErrorList {
	if prev, ok := old.(*EgressDenyPolicy); ok && apiequality.Semantic.DeepEqual(&prev.Spec, &p.Spec) {
		return nil
	}
	return p.Validate(ctx)
}

// validateToolRateLimit applies the same windowing/scope checks as
// RateLimitPolicy.Validate to an inline MCP ToolRateLimit, plus its regex
// tool selector (compiled and rule-dropping in the data plane).
func validateToolRateLimit(rl *ToolRateLimit, fp *field.Path) field.ErrorList {
	var errs field.ErrorList
	if rl.Requests <= 0 {
		errs = append(errs, field.Invalid(fp.Child("requests"), rl.Requests, "requests must be > 0"))
	}
	errs = append(errs, validateWindow(rl.Window, fp.Child("window"))...)
	errs = append(errs, validateRateLimitScope(rl.Scope, fp.Child("scope"))...)
	errs = append(errs, validateRegexList(rl.ToolsRegex, fp.Child("toolsRegex"))...)
	return errs
}

func (r *MCPRoute) Validate(_ context.Context) field.ErrorList {
	var errs field.ErrorList
	rules := field.NewPath("spec").Child("rules")
	for i := range r.Spec.Rules {
		rule := &r.Spec.Rules[i]
		rp := rules.Index(i)
		// Match selectors compile to regexps in the data plane; a bad
		// pattern drops the whole rule.
		for j := range rule.Matches {
			errs = append(errs, validateRegexList(rule.Matches[j].ToolsRegex,
				rp.Child("matches").Index(j).Child("toolsRegex"))...)
		}
		filters := rp.Child("filters")
		for j := range rule.Filters {
			f := rule.Filters[j]
			fp := filters.Index(j)
			switch f.Type {
			case MCPFilterExtensionRef:
				errs = append(errs, validateExtensionRef(f.ExtensionRef, extensionRefPolicyKinds, fp.Child("extensionRef"))...)
			case MCPFilterToolPolicy:
				if f.ToolPolicy == nil {
					errs = append(errs, field.Required(fp.Child("toolPolicy"), "toolPolicy is required when type=ToolPolicy"))
					continue
				}
				tp := f.ToolPolicy
				tpp := fp.Child("toolPolicy")
				errs = append(errs, validateRegexList(tp.AllowedToolsRegex, tpp.Child("allowedToolsRegex"))...)
				errs = append(errs, validateRegexList(tp.DeniedToolsRegex, tpp.Child("deniedToolsRegex"))...)
				for k := range tp.RateLimits {
					errs = append(errs, validateToolRateLimit(&tp.RateLimits[k], tpp.Child("rateLimits").Index(k))...)
				}
			}
		}
	}
	return errs
}

func (r *MCPRoute) ValidateUpdate(ctx context.Context, old runtime.Object) field.ErrorList {
	if prev, ok := old.(*MCPRoute); ok && apiequality.Semantic.DeepEqual(&prev.Spec, &r.Spec) {
		return nil
	}
	return r.Validate(ctx)
}

func (r *EgressL4Route) Validate(_ context.Context) field.ErrorList {
	var errs field.ErrorList
	rules := field.NewPath("spec").Child("rules")
	for i := range r.Spec.Rules {
		filters := rules.Index(i).Child("filters")
		for j := range r.Spec.Rules[i].Filters {
			f := r.Spec.Rules[i].Filters[j]
			if f.Type == L4FilterExtensionRef {
				errs = append(errs, validateExtensionRef(f.ExtensionRef, extensionRefPolicyKinds, filters.Index(j).Child("extensionRef"))...)
			}
		}
	}
	return errs
}

func (r *EgressL4Route) ValidateUpdate(ctx context.Context, old runtime.Object) field.ErrorList {
	if prev, ok := old.(*EgressL4Route); ok && apiequality.Semantic.DeepEqual(&prev.Spec, &r.Spec) {
		return nil
	}
	return r.Validate(ctx)
}
