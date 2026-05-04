package v1alpha1

import (
	"context"
	"regexp"
	"strings"

	"github.com/robfig/cron"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
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

	if ta.Spec.TimeoutSeconds != nil && *ta.Spec.TimeoutSeconds <= 0 {
		errs = append(errs, field.Invalid(specPath.Child("timeoutSeconds"), *ta.Spec.TimeoutSeconds,
			"timeoutSeconds must be > 0"))
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
