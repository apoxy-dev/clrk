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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// These types are served through the apoxy-cli aggregated apiserver builder,
// NOT kube-apiserver's CRD machinery, so the +kubebuilder:default markers on
// their fields are never enforced on the serving path — they are documentation
// only. To make the documented defaults real, each type below implements
// resourcestrategy.Defaulter (the compile-time assertions live in resources.go
// alongside the Validater assertions). The builder registers Default() as a
// scheme defaulting func via resource.AddToScheme -> Scheme.AddTypeDefaultingFunc,
// so it fires at decode time: on create/update the default is persisted, and on
// read it is stamped so clients and controllers observe it. See APO-638 / APO-639.
//
// Only load-bearing defaults are stamped. Fields whose nil/zero value is already
// identical to the default at every consumer (TaskAgent.Spec.WarmPoolSize nil==0,
// AgentDelivery.Mode ""==Stdin) are intentionally left unstamped to avoid
// server-side-apply field-ownership churn. Nested defaults are applied only when
// their parent struct is already present — Default() never allocates an absent
// optional parent just to stamp a default the user did not opt into.

// DefaultTaskAgentTimeout mirrors the +kubebuilder:default on
// TaskAgentSpec.Timeout. It is the maximum a single execution runs when the
// agent author does not set spec.timeout.
const DefaultTaskAgentTimeout = 100 * time.Second

// defaultBodyCaptureMaxBytes mirrors the +kubebuilder:default on
// BodyCaptureSpec.MaxBytes (64 KiB per direction).
const defaultBodyCaptureMaxBytes int32 = 64 * 1024

// Default sets spec.timeout to DefaultTaskAgentTimeout when unset. Without it a
// timeout-less TaskAgent dereferenced a nil Spec.Timeout in the ingress
// reconciler (APO-638); the consumer guards remain as defense-in-depth for
// objects persisted before this defaulter shipped.
func (r *TaskAgent) Default() {
	if r.Spec.Timeout == nil {
		r.Spec.Timeout = &metav1.Duration{Duration: DefaultTaskAgentTimeout}
	}
}

// Default sets spec.replicas to 1 when unset and stamps the curated overlay's
// load-bearing defaults (image pull policy and service account) so a minimal
// WorkerPool resolves to the canonical worker pod. The gVisor invariants
// themselves are not in the overlay -- the controller's pod builder owns them
// -- so they need no defaulting here.
func (r *WorkerPool) Default() {
	if r.Spec.Replicas == nil {
		r.Spec.Replicas = ptr.To(int32(1))
	}
	if r.Spec.Template.ImagePullPolicy == "" {
		r.Spec.Template.ImagePullPolicy = corev1.PullIfNotPresent
	}
	if r.Spec.Template.ServiceAccountName == "" {
		r.Spec.Template.ServiceAccountName = WorkerServiceAccountName
	}
}

// Default sets spec.restartPolicy to Always when unset, matching the
// decideRestart consumer which already treats "" as Always.
func (r *DaemonAgent) Default() {
	if r.Spec.RestartPolicy == "" {
		r.Spec.RestartPolicy = RestartPolicyAlways
	}
}

// Default fills the EgressGateway's documented defaults. The defaultPolicy
// default is security-load-bearing: an empty defaultPolicy previously routed
// unmatched traffic as allow-all (fail open) in internal/egress; stamping
// deny-all makes the gateway fail closed, matching the marker.
func (r *EgressGateway) Default() {
	if r.Spec.DefaultPolicy == "" {
		r.Spec.DefaultPolicy = EgressPolicyDenyAll
	}
	if r.Spec.DNS != nil && r.Spec.DNS.LookupFamily == "" {
		r.Spec.DNS.LookupFamily = EgressDNSLookupV4Preferred
	}
	if r.Spec.OTLP != nil && r.Spec.OTLP.CaptureBody != nil && r.Spec.OTLP.CaptureBody.MaxBytes == nil {
		r.Spec.OTLP.CaptureBody.MaxBytes = ptr.To(defaultBodyCaptureMaxBytes)
	}
}

// Default sets spec.secretKey to "token" when unset, matching the credential
// injection consumer's own "" fallback.
func (r *CredentialInjectionPolicy) Default() {
	if r.Spec.SecretKey == "" {
		r.Spec.SecretKey = "token"
	}
}

// Default sets spec.scope to PerAgent when unset.
func (r *RateLimitPolicy) Default() {
	if r.Spec.Scope == "" {
		r.Spec.Scope = RateLimitScopePerAgent
	}
}

// Default stamps the documented PerAgent default on each inline tool rate
// limit's Scope. The +kubebuilder:default marker on ToolRateLimit.Scope is
// inert under the aggregated apiserver (see the package comment), so without
// this an omitted scope persists as "" rather than PerAgent. Nested defaults
// are applied only where the parent ToolPolicy already exists.
func (r *MCPRoute) Default() {
	for i := range r.Spec.Rules {
		for j := range r.Spec.Rules[i].Filters {
			tp := r.Spec.Rules[i].Filters[j].ToolPolicy
			if tp == nil {
				continue
			}
			for k := range tp.RateLimits {
				if tp.RateLimits[k].Scope == "" {
					tp.RateLimits[k].Scope = RateLimitScopePerAgent
				}
			}
		}
	}
}

// Default sets the deny response status to 403 when a denyResponse block is
// present without an explicit code. It does not allocate an absent denyResponse.
func (r *EgressDenyPolicy) Default() {
	if r.Spec.DenyResponse != nil && r.Spec.DenyResponse.StatusCode == 0 {
		r.Spec.DenyResponse.StatusCode = 403
	}
}

// Default sets spec.type to Upstream when unset. This is load-bearing: the
// aggregated apiserver does not honor the +kubebuilder:default marker (see the
// package comment), so without it a type-less Backend persists with Type="",
// and the egress data plane's resolveBackends drops an empty-type backend as
// malformed instead of treating it as the documented Upstream default.
// BackendUpstream.Port is intentionally left unstamped: its only consumer
// (resolveBackends) already maps 0 to 443, so stamping it would add
// server-side-apply field-ownership churn for no behavioral change.
func (r *Backend) Default() {
	if r.Spec.Type == "" {
		r.Spec.Type = BackendTypeUpstream
	}
}
