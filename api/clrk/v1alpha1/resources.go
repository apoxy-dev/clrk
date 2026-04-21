package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"

	"github.com/apoxy-dev/apoxy/api/resource"
)

// Compile-time assertions that each kind implements the interfaces required
// by the in-tree apiserver builder.

var (
	_ runtime.Object                       = &TaskAgent{}
	_ resource.Object                      = &TaskAgent{}
	_ resource.ObjectWithStatusSubResource = &TaskAgent{}
	_ rest.SingularNameProvider            = &TaskAgent{}
	_ resource.StatusSubResource           = &TaskAgentStatus{}

	_ runtime.Object                       = &DaemonAgent{}
	_ resource.Object                      = &DaemonAgent{}
	_ resource.ObjectWithStatusSubResource = &DaemonAgent{}
	_ rest.SingularNameProvider            = &DaemonAgent{}
	_ resource.StatusSubResource           = &DaemonAgentStatus{}

	_ runtime.Object                       = &WorkerPool{}
	_ resource.Object                      = &WorkerPool{}
	_ resource.ObjectWithStatusSubResource = &WorkerPool{}
	_ rest.SingularNameProvider            = &WorkerPool{}
	_ resource.StatusSubResource           = &WorkerPoolStatus{}

	_ runtime.Object                       = &AgentSandboxRevision{}
	_ resource.Object                      = &AgentSandboxRevision{}
	_ resource.ObjectWithStatusSubResource = &AgentSandboxRevision{}
	_ rest.SingularNameProvider            = &AgentSandboxRevision{}
	_ resource.StatusSubResource           = &AgentSandboxRevisionStatus{}

	_ runtime.Object                       = &EgressGateway{}
	_ resource.Object                      = &EgressGateway{}
	_ resource.ObjectWithStatusSubResource = &EgressGateway{}
	_ rest.SingularNameProvider            = &EgressGateway{}
	_ resource.StatusSubResource           = &EgressGatewayStatus{}

	_ runtime.Object                       = &EgressL4Route{}
	_ resource.Object                      = &EgressL4Route{}
	_ resource.ObjectWithStatusSubResource = &EgressL4Route{}
	_ rest.SingularNameProvider            = &EgressL4Route{}
	_ resource.StatusSubResource           = &EgressL4RouteStatus{}

	_ runtime.Object                       = &MCPRoute{}
	_ resource.Object                      = &MCPRoute{}
	_ resource.ObjectWithStatusSubResource = &MCPRoute{}
	_ rest.SingularNameProvider            = &MCPRoute{}
	_ resource.StatusSubResource           = &MCPRouteStatus{}

	_ runtime.Object                       = &AIProviderRoute{}
	_ resource.Object                      = &AIProviderRoute{}
	_ resource.ObjectWithStatusSubResource = &AIProviderRoute{}
	_ rest.SingularNameProvider            = &AIProviderRoute{}
	_ resource.StatusSubResource           = &AIProviderRouteStatus{}

	_ runtime.Object            = &CredentialInjectionPolicy{}
	_ resource.Object           = &CredentialInjectionPolicy{}
	_ rest.SingularNameProvider = &CredentialInjectionPolicy{}

	_ runtime.Object            = &RateLimitPolicy{}
	_ resource.Object           = &RateLimitPolicy{}
	_ rest.SingularNameProvider = &RateLimitPolicy{}

	_ runtime.Object            = &LoggingPolicy{}
	_ resource.Object           = &LoggingPolicy{}
	_ rest.SingularNameProvider = &LoggingPolicy{}

	_ runtime.Object            = &EgressDenyPolicy{}
	_ resource.Object           = &EgressDenyPolicy{}
	_ rest.SingularNameProvider = &EgressDenyPolicy{}
)

func gvr(resource string) schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    SchemeGroupVersion.Group,
		Version:  SchemeGroupVersion.Version,
		Resource: resource,
	}
}

// TaskAgent.

func (r *TaskAgent) GetObjectMeta() *metav1.ObjectMeta { return &r.ObjectMeta }
func (r *TaskAgent) NamespaceScoped() bool             { return true }
func (r *TaskAgent) New() runtime.Object               { return &TaskAgent{} }
func (r *TaskAgent) NewList() runtime.Object           { return &TaskAgentList{} }
func (r *TaskAgent) GetGroupVersionResource() schema.GroupVersionResource {
	return gvr("taskagents")
}
func (r *TaskAgent) IsStorageVersion() bool                { return true }
func (r *TaskAgent) GetSingularName() string               { return "taskagent" }
func (r *TaskAgent) GetStatus() resource.StatusSubResource { return &r.Status }

func (s *TaskAgentStatus) SubResourceName() string { return "status" }
func (s *TaskAgentStatus) CopyTo(obj resource.ObjectWithStatusSubResource) {
	if p, ok := obj.(*TaskAgent); ok {
		p.Status = *s
	}
}

// DaemonAgent.

func (r *DaemonAgent) GetObjectMeta() *metav1.ObjectMeta { return &r.ObjectMeta }
func (r *DaemonAgent) NamespaceScoped() bool             { return true }
func (r *DaemonAgent) New() runtime.Object               { return &DaemonAgent{} }
func (r *DaemonAgent) NewList() runtime.Object           { return &DaemonAgentList{} }
func (r *DaemonAgent) GetGroupVersionResource() schema.GroupVersionResource {
	return gvr("daemonagents")
}
func (r *DaemonAgent) IsStorageVersion() bool                { return true }
func (r *DaemonAgent) GetSingularName() string               { return "daemonagent" }
func (r *DaemonAgent) GetStatus() resource.StatusSubResource { return &r.Status }

func (s *DaemonAgentStatus) SubResourceName() string { return "status" }
func (s *DaemonAgentStatus) CopyTo(obj resource.ObjectWithStatusSubResource) {
	if p, ok := obj.(*DaemonAgent); ok {
		p.Status = *s
	}
}

// WorkerPool.

func (r *WorkerPool) GetObjectMeta() *metav1.ObjectMeta { return &r.ObjectMeta }
func (r *WorkerPool) NamespaceScoped() bool             { return true }
func (r *WorkerPool) New() runtime.Object               { return &WorkerPool{} }
func (r *WorkerPool) NewList() runtime.Object           { return &WorkerPoolList{} }
func (r *WorkerPool) GetGroupVersionResource() schema.GroupVersionResource {
	return gvr("workerpools")
}
func (r *WorkerPool) IsStorageVersion() bool                { return true }
func (r *WorkerPool) GetSingularName() string               { return "workerpool" }
func (r *WorkerPool) GetStatus() resource.StatusSubResource { return &r.Status }

func (s *WorkerPoolStatus) SubResourceName() string { return "status" }
func (s *WorkerPoolStatus) CopyTo(obj resource.ObjectWithStatusSubResource) {
	if p, ok := obj.(*WorkerPool); ok {
		p.Status = *s
	}
}

// AgentSandboxRevision.

func (r *AgentSandboxRevision) GetObjectMeta() *metav1.ObjectMeta { return &r.ObjectMeta }
func (r *AgentSandboxRevision) NamespaceScoped() bool             { return true }
func (r *AgentSandboxRevision) New() runtime.Object               { return &AgentSandboxRevision{} }
func (r *AgentSandboxRevision) NewList() runtime.Object           { return &AgentSandboxRevisionList{} }
func (r *AgentSandboxRevision) GetGroupVersionResource() schema.GroupVersionResource {
	return gvr("agentsandboxrevisions")
}
func (r *AgentSandboxRevision) IsStorageVersion() bool                { return true }
func (r *AgentSandboxRevision) GetSingularName() string               { return "agentsandboxrevision" }
func (r *AgentSandboxRevision) GetStatus() resource.StatusSubResource { return &r.Status }

func (s *AgentSandboxRevisionStatus) SubResourceName() string { return "status" }
func (s *AgentSandboxRevisionStatus) CopyTo(obj resource.ObjectWithStatusSubResource) {
	if p, ok := obj.(*AgentSandboxRevision); ok {
		p.Status = *s
	}
}

// EgressGateway.

func (r *EgressGateway) GetObjectMeta() *metav1.ObjectMeta { return &r.ObjectMeta }
func (r *EgressGateway) NamespaceScoped() bool             { return true }
func (r *EgressGateway) New() runtime.Object               { return &EgressGateway{} }
func (r *EgressGateway) NewList() runtime.Object           { return &EgressGatewayList{} }
func (r *EgressGateway) GetGroupVersionResource() schema.GroupVersionResource {
	return gvr("egressgateways")
}
func (r *EgressGateway) IsStorageVersion() bool                { return true }
func (r *EgressGateway) GetSingularName() string               { return "egressgateway" }
func (r *EgressGateway) GetStatus() resource.StatusSubResource { return &r.Status }

func (s *EgressGatewayStatus) SubResourceName() string { return "status" }
func (s *EgressGatewayStatus) CopyTo(obj resource.ObjectWithStatusSubResource) {
	if p, ok := obj.(*EgressGateway); ok {
		p.Status = *s
	}
}

// EgressL4Route.

func (r *EgressL4Route) GetObjectMeta() *metav1.ObjectMeta { return &r.ObjectMeta }
func (r *EgressL4Route) NamespaceScoped() bool             { return true }
func (r *EgressL4Route) New() runtime.Object               { return &EgressL4Route{} }
func (r *EgressL4Route) NewList() runtime.Object           { return &EgressL4RouteList{} }
func (r *EgressL4Route) GetGroupVersionResource() schema.GroupVersionResource {
	return gvr("egressl4routes")
}
func (r *EgressL4Route) IsStorageVersion() bool                { return true }
func (r *EgressL4Route) GetSingularName() string               { return "egressl4route" }
func (r *EgressL4Route) GetStatus() resource.StatusSubResource { return &r.Status }

func (s *EgressL4RouteStatus) SubResourceName() string { return "status" }
func (s *EgressL4RouteStatus) CopyTo(obj resource.ObjectWithStatusSubResource) {
	if p, ok := obj.(*EgressL4Route); ok {
		p.Status = *s
	}
}

// MCPRoute.

func (r *MCPRoute) GetObjectMeta() *metav1.ObjectMeta { return &r.ObjectMeta }
func (r *MCPRoute) NamespaceScoped() bool             { return true }
func (r *MCPRoute) New() runtime.Object               { return &MCPRoute{} }
func (r *MCPRoute) NewList() runtime.Object           { return &MCPRouteList{} }
func (r *MCPRoute) GetGroupVersionResource() schema.GroupVersionResource {
	return gvr("mcproutes")
}
func (r *MCPRoute) IsStorageVersion() bool                { return true }
func (r *MCPRoute) GetSingularName() string               { return "mcproute" }
func (r *MCPRoute) GetStatus() resource.StatusSubResource { return &r.Status }

func (s *MCPRouteStatus) SubResourceName() string { return "status" }
func (s *MCPRouteStatus) CopyTo(obj resource.ObjectWithStatusSubResource) {
	if p, ok := obj.(*MCPRoute); ok {
		p.Status = *s
	}
}

// AIProviderRoute.

func (r *AIProviderRoute) GetObjectMeta() *metav1.ObjectMeta { return &r.ObjectMeta }
func (r *AIProviderRoute) NamespaceScoped() bool             { return true }
func (r *AIProviderRoute) New() runtime.Object               { return &AIProviderRoute{} }
func (r *AIProviderRoute) NewList() runtime.Object           { return &AIProviderRouteList{} }
func (r *AIProviderRoute) GetGroupVersionResource() schema.GroupVersionResource {
	return gvr("aiproviderroutes")
}
func (r *AIProviderRoute) IsStorageVersion() bool                { return true }
func (r *AIProviderRoute) GetSingularName() string               { return "aiproviderroute" }
func (r *AIProviderRoute) GetStatus() resource.StatusSubResource { return &r.Status }

func (s *AIProviderRouteStatus) SubResourceName() string { return "status" }
func (s *AIProviderRouteStatus) CopyTo(obj resource.ObjectWithStatusSubResource) {
	if p, ok := obj.(*AIProviderRoute); ok {
		p.Status = *s
	}
}

// CredentialInjectionPolicy (no status subresource).

func (r *CredentialInjectionPolicy) GetObjectMeta() *metav1.ObjectMeta { return &r.ObjectMeta }
func (r *CredentialInjectionPolicy) NamespaceScoped() bool             { return true }
func (r *CredentialInjectionPolicy) New() runtime.Object               { return &CredentialInjectionPolicy{} }
func (r *CredentialInjectionPolicy) NewList() runtime.Object {
	return &CredentialInjectionPolicyList{}
}
func (r *CredentialInjectionPolicy) GetGroupVersionResource() schema.GroupVersionResource {
	return gvr("credentialinjectionpolicies")
}
func (r *CredentialInjectionPolicy) IsStorageVersion() bool  { return true }
func (r *CredentialInjectionPolicy) GetSingularName() string { return "credentialinjectionpolicy" }

// RateLimitPolicy (no status subresource).

func (r *RateLimitPolicy) GetObjectMeta() *metav1.ObjectMeta { return &r.ObjectMeta }
func (r *RateLimitPolicy) NamespaceScoped() bool             { return true }
func (r *RateLimitPolicy) New() runtime.Object               { return &RateLimitPolicy{} }
func (r *RateLimitPolicy) NewList() runtime.Object           { return &RateLimitPolicyList{} }
func (r *RateLimitPolicy) GetGroupVersionResource() schema.GroupVersionResource {
	return gvr("ratelimitpolicies")
}
func (r *RateLimitPolicy) IsStorageVersion() bool  { return true }
func (r *RateLimitPolicy) GetSingularName() string { return "ratelimitpolicy" }

// LoggingPolicy (no status subresource).

func (r *LoggingPolicy) GetObjectMeta() *metav1.ObjectMeta { return &r.ObjectMeta }
func (r *LoggingPolicy) NamespaceScoped() bool             { return true }
func (r *LoggingPolicy) New() runtime.Object               { return &LoggingPolicy{} }
func (r *LoggingPolicy) NewList() runtime.Object           { return &LoggingPolicyList{} }
func (r *LoggingPolicy) GetGroupVersionResource() schema.GroupVersionResource {
	return gvr("loggingpolicies")
}
func (r *LoggingPolicy) IsStorageVersion() bool  { return true }
func (r *LoggingPolicy) GetSingularName() string { return "loggingpolicy" }

// EgressDenyPolicy (no status subresource).

func (r *EgressDenyPolicy) GetObjectMeta() *metav1.ObjectMeta { return &r.ObjectMeta }
func (r *EgressDenyPolicy) NamespaceScoped() bool             { return true }
func (r *EgressDenyPolicy) New() runtime.Object               { return &EgressDenyPolicy{} }
func (r *EgressDenyPolicy) NewList() runtime.Object           { return &EgressDenyPolicyList{} }
func (r *EgressDenyPolicy) GetGroupVersionResource() schema.GroupVersionResource {
	return gvr("egressdenypolicies")
}
func (r *EgressDenyPolicy) IsStorageVersion() bool  { return true }
func (r *EgressDenyPolicy) GetSingularName() string { return "egressdenypolicy" }
