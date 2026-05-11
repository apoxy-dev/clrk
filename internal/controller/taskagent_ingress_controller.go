package controller

import (
	"context"
	"fmt"

	egv1alpha1 "github.com/envoyproxy/gateway/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
	gwapiv1a2 "sigs.k8s.io/gateway-api/apis/v1alpha2"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/ports"
)

// TaskAgentIngressReconciler owns the k8s-side half of TaskAgent.
//
// Per-TaskAgent it materializes:
//   - a Gateway (one HTTP listener) and HTTPRoute (per-TA path tree)
//   - an EG `Backend` of `type: DynamicResolver` — EG generates a
//     dynamic_forward_proxy cluster that dials whatever `:authority`
//     the request carries, so the HTTPRoute backendRef is this object
//     instead of a static Service
//   - an `EnvoyExtensionPolicy` attaching the controller-manager's
//     ingress ext_proc onto the per-TA HTTPRoute
//
// Per TaskAgent's namespace it also materializes a shared
// `clrk-ingress-extproc` Backend pointing at the controller-manager's
// ingress ext_proc gRPC service (FQDN + port from
// IngressExtProc{Host,Port}). One Backend per namespace keeps the
// ExtensionPolicy backendRef short and re-targetable from a single
// place.
type TaskAgentIngressReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// IngressExtProcHost / IngressExtProcPort point at the
	// controller-manager's ingress ext_proc gRPC endpoint. Wired from
	// `--ingress-extproc-host` / `--ingress-extproc-port`.
	IngressExtProcHost string
	IngressExtProcPort int32
}

// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gatewayclasses,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=gateway.envoyproxy.io,resources=backends;envoyextensionpolicies;envoyproxies,verbs=get;list;watch;create;update;patch

func (r *TaskAgentIngressReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var ta clrkv1alpha1.TaskAgent
	if err := r.Get(ctx, req.NamespacedName, &ta); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	taStatusBase := ta.DeepCopy()

	// Cluster-wide GatewayClass — reconciled out-of-band so a TA
	// applied before the controller bootstraps still finds the class
	// when it lands. Idempotent SSA, no owner ref.
	if err := r.ensureGatewayClass(ctx); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling GatewayClass: %w", err)
	}

	// Per-TA EnvoyProxy with a Service-name override so the data
	// plane is addressable as `clrk-<ta>.<ns>.svc`. Reconciled
	// before the Gateway so the Gateway's parametersRef can resolve.
	ep := &egv1alpha1.EnvoyProxy{ObjectMeta: metav1.ObjectMeta{Name: taskAgentEnvoyProxyName(&ta), Namespace: ta.Namespace}}
	if err := createOrUpdateWithRetry(ctx, r.Client, ep, func() error {
		desired := desiredEnvoyProxy(&ta)
		ep.Labels = desired.Labels
		ep.Spec = desired.Spec
		return ctrl.SetControllerReference(&ta, ep, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling EnvoyProxy: %w", err)
	}

	gw := &gwapiv1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: ta.Name, Namespace: ta.Namespace}}
	if err := createOrUpdateWithRetry(ctx, r.Client, gw, func() error {
		desired := desiredGateway(&ta)
		gw.Labels = desired.Labels
		gw.Spec = desired.Spec
		return ctrl.SetControllerReference(&ta, gw, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling Gateway: %w", err)
	}

	gatewayReady := metav1.Condition{
		Type:               condGatewayReady,
		ObservedGeneration: ta.Generation,
		LastTransitionTime: metav1.Now(),
	}
	if gatewayProgrammed(gw) {
		gatewayReady.Status = metav1.ConditionTrue
		gatewayReady.Reason = "GatewayProgrammed"
		gatewayReady.Message = "Gateway has Programmed=True"
	} else {
		gatewayReady.Status = metav1.ConditionFalse
		gatewayReady.Reason = "GatewayNotProgrammed"
		gatewayReady.Message = "Gateway exists but Programmed != True"
	}
	meta.SetStatusCondition(&ta.Status.Conditions, gatewayReady)

	// Per-TA DynamicResolver Backend. EG generates a
	// dynamic_forward_proxy cluster with no static endpoints; the
	// host:port to dial comes from the request `:authority`, which
	// the ingress ext_proc rewrites per request to the worker pod
	// picked by WorkerHealthChecker.
	dynBackend := &egv1alpha1.Backend{ObjectMeta: metav1.ObjectMeta{Name: dynamicBackendName(&ta), Namespace: ta.Namespace}}
	if err := createOrUpdateWithRetry(ctx, r.Client, dynBackend, func() error {
		desired := desiredDynamicResolverBackend(&ta)
		dynBackend.Labels = desired.Labels
		dynBackend.Spec = desired.Spec
		return ctrl.SetControllerReference(&ta, dynBackend, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling DynamicResolver Backend: %w", err)
	}

	// Per-namespace shared Backend pointing at the controller-manager's
	// ingress ext_proc gRPC endpoint. Reconciled by every TaskAgent in
	// the namespace; idempotent SSA. Owner ref left blank so the
	// Backend survives any single TA's deletion (other TAs in the
	// namespace still need it).
	if err := r.reconcileExtProcBackend(ctx, ta.Namespace); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling ingress ext_proc Backend: %w", err)
	}

	hr := &gwapiv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Name: ta.Name, Namespace: ta.Namespace}}
	if err := createOrUpdateWithRetry(ctx, r.Client, hr, func() error {
		desired := desiredHTTPRoute(&ta)
		hr.Labels = desired.Labels
		hr.Spec = desired.Spec
		return ctrl.SetControllerReference(&ta, hr, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling HTTPRoute: %w", err)
	}

	// Per-TA EnvoyExtensionPolicy attaching the ingress ext_proc onto
	// the per-TA HTTPRoute.
	pol := &egv1alpha1.EnvoyExtensionPolicy{ObjectMeta: metav1.ObjectMeta{Name: extProcPolicyName(&ta), Namespace: ta.Namespace}}
	if err := createOrUpdateWithRetry(ctx, r.Client, pol, func() error {
		desired := desiredExtProcPolicy(&ta, r.IngressExtProcPort)
		pol.Labels = desired.Labels
		pol.Spec = desired.Spec
		return ctrl.SetControllerReference(&ta, pol, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling EnvoyExtensionPolicy: %w", err)
	}

	if err := r.Status().Patch(ctx, &ta, client.MergeFrom(taStatusBase)); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	return ctrl.Result{}, nil
}

// reconcileExtProcBackend ensures a per-namespace EG Backend pointing
// at the controller-manager's ingress ext_proc gRPC endpoint exists.
// FQDN is preferred (in-cluster Service DNS); IP also accepted via
// flag. Owner-ref-less so the Backend stays around as long as any
// TaskAgent in the namespace needs it; orphan cleanup is a follow-up.
func (r *TaskAgentIngressReconciler) reconcileExtProcBackend(ctx context.Context, namespace string) error {
	if r.IngressExtProcHost == "" || r.IngressExtProcPort == 0 {
		return fmt.Errorf("ingress ext_proc host/port not configured")
	}
	be := &egv1alpha1.Backend{ObjectMeta: metav1.ObjectMeta{Name: IngressExtProcBackendName, Namespace: namespace}}
	return createOrUpdateWithRetry(ctx, r.Client, be, func() error {
		be.Labels = map[string]string{
			labelComponent: "ingress-extproc",
		}
		be.Spec = egv1alpha1.BackendSpec{
			Endpoints: []egv1alpha1.BackendEndpoint{
				{
					FQDN: &egv1alpha1.FQDNEndpoint{
						Hostname: r.IngressExtProcHost,
						Port:     r.IngressExtProcPort,
					},
				},
			},
		}
		return nil
	})
}

func gatewayProgrammed(gw *gwapiv1.Gateway) bool {
	for _, cond := range gw.Status.Conditions {
		if cond.Type == string(gwapiv1.GatewayConditionProgrammed) && cond.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func (r *TaskAgentIngressReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("taskagent-ingress").
		For(&clrkv1alpha1.TaskAgent{}).
		Owns(&gwapiv1.Gateway{}).
		Owns(&gwapiv1.HTTPRoute{}).
		Owns(&egv1alpha1.Backend{}).
		Owns(&egv1alpha1.EnvoyExtensionPolicy{}).
		Owns(&egv1alpha1.EnvoyProxy{}).
		Complete(r)
}

// ensureGatewayClass installs the cluster-scoped clrk-taskagent
// GatewayClass that points at Envoy Gateway's controller. Idempotent;
// the class survives across TA reconciles. No owner ref — the class
// is shared by every TaskAgent in the cluster.
func (r *TaskAgentIngressReconciler) ensureGatewayClass(ctx context.Context) error {
	gc := &gwapiv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: clrkTaskAgentGatewayClassName}}
	return createOrUpdateWithRetry(ctx, r.Client, gc, func() error {
		gc.Spec.ControllerName = gwapiv1.GatewayController("gateway.envoyproxy.io/gatewayclass-controller")
		return nil
	})
}

func dynamicBackendName(ta *clrkv1alpha1.TaskAgent) string    { return ta.Name + "-resolver" }
func extProcPolicyName(ta *clrkv1alpha1.TaskAgent) string     { return ta.Name + "-ext-proc" }
func taskAgentEnvoyProxyName(ta *clrkv1alpha1.TaskAgent) string { return "clrk-" + ta.Name }

// desiredEnvoyProxy materializes the per-TaskAgent EnvoyProxy. The
// EnvoyService.Name override is what makes the data-plane Service
// land as `clrk-<ta>` instead of EG's default
// `envoy-<gateway-name>-<hash>` shape. mergeGateways=false is set
// explicitly to defend against the cluster-wide default flipping.
func desiredEnvoyProxy(ta *clrkv1alpha1.TaskAgent) *egv1alpha1.EnvoyProxy {
	svcType := egv1alpha1.ServiceTypeClusterIP
	return &egv1alpha1.EnvoyProxy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskAgentEnvoyProxyName(ta),
			Namespace: ta.Namespace,
			Labels: map[string]string{
				labelAgent:     ta.Name,
				labelComponent: "ingress",
			},
		},
		Spec: egv1alpha1.EnvoyProxySpec{
			MergeGateways: ptr.To(false),
			Provider: &egv1alpha1.EnvoyProxyProvider{
				Type: egv1alpha1.ProviderTypeKubernetes,
				Kubernetes: &egv1alpha1.EnvoyProxyKubernetesProvider{
					EnvoyService: &egv1alpha1.KubernetesServiceSpec{
						Name: ptr.To(taskAgentEnvoyProxyName(ta)),
						Type: &svcType,
					},
				},
			},
		},
	}
}

func desiredDynamicResolverBackend(ta *clrkv1alpha1.TaskAgent) *egv1alpha1.Backend {
	dr := egv1alpha1.BackendTypeDynamicResolver
	return &egv1alpha1.Backend{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dynamicBackendName(ta),
			Namespace: ta.Namespace,
			Labels: map[string]string{
				labelAgent:     ta.Name,
				labelComponent: "ingress",
			},
		},
		Spec: egv1alpha1.BackendSpec{
			Type: &dr,
		},
	}
}

func desiredExtProcPolicy(ta *clrkv1alpha1.TaskAgent, extProcPort int32) *egv1alpha1.EnvoyExtensionPolicy {
	beGroup := gwapiv1.Group(envoyGatewayGroup)
	beKind := gwapiv1.Kind(egv1alpha1.KindBackend)
	beName := gwapiv1.ObjectName(IngressExtProcBackendName)
	beNs := gwapiv1.Namespace(ta.Namespace)
	// EG's CRD validates port >= 1 even when Kind=Backend resolves the
	// real endpoint internally. Stamp the controller-manager's ingress
	// ext_proc port so the schema check passes; the value is otherwise
	// not the source of truth (the Backend's FQDN.Port wins).
	bePort := gwapiv1.PortNumber(extProcPort)

	streamed := egv1alpha1.StreamedExtProcBodyProcessingMode

	return &egv1alpha1.EnvoyExtensionPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      extProcPolicyName(ta),
			Namespace: ta.Namespace,
			Labels: map[string]string{
				labelAgent:     ta.Name,
				labelComponent: "ingress",
			},
		},
		Spec: egv1alpha1.EnvoyExtensionPolicySpec{
			PolicyTargetReferences: egv1alpha1.PolicyTargetReferences{
				TargetRefs: []gwapiv1a2.LocalPolicyTargetReferenceWithSectionName{
					{
						LocalPolicyTargetReference: gwapiv1a2.LocalPolicyTargetReference{
							Group: gwapiv1a2.Group("gateway.networking.k8s.io"),
							Kind:  gwapiv1a2.Kind("HTTPRoute"),
							Name:  gwapiv1a2.ObjectName(ta.Name),
						},
					},
				},
			},
			ExtProc: []egv1alpha1.ExtProc{
				{
					BackendCluster: egv1alpha1.BackendCluster{
						BackendRefs: []egv1alpha1.BackendRef{
							{
								BackendObjectReference: gwapiv1.BackendObjectReference{
									Group:     &beGroup,
									Kind:      &beKind,
									Name:      beName,
									Namespace: &beNs,
									Port:      &bePort,
								},
							},
						},
					},
					ProcessingMode: &egv1alpha1.ExtProcProcessingMode{
						Request: &egv1alpha1.ProcessingModeOptions{
							// Streamed body so request payloads pass through
							// without buffering — the dispatcher pumps stdin
							// of the sandbox and we don't want to wait on
							// full-body buffering for endpoint selection.
							Body: &streamed,
						},
					},
				},
			},
		},
	}
}

func desiredGateway(ta *clrkv1alpha1.TaskAgent) *gwapiv1.Gateway {
	return &gwapiv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ta.Name,
			Namespace: ta.Namespace,
			Labels: map[string]string{
				labelAgent:     ta.Name,
				labelComponent: "ingress",
			},
		},
		Spec: gwapiv1.GatewaySpec{
			GatewayClassName: gwapiv1.ObjectName(clrkTaskAgentGatewayClassName),
			Listeners: []gwapiv1.Listener{
				{
					Name:     "http",
					Port:     80,
					Protocol: gwapiv1.HTTPProtocolType,
				},
			},
			// Point at the per-TA EnvoyProxy so EG materializes the
			// data plane as a dedicated Deployment + Service per TA
			// (see desiredEnvoyProxy).
			Infrastructure: &gwapiv1.GatewayInfrastructure{
				ParametersRef: &gwapiv1.LocalParametersReference{
					Group: gwapiv1.Group(envoyGatewayGroup),
					Kind:  gwapiv1.Kind("EnvoyProxy"),
					Name:  taskAgentEnvoyProxyName(ta),
				},
			},
		},
	}
}

func desiredHTTPRoute(ta *clrkv1alpha1.TaskAgent) *gwapiv1.HTTPRoute {
	pathPrefix := gwapiv1.PathMatchPathPrefix

	// BackendRef is the per-TA DynamicResolver Backend. EG generates
	// a dynamic_forward_proxy cluster with no static endpoints; the
	// destination host is read off the request `:authority`, which
	// the ingress ext_proc rewrites to the worker pod IP picked by
	// WorkerHealthChecker. The X-Clrk-TaskAgent header injected here
	// gives the worker dispatcher its lookup key on the receiving
	// end (ext_proc has set `:authority` to a pod IP, so the worker
	// can't infer the TaskAgent from Host).
	beGroup := gwapiv1.Group(envoyGatewayGroup)
	beKind := gwapiv1.Kind("Backend")
	beName := gwapiv1.ObjectName(dynamicBackendName(ta))

	// Peg the route timeout to spec.timeout so cold-start LLM agents
	// (Claude Code, etc.) don't get cut off by Envoy's 15s HTTPRoute
	// default. The API server defaults Timeout via the +kubebuilder
	// tag, so it's always set by the time reconcile observes the object.
	routeTimeout := gwapiv1.Duration(ta.Spec.Timeout.Duration.String())

	return &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ta.Name,
			Namespace: ta.Namespace,
			Labels: map[string]string{
				labelAgent:     ta.Name,
				labelComponent: "ingress",
			},
		},
		Spec: gwapiv1.HTTPRouteSpec{
			CommonRouteSpec: gwapiv1.CommonRouteSpec{
				ParentRefs: []gwapiv1.ParentReference{
					{
						Name: gwapiv1.ObjectName(ta.Name),
					},
				},
			},
			Rules: []gwapiv1.HTTPRouteRule{
				{
					Timeouts: &gwapiv1.HTTPRouteTimeouts{
						Request:        &routeTimeout,
						BackendRequest: &routeTimeout,
					},
					Matches: []gwapiv1.HTTPRouteMatch{
						{
							Path: &gwapiv1.HTTPPathMatch{
								Type:  &pathPrefix,
								Value: ptr.To("/"),
							},
						},
					},
					Filters: []gwapiv1.HTTPRouteFilter{
						{
							Type: gwapiv1.HTTPRouteFilterRequestHeaderModifier,
							RequestHeaderModifier: &gwapiv1.HTTPHeaderFilter{
								Set: []gwapiv1.HTTPHeader{
									{
										Name:  ports.HeaderTaskAgent,
										Value: ta.Namespace + "/" + ta.Name,
									},
									{
										Name:  ports.HeaderTrigger,
										Value: "http",
									},
								},
							},
						},
					},
					BackendRefs: []gwapiv1.HTTPBackendRef{
						{
							BackendRef: gwapiv1.BackendRef{
								BackendObjectReference: gwapiv1.BackendObjectReference{
									Group: &beGroup,
									Kind:  &beKind,
									Name:  beName,
								},
							},
						},
					},
				},
			},
		},
	}
}
