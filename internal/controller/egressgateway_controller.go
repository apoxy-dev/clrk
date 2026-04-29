package controller

import (
	"context"
	"fmt"
	"time"

	egv1alpha1 "github.com/envoyproxy/gateway/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

const (
	// EgressGatewayClassName is the shared GatewayClass we install for all
	// clrk EgressGateways. Envoy Gateway's Gateway controller binds Gateways
	// that reference this class to the clrk EnvoyProxy configuration.
	EgressGatewayClassName = "clrk-egress"

	// GatewayNamePrefix must match internal/egextension.GatewayNamePrefix.
	// Kept as a constant here to avoid importing the extension package into
	// the reconciler layer.
	GatewayNamePrefix = "clrk-eg-"

	// EgressListenerPort is the fixed TCP port that each clrk egress
	// listener binds. Workers dial this port on the EG Service with a
	// PROXY v2 header for every outbound sandbox connection.
	EgressListenerPort int32 = 18080
)

// EgressGatewayReconciler provisions Envoy Gateway infrastructure for each
// clrk EgressGateway: a shared GatewayClass, an EnvoyProxy referencing our
// private Envoy image, a Gateway with one TCP listener, and the per-EG CA
// Secret consumed by both the sandbox trust store and the handshaker.
type EgressGatewayReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// EnvoyImage is the container image used for the EG-managed Envoy
	// fleet. Must contain the grpc_certificate_provider handshaker
	// extension. Set from --envoy-image on controller-manager.
	EnvoyImage string

	// DevBackendHost, when non-empty, overrides Status.EgressBackendAddress
	// to use this host with the EG Service NodePort instead of the
	// in-cluster DNS name. Set by `clrk dev` to the docker hostname of the
	// k3s container ("clrk-k3s") because workers run on the docker network
	// and can't route to k3s ClusterIPs.
	DevBackendHost string
}

// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=egressgateways,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=egressgateways/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=gateway.envoyproxy.io,resources=envoyproxies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gatewayclasses,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways,verbs=get;list;watch;create;update;patch;delete

func (r *EgressGatewayReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var eg clrkv1alpha1.EgressGateway
	if err := r.Get(ctx, req.NamespacedName, &eg); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if err := r.ensureCASecret(ctx, &eg); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensuring CA secret: %w", err)
	}

	if err := r.ensureEnvoyProxy(ctx, &eg); err != nil {
		if meta.IsNoMatchError(err) {
			logger.V(1).Info("EnvoyProxy CRD not installed, skipping EnvoyProxy reconciliation")
		} else {
			return ctrl.Result{}, fmt.Errorf("ensuring EnvoyProxy: %w", err)
		}
	}

	if err := r.ensureGatewayClass(ctx); err != nil {
		if meta.IsNoMatchError(err) {
			logger.V(1).Info("Gateway API CRDs not installed, skipping Gateway reconciliation")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("ensuring GatewayClass: %w", err)
	}

	if err := r.ensureGateway(ctx, &eg); err != nil {
		if meta.IsNoMatchError(err) {
			logger.V(1).Info("Gateway API CRDs not installed, skipping Gateway reconciliation")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("ensuring Gateway: %w", err)
	}

	// EG-managed Service is created asynchronously by Envoy Gateway after it
	// observes our Gateway. Re-queue when it isn't there yet so workers see
	// EgressBackendAddress as soon as the data plane is provisioned.
	requeue, err := r.updateBackendAddress(ctx, &eg)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("updating backend address: %w", err)
	}

	logger.Info("Reconciled EgressGateway", "eg", req.NamespacedName)
	if requeue {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func (r *EgressGatewayReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Do not Own EnvoyProxy / Gateway here — their CRDs may not be
	// present in the cluster (e.g. clrk dev without Envoy Gateway
	// installed) and Owns() would fail manager setup. We still re-
	// reconcile on CA Secret changes so expiry or operator actions
	// on the Secret trigger mint logic.
	return ctrl.NewControllerManagedBy(mgr).
		For(&clrkv1alpha1.EgressGateway{}).
		Owns(&corev1.Secret{}).
		Named("egressgateway").
		Complete(r)
}


// ensureCASecret mints the per-EgressGateway MITM CA if one doesn't exist.
// The Secret is owned by the EgressGateway so deletion cascades cleanly.
func (r *EgressGatewayReconciler) ensureCASecret(ctx context.Context, eg *clrkv1alpha1.EgressGateway) error {
	name := EgressGatewayCASecretName(eg.Name)
	key := types.NamespacedName{Name: name, Namespace: eg.Namespace}

	var existing corev1.Secret
	err := r.Get(ctx, key, &existing)
	switch {
	case err == nil:
		return nil
	case !apierrors.IsNotFound(err):
		return err
	}

	certPEM, keyPEM, err := MintEgressGatewayCA(eg.Namespace, eg.Name)
	if err != nil {
		return err
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: eg.Namespace,
			Labels:    map[string]string{EgressGatewayCASecretLabel: eg.Name},
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       certPEM,
			corev1.TLSPrivateKeyKey: keyPEM,
		},
	}
	if err := ctrl.SetControllerReference(eg, secret, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// ensureEnvoyProxy creates or updates the EnvoyProxy CR that pins our
// private Envoy image for EG-managed pods backing this gateway. Each
// EgressGateway gets its own EnvoyProxy; the extension manager (which
// points at controller-manager's gRPC server) is configured operator-wide
// on Envoy Gateway itself, not per-EnvoyProxy.
func (r *EgressGatewayReconciler) ensureEnvoyProxy(ctx context.Context, eg *clrkv1alpha1.EgressGateway) error {
	name := envoyProxyName(eg.Name)
	img := r.EnvoyImage

	ep := &egv1alpha1.EnvoyProxy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: eg.Namespace,
		},
	}
	op, err := ctrl.CreateOrUpdate(ctx, r.Client, ep, func() error {
		if img != "" {
			ep.Spec.Provider = &egv1alpha1.EnvoyProxyProvider{
				Type: egv1alpha1.ProviderTypeKubernetes,
				Kubernetes: &egv1alpha1.EnvoyProxyKubernetesProvider{
					EnvoyDeployment: &egv1alpha1.KubernetesDeploymentSpec{
						Container: &egv1alpha1.KubernetesContainerSpec{Image: &img},
					},
				},
			}
		}
		return ctrl.SetControllerReference(eg, ep, r.Scheme)
	})
	if err != nil {
		return err
	}
	log.FromContext(ctx).V(1).Info("EnvoyProxy reconciled", "op", op, "name", name)
	return nil
}

// ensureGatewayClass creates the shared GatewayClass the first time it's
// needed. No parametersRef — each Gateway carries its own
// infrastructure.parametersRef pointing at the per-EG EnvoyProxy CR so
// the private-Envoy image override applies per-EgressGateway.
func (r *EgressGatewayReconciler) ensureGatewayClass(ctx context.Context) error {
	gc := &gwapiv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: EgressGatewayClassName}}
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, gc, func() error {
		gc.Spec.ControllerName = gwapiv1.GatewayController("gateway.envoyproxy.io/gatewayclass-controller")
		gc.Spec.ParametersRef = nil
		return nil
	})
	return err
}

// ensureGateway creates or updates the Gateway referencing our class with a
// single HTTPS listener. We use HTTPS (not raw TCP) so EG generates an
// HCM-shaped listener that PostHTTPListenerModify can mutate — TCP
// listeners get no filter chains and no extension hook fires for them.
//
// The TLS certificateRef points at the per-EG CA Secret as a placeholder;
// the extension swaps DownstreamTlsContext.CommonTlsContext.CustomHandshaker
// in for the gRPC certificate provider so leaves are minted on the fly per
// SNI and the placeholder cert is never actually presented.
//
// The Gateway carries an infrastructure parametersRef so EG provisions
// Envoy with the per-EG EnvoyProxy spec (which pins our private image
// containing the grpc_certificate_provider handshaker extension).
func (r *EgressGatewayReconciler) ensureGateway(ctx context.Context, eg *clrkv1alpha1.EgressGateway) error {
	gw := &gwapiv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GatewayNamePrefix + eg.Name,
			Namespace: eg.Namespace,
		},
	}
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, gw, func() error {
		caSecret := gwapiv1.ObjectName(EgressGatewayCASecretName(eg.Name))
		tlsMode := gwapiv1.TLSModeTerminate
		allRoutes := gwapiv1.NamespacesFromAll
		gw.Spec.GatewayClassName = gwapiv1.ObjectName(EgressGatewayClassName)
		gw.Spec.Listeners = []gwapiv1.Listener{
			{
				Name:     "egress",
				Port:     gwapiv1.PortNumber(EgressListenerPort),
				Protocol: gwapiv1.HTTPSProtocolType,
				TLS: &gwapiv1.GatewayTLSConfig{
					Mode: &tlsMode,
					CertificateRefs: []gwapiv1.SecretObjectReference{
						{Name: caSecret},
					},
				},
				AllowedRoutes: &gwapiv1.AllowedRoutes{
					Namespaces: &gwapiv1.RouteNamespaces{From: &allRoutes},
				},
			},
		}
		gw.Spec.Infrastructure = &gwapiv1.GatewayInfrastructure{
			ParametersRef: &gwapiv1.LocalParametersReference{
				Group: gwapiv1.Group(egv1alpha1.GroupVersion.Group),
				Kind:  gwapiv1.Kind(egv1alpha1.KindEnvoyProxy),
				Name:  envoyProxyName(eg.Name),
			},
		}
		return ctrl.SetControllerReference(eg, gw, r.Scheme)
	})
	if err != nil {
		return err
	}
	return r.ensureCatchAllRoute(ctx, eg)
}

// ensureCatchAllRoute creates a placeholder HTTPRoute on the Gateway so EG
// has at least one accepted route and produces an HCM filter chain for our
// extension to rewrite. The route's backendRef points at a synthetic
// service that our PostTranslateModify replaces with a
// dynamic_forward_proxy cluster — the backend is never actually dialed.
func (r *EgressGatewayReconciler) ensureCatchAllRoute(ctx context.Context, eg *clrkv1alpha1.EgressGateway) error {
	rt := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GatewayNamePrefix + eg.Name + "-catchall",
			Namespace: eg.Namespace,
		},
	}
	port := gwapiv1.PortNumber(80)
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, rt, func() error {
		rt.Spec.ParentRefs = []gwapiv1.ParentReference{{
			Name: gwapiv1.ObjectName(GatewayNamePrefix + eg.Name),
		}}
		rt.Spec.Rules = []gwapiv1.HTTPRouteRule{{
			BackendRefs: []gwapiv1.HTTPBackendRef{{
				BackendRef: gwapiv1.BackendRef{
					BackendObjectReference: gwapiv1.BackendObjectReference{
						Name: gwapiv1.ObjectName(GatewayNamePrefix + eg.Name + "-dfp-placeholder"),
						Port: &port,
					},
				},
			}},
		}}
		return ctrl.SetControllerReference(eg, rt, r.Scheme)
	})
	return err
}

func envoyProxyName(egName string) string { return "clrk-eg-envoyproxy-" + egName }

const envoyGatewayNamespace = "envoy-gateway-system"

// updateBackendAddress publishes the host:port workers should dial to
// reach this EgressGateway's data plane. Returns requeue=true when the
// EG-managed Service hasn't materialized yet so the next reconcile picks
// it up without waiting for an unrelated event.
func (r *EgressGatewayReconciler) updateBackendAddress(ctx context.Context, eg *clrkv1alpha1.EgressGateway) (bool, error) {
	gwName := GatewayNamePrefix + eg.Name

	var svcs corev1.ServiceList
	if err := r.List(ctx, &svcs,
		client.InNamespace(envoyGatewayNamespace),
		client.MatchingLabels{
			"gateway.envoyproxy.io/owning-gateway-name":      gwName,
			"gateway.envoyproxy.io/owning-gateway-namespace": eg.Namespace,
		},
	); err != nil {
		return false, fmt.Errorf("listing EG-managed services: %w", err)
	}
	if len(svcs.Items) == 0 {
		return true, nil
	}
	svc := &svcs.Items[0]

	addr := r.computeBackendAddress(svc)
	if addr == "" {
		return true, nil
	}
	if eg.Status.EgressBackendAddress == addr {
		return false, nil
	}
	eg.Status.EgressBackendAddress = addr
	if err := r.Status().Update(ctx, eg); err != nil {
		return false, fmt.Errorf("updating status: %w", err)
	}
	return false, nil
}

// computeBackendAddress picks the right form depending on whether we're
// running under `clrk dev` (workers on docker network — must use NodePort
// on the dev host) or in-cluster (use the Service's cluster DNS name).
func (r *EgressGatewayReconciler) computeBackendAddress(svc *corev1.Service) string {
	port := r.findEgressPort(svc)
	if port == nil {
		return ""
	}
	if r.DevBackendHost != "" {
		if port.NodePort == 0 {
			return ""
		}
		return fmt.Sprintf("%s:%d", r.DevBackendHost, port.NodePort)
	}
	return fmt.Sprintf("%s.%s.svc.cluster.local:%d", svc.Name, svc.Namespace, port.Port)
}

func (r *EgressGatewayReconciler) findEgressPort(svc *corev1.Service) *corev1.ServicePort {
	for i := range svc.Spec.Ports {
		if svc.Spec.Ports[i].Port == EgressListenerPort {
			return &svc.Spec.Ports[i]
		}
	}
	return nil
}
