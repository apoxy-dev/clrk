package controller

import (
	"context"
	"fmt"

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

	logger.Info("Reconciled EgressGateway", "eg", req.NamespacedName)
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
// needed. No owner reference because the class is cluster-scoped and
// shared across all EgressGateways.
func (r *EgressGatewayReconciler) ensureGatewayClass(ctx context.Context) error {
	gc := &gwapiv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: EgressGatewayClassName}}
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, gc, func() error {
		gc.Spec.ControllerName = gwapiv1.GatewayController("gateway.envoyproxy.io/gatewayclass-controller")
		gc.Spec.ParametersRef = &gwapiv1.ParametersReference{
			Group:     gwapiv1.Group(egv1alpha1.GroupVersion.Group),
			Kind:      gwapiv1.Kind(egv1alpha1.KindEnvoyProxy),
			Name:      "clrk-egress",
			Namespace: ptrNamespace("default"),
		}
		return nil
	})
	return err
}

// ensureGateway creates or updates the Gateway referencing our class with a
// single TCP listener. The extension server picks listeners owned by clrk
// via the "clrk-eg-*" name prefix.
func (r *EgressGatewayReconciler) ensureGateway(ctx context.Context, eg *clrkv1alpha1.EgressGateway) error {
	gw := &gwapiv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GatewayNamePrefix + eg.Name,
			Namespace: eg.Namespace,
		},
	}
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, gw, func() error {
		gw.Spec.GatewayClassName = gwapiv1.ObjectName(EgressGatewayClassName)
		gw.Spec.Listeners = []gwapiv1.Listener{
			{
				Name:     "egress",
				Port:     gwapiv1.PortNumber(EgressListenerPort),
				Protocol: gwapiv1.TCPProtocolType,
			},
		}
		return ctrl.SetControllerReference(eg, gw, r.Scheme)
	})
	return err
}

func envoyProxyName(egName string) string { return "clrk-eg-envoyproxy-" + egName }

func ptrNamespace(ns string) *gwapiv1.Namespace {
	n := gwapiv1.Namespace(ns)
	return &n
}
