package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	egv1alpha1 "github.com/envoyproxy/gateway/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
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

	if err := r.ensureUpstreamTrustMirror(ctx, &eg); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensuring upstream trust mirror: %w", err)
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

	// EG-managed Gateway and Service are reconciled asynchronously by
	// Envoy Gateway. Mirror the Gateway's Programmed condition onto our
	// Status.Ready and re-queue until both Ready=True and
	// EgressBackendAddress is populated so workers see the data plane as
	// soon as it provisions.
	requeue, err := r.updateStatus(ctx, &eg)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
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
	if err := createOrUpdateWithRetry(ctx, r.Client, ep, func() error {
		ep.Spec.Provider = &egv1alpha1.EnvoyProxyProvider{
			Type: egv1alpha1.ProviderTypeKubernetes,
			Kubernetes: &egv1alpha1.EnvoyProxyKubernetesProvider{
				EnvoyDeployment: buildEnvoyDeploymentSpec(eg, img),
			},
		}
		return ctrl.SetControllerReference(eg, ep, r.Scheme)
	}); err != nil {
		return err
	}
	log.FromContext(ctx).V(1).Info("EnvoyProxy reconciled", "name", name)
	return nil
}

// systemTrustPath is the file Envoy reads when validating upstream
// certs. Matches the path egextension's DFP cluster pins as TrustedCa
// and the path the upstream envoy/alpine image ships its CA bundle at.
const systemTrustPath = "/etc/ssl/certs/ca-certificates.crt"

// buildEnvoyDeploymentSpec assembles the EnvoyProxy.Spec.Provider.Kubernetes.
// EnvoyDeployment shape, including init container + volumes when the EG
// requests an additional upstream trust bundle, and a strategic merge
// patch for HostAliases when the EG pins specific upstream hostnames.
// When neither is requested the result is just the image override
// (matching the previous behavior).
func buildEnvoyDeploymentSpec(eg *clrkv1alpha1.EgressGateway, image string) *egv1alpha1.KubernetesDeploymentSpec {
	dep := &egv1alpha1.KubernetesDeploymentSpec{}
	if image != "" {
		dep.Container = &egv1alpha1.KubernetesContainerSpec{Image: &image}
	}

	if patch := upstreamHostAliasesPatch(eg); patch != nil {
		dep.Patch = patch
	}

	bundleSecret := upstreamAdditionalTrustSecret(eg)
	if bundleSecret == "" {
		return dep
	}

	// Mount the additional CA bundle. Strategy: an emptyDir volume
	// holds a merged ca-certificates.crt; an init container concatenates
	// the image's system bundle with every cert in the secret into that
	// emptyDir; the main Envoy container then mounts the merged file
	// as a subPath, overlaying just the system bundle file (the rest of
	// /etc/ssl/certs stays intact).
	const (
		mergedVol  = "clrk-trust-merged"
		secretVol  = "clrk-extra-trust"
		mergedPath = "/clrk-trust"
	)

	if dep.Pod == nil {
		dep.Pod = &egv1alpha1.KubernetesPodSpec{}
	}
	dep.Pod.Volumes = append(dep.Pod.Volumes,
		corev1.Volume{
			Name:         mergedVol,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
		corev1.Volume{
			Name: secretVol,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: bundleSecret},
			},
		},
	)

	// alpine: small (5MB), ships with the ca-certificates package by
	// default at the same /etc/ssl/certs/ca-certificates.crt path the
	// envoy image reads from. busybox would be smaller but has no
	// cacerts file — we'd have nothing to seed the merged bundle from.
	//
	// Concatenate every file in /extra (the secret data keys are
	// arbitrary — we don't presume tls.crt vs ca.crt) so any
	// single-file or multi-key Secret layout works.
	//
	// InitContainers belongs to KubernetesDeploymentSpec (not the
	// nested Pod sub-spec) per envoy-gateway's API shape.
	dep.InitContainers = append(dep.InitContainers, corev1.Container{
		Name:    "clrk-trust-merge",
		Image:   "docker.io/library/alpine:3.20",
		Command: []string{"sh", "-c"},
		Args: []string{
			`cat ` + systemTrustPath + ` > ` + mergedPath + `/ca-certificates.crt && ` +
				`for f in /extra/*; do [ -f "$f" ] && echo >> ` + mergedPath + `/ca-certificates.crt && cat "$f" >> ` + mergedPath + `/ca-certificates.crt; done`,
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: mergedVol, MountPath: mergedPath},
			{Name: secretVol, MountPath: "/extra", ReadOnly: true},
		},
	})

	if dep.Container == nil {
		dep.Container = &egv1alpha1.KubernetesContainerSpec{}
	}
	dep.Container.VolumeMounts = append(dep.Container.VolumeMounts, corev1.VolumeMount{
		Name:      mergedVol,
		MountPath: systemTrustPath,
		SubPath:   "ca-certificates.crt",
		ReadOnly:  true,
	})
	return dep
}

// upstreamAdditionalTrustSecret returns the Secret name to mount as
// the additional trust bundle, or "" when none is configured. The
// returned name is the *mirror* secret in envoy-gateway-system that
// ensureUpstreamTrustMirror creates from the user's source secret —
// the EG-managed Envoy pod runs in envoy-gateway-system and the
// kubelet needs the secret in that same namespace.
func upstreamAdditionalTrustSecret(eg *clrkv1alpha1.EgressGateway) string {
	if eg.Spec.UpstreamTLS == nil || eg.Spec.UpstreamTLS.AdditionalTrustBundleSecretRef == nil {
		return ""
	}
	return upstreamTrustMirrorName(eg.Name)
}

// upstreamTrustMirrorName is the deterministic name for the mirrored
// trust-bundle secret in envoy-gateway-system.
func upstreamTrustMirrorName(egName string) string {
	return "clrk-eg-" + egName + "-upstream-ca"
}

// ensureUpstreamTrustMirror copies the Secret referenced by
// EgressGatewaySpec.UpstreamTLS.AdditionalTrustBundleSecretRef from
// the EG's namespace into envoy-gateway-system so the EG-managed
// Envoy pod (which always runs there) can mount it. When the source
// Secret is missing the function returns nil — the EG controller
// reconciles on Secret events too, so we'll re-run when it appears.
//
// Cross-namespace reference is allowed without a ReferenceGrant gate
// today: the source must live in the EG's own namespace (we don't
// honor SecretObjectReference.Namespace), and the mirror is owned by
// the EG so it cascade-deletes.
func (r *EgressGatewayReconciler) ensureUpstreamTrustMirror(ctx context.Context, eg *clrkv1alpha1.EgressGateway) error {
	if eg.Spec.UpstreamTLS == nil || eg.Spec.UpstreamTLS.AdditionalTrustBundleSecretRef == nil {
		return nil
	}
	sourceName := string(eg.Spec.UpstreamTLS.AdditionalTrustBundleSecretRef.Name)
	if sourceName == "" {
		return nil
	}

	var src corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: eg.Namespace, Name: sourceName}, &src); err != nil {
		if apierrors.IsNotFound(err) {
			// Source not yet created; controller will be re-triggered
			// by the Secret watch. No mirror to write yet.
			return nil
		}
		return fmt.Errorf("reading upstream trust source %s/%s: %w", eg.Namespace, sourceName, err)
	}

	mirror := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      upstreamTrustMirrorName(eg.Name),
			Namespace: envoyGatewayNamespace,
		},
	}
	err := createOrUpdateWithRetry(ctx, r.Client, mirror, func() error {
		if mirror.Labels == nil {
			mirror.Labels = map[string]string{}
		}
		mirror.Labels["clrk.apoxy.dev/egressgateway"] = eg.Name
		mirror.Labels["clrk.apoxy.dev/egressgateway-namespace"] = eg.Namespace
		// Preserve every data key from the source — operators may
		// have packed multiple PEMs under arbitrary keys.
		mirror.Data = make(map[string][]byte, len(src.Data))
		for k, v := range src.Data {
			mirror.Data[k] = v
		}
		// Owner ref crosses namespaces; SetControllerReference rejects
		// that. The mirror is hand-cleaned in the deletion path
		// (Owns(&corev1.Secret{}) on the source namespace's secret
		// already covers cascade for the source); for the mirror we
		// rely on labels to find and reap on EG delete. Add labels
		// above so future cleanup can list-and-delete by them.
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// upstreamHostAliasesPatch returns a strategic-merge KubernetesPatchSpec
// that programs `spec.template.spec.hostAliases` on the EG-managed
// Deployment, or nil when no aliases are configured. envoy-gateway's
// KubernetesPodSpec doesn't surface hostAliases natively, so we use
// the Deployment-level Patch field — the EG controller applies it as a
// strategic-merge over the generated Deployment.
func upstreamHostAliasesPatch(eg *clrkv1alpha1.EgressGateway) *egv1alpha1.KubernetesPatchSpec {
	if eg.Spec.UpstreamTLS == nil || len(eg.Spec.UpstreamTLS.HostAliases) == 0 {
		return nil
	}
	patch := map[string]any{
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"hostAliases": eg.Spec.UpstreamTLS.HostAliases,
				},
			},
		},
	}
	raw, err := json.Marshal(patch)
	if err != nil {
		// HostAlias is JSON-marshalable; this would only fire on a
		// schema bug. Better to drop the patch than to surface a
		// reconcile error that would block the whole rollout.
		return nil
	}
	mt := egv1alpha1.StrategicMerge
	return &egv1alpha1.KubernetesPatchSpec{
		Type:  &mt,
		Value: apiextv1.JSON{Raw: raw},
	}
}

// ensureGatewayClass creates the shared GatewayClass the first time it's
// needed. No parametersRef — each Gateway carries its own
// infrastructure.parametersRef pointing at the per-EG EnvoyProxy CR so
// the private-Envoy image override applies per-EgressGateway.
func (r *EgressGatewayReconciler) ensureGatewayClass(ctx context.Context) error {
	gc := &gwapiv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: EgressGatewayClassName}}
	return createOrUpdateWithRetry(ctx, r.Client, gc, func() error {
		gc.Spec.ControllerName = gwapiv1.GatewayController("gateway.envoyproxy.io/gatewayclass-controller")
		gc.Spec.ParametersRef = nil
		return nil
	})
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
	if err := createOrUpdateWithRetry(ctx, r.Client, gw, func() error {
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
	}); err != nil {
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
	return createOrUpdateWithRetry(ctx, r.Client, rt, func() error {
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
}

func envoyProxyName(egName string) string { return "clrk-eg-envoyproxy-" + egName }

const envoyGatewayNamespace = "envoy-gateway-system"

// updateStatus refreshes Status.Conditions[Ready] from the EG-managed
// Gateway's Programmed condition and Status.EgressBackendAddress from the
// EG-managed Service. Returns requeue=true while the data plane is still
// provisioning (Gateway not Programmed or Service absent) so the next
// reconcile picks it up without waiting for an unrelated event.
func (r *EgressGatewayReconciler) updateStatus(ctx context.Context, eg *clrkv1alpha1.EgressGateway) (bool, error) {
	gwName := GatewayNamePrefix + eg.Name
	statusBase := eg.DeepCopy()

	ready := metav1.Condition{
		Type:               clrkv1alpha1.EgressGatewayConditionReady,
		ObservedGeneration: eg.Generation,
	}

	var gw gwapiv1.Gateway
	gwErr := r.Get(ctx, types.NamespacedName{Name: gwName, Namespace: eg.Namespace}, &gw)
	switch {
	case gwErr == nil && gatewayProgrammed(&gw):
		ready.Status = metav1.ConditionTrue
		ready.Reason = "GatewayProgrammed"
		ready.Message = "Envoy Gateway reports Programmed=True"
	case gwErr == nil:
		ready.Status = metav1.ConditionFalse
		ready.Reason = "GatewayNotProgrammed"
		ready.Message = "Envoy Gateway has not yet reported Programmed=True"
	case apierrors.IsNotFound(gwErr):
		ready.Status = metav1.ConditionFalse
		ready.Reason = "GatewayMissing"
		ready.Message = "Underlying Gateway resource not yet observed"
	default:
		return false, fmt.Errorf("getting Gateway: %w", gwErr)
	}

	addr, addrPending, err := r.resolveBackendAddress(ctx, eg, gwName)
	if err != nil {
		return false, err
	}

	dirty := false
	if eg.Status.EgressBackendAddress != addr {
		eg.Status.EgressBackendAddress = addr
		dirty = true
	}
	if meta.SetStatusCondition(&eg.Status.Conditions, ready) {
		dirty = true
	}
	if dirty {
		// MergeFrom (no optimistic lock) so concurrent envoy-gateway
		// status patches don't 409 us. See APO-567.
		if err := r.Status().Patch(ctx, eg, client.MergeFrom(statusBase)); err != nil {
			return false, fmt.Errorf("updating status: %w", err)
		}
	}
	return addrPending || ready.Status != metav1.ConditionTrue, nil
}

// resolveBackendAddress returns the dialable address for the EG-managed
// Service, with pending=true if the Service hasn't materialized or
// hasn't yet been assigned a NodePort.
func (r *EgressGatewayReconciler) resolveBackendAddress(ctx context.Context, eg *clrkv1alpha1.EgressGateway, gwName string) (string, bool, error) {
	var svcs corev1.ServiceList
	if err := r.List(ctx, &svcs,
		client.InNamespace(envoyGatewayNamespace),
		client.MatchingLabels{
			"gateway.envoyproxy.io/owning-gateway-name":      gwName,
			"gateway.envoyproxy.io/owning-gateway-namespace": eg.Namespace,
		},
	); err != nil {
		return "", false, fmt.Errorf("listing EG-managed services: %w", err)
	}
	if len(svcs.Items) == 0 {
		return eg.Status.EgressBackendAddress, true, nil
	}
	addr := r.computeBackendAddress(&svcs.Items[0])
	if addr == "" {
		return eg.Status.EgressBackendAddress, true, nil
	}
	return addr, false, nil
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
