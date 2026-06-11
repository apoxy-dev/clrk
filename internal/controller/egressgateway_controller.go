package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
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
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/llmroute"
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

	// EgressListenerBasePort is the first TCP port the EG-managed Envoy
	// binds. Each EgressListener in spec.listeners is assigned
	// EgressListenerBasePort+index — protocol shapes (TLS-Terminate,
	// plain TCP, etc.) cannot share a port, so each materializes as a
	// distinct Envoy listener and Service ServicePort.
	EgressListenerBasePort int32 = 18080

	// MaxEgressListeners caps spec.listeners to keep the assigned port
	// range narrow and reviewable. Bump if a real workload needs more.
	MaxEgressListeners = 8

	// LLMRevAnnotation carries a hash of the LLM routing inputs
	// (attached AIProviderRoutes, their referenced Backends, attached
	// FallbackRoutingPolicies) on the clrk-owned Gateway, for operator
	// observability. NOTE: the annotation alone does NOT trigger Envoy
	// Gateway retranslation — EG's watch predicate passes annotation
	// changes, but its gateway-api runner dedupes on the translated IR
	// and Gateway metadata never reaches the IR, so the xds-translator
	// (where the egextension hooks live) is never re-run. The actual
	// trigger is the revision header-match rule ensureCatchAllRoute
	// stamps on the catch-all HTTPRoute (see LLMRevHeaderName): route
	// rules ARE IR-visible, and the egextension replaces EG's whole
	// route configuration so the rule never serves traffic.
	LLMRevAnnotation = "clrk.apoxy.dev/llm-rev"

	// LLMRevHeaderName is the header name of the IR-perturbing revision
	// match on the catch-all HTTPRoute. Never sent by clients; exists
	// solely so a clrk CR change makes EG's translated IR differ and
	// forces the xds-translation pass that re-runs the extension hooks.
	LLMRevHeaderName = "x-clrk-llm-rev"
)

// listenerPort assigns a deterministic port per EgressListener index.
// Operators can override per-listener with EgressListener.Port (the
// downstream sandbox-side port-match), but the upstream Envoy-listener
// port is always controlled by the controller to keep Service ports
// predictable.
func listenerPort(idx int) int32 { return EgressListenerBasePort + int32(idx) }

// resolveRequestTimeout returns the effective per-request timeout for
// the catch-all HTTPRoute. Resolution lives on the API type so the EG
// extension server applies the same value to synthesized routes.
func resolveRequestTimeout(eg *clrkv1alpha1.EgressGateway) time.Duration {
	return eg.Spec.EffectiveRequestTimeout()
}

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

	// DevBackendHost, when non-empty, overrides
	// Status.Listeners[*].BackendAddress to use this host with the EG
	// Service NodePort instead of the in-cluster DNS name. Set by
	// `clrk dev` to the docker hostname of the k3s container ("clrk-k3s")
	// because workers run on the docker network and can't route to k3s
	// ClusterIPs.
	DevBackendHost string

	// EnvoyGatewayNamespace is the namespace EG provisions per-EG
	// Envoy Deployments + Services into. Equal to the controller-
	// manager's runtime namespace (mirrors EG's
	// ENVOY_GATEWAY_NAMESPACE env var). Used here to mirror
	// upstream-CA Secrets and to list EG-managed Services for
	// listener status. Required.
	EnvoyGatewayNamespace string
}

// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=egressgateways,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=egressgateways/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=aiproviderroutes;backends;fallbackroutingpolicies,verbs=get;list;watch
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

	if err := validateEgressGatewaySpec(&eg); err != nil {
		// Surface the validation error on Status.Ready and stop —
		// Gateway / EnvoyProxy materialization would either fail
		// downstream or expose a half-configured data plane.
		return ctrl.Result{}, r.markInvalid(ctx, &eg, err)
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
	// Status.Ready and re-queue until Ready=True and every
	// Status.Listeners[*].BackendAddress is populated so workers see
	// the data plane as soon as it provisions.
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
	//
	// AIProviderRoute / Backend / FallbackRoutingPolicy changes alter
	// the LLM routing inputs hashed into the Gateway's LLMRevAnnotation,
	// so each enqueues every EG (the fleet is small; precision isn't
	// worth a reverse index). CredentialInjectionPolicy is deliberately
	// absent — credentials are injected at request time and have no xDS
	// footprint.
	return ctrl.NewControllerManagedBy(mgr).
		For(&clrkv1alpha1.EgressGateway{}).
		Owns(&corev1.Secret{}).
		Watches(&clrkv1alpha1.AIProviderRoute{}, handler.EnqueueRequestsFromMapFunc(r.allEgressGateways)).
		Watches(&clrkv1alpha1.Backend{}, handler.EnqueueRequestsFromMapFunc(r.allEgressGateways)).
		Watches(&clrkv1alpha1.FallbackRoutingPolicy{}, handler.EnqueueRequestsFromMapFunc(r.allEgressGateways)).
		Named("egressgateway").
		Complete(r)
}

// allEgressGateways enqueues every EgressGateway. Used by the LLM-input
// watches above, where any change can affect any EG's synthesized
// routes.
func (r *EgressGatewayReconciler) allEgressGateways(ctx context.Context, _ client.Object) []reconcile.Request {
	var egs clrkv1alpha1.EgressGatewayList
	if err := r.List(ctx, &egs); err != nil {
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(egs.Items))
	for _, eg := range egs.Items {
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: eg.Namespace, Name: eg.Name}})
	}
	return reqs
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

	dep.Patch = envoyDeploymentPatch(eg)

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
		Image:   "mirror.gcr.io/library/alpine:3.20",
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
// returned name is the *mirror* secret in the EG control-plane
// namespace that ensureUpstreamTrustMirror creates from the user's
// source secret — the EG-managed Envoy pod runs in the EG control-
// plane namespace and the kubelet needs the secret in that same
// namespace.
func upstreamAdditionalTrustSecret(eg *clrkv1alpha1.EgressGateway) string {
	if eg.Spec.UpstreamTLS == nil || eg.Spec.UpstreamTLS.AdditionalTrustBundleSecretRef == nil {
		return ""
	}
	return upstreamTrustMirrorName(eg.Name)
}

// upstreamTrustMirrorName is the deterministic name for the mirrored
// trust-bundle secret in the EG control-plane namespace.
func upstreamTrustMirrorName(egName string) string {
	return "clrk-eg-" + egName + "-upstream-ca"
}

// ensureUpstreamTrustMirror copies the Secret referenced by
// EgressGatewaySpec.UpstreamTLS.AdditionalTrustBundleSecretRef from
// the EG's namespace into the EG control-plane namespace
// (r.EnvoyGatewayNamespace) so the EG-managed Envoy pod (which always
// runs there) can mount it. When the source Secret is missing the
// function returns nil — the EG controller reconciles on Secret
// events too, so we'll re-run when it appears.
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
			Namespace: r.EnvoyGatewayNamespace,
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

// envoyDeploymentPatch returns the strategic-merge patch applied to the
// EG-managed Envoy Deployment. It always overrides the Envoy container's
// startup/readiness probes so per-EG cold start isn't gated on EG's
// default `periodSeconds: 10` startupProbe (kubelet won't run the first
// probe until t = periodSeconds, so the default holds the pod NotReady
// for ~10 s even though Envoy /ready is live within ~1 s). When the EG
// also pins `spec.upstreamTLS.hostAliases`, those land in the same patch
// (KubernetesDeploymentSpec.Patch is single-valued — there's only one
// merge slot on the parent EnvoyProxy). Returns nil if no patch is
// needed, which today never happens because the probe override is
// unconditional.
func envoyDeploymentPatch(eg *clrkv1alpha1.EgressGateway) *egv1alpha1.KubernetesPatchSpec {
	// Strategic-merge by container name. EG ships two containers in the
	// proxy pod (`envoy` + `shutdown-manager`), both defaulted to
	// `periodSeconds: 10` probes; pod readiness is AND-over-containers,
	// so failing to override either holds the pod NotReady for ~10 s.
	podSpec := map[string]any{
		"containers": []any{
			fastProbeContainer("envoy", "/ready", 19003),
			fastProbeContainer("shutdown-manager", "/healthz", 19002),
		},
	}
	if eg.Spec.UpstreamTLS != nil && len(eg.Spec.UpstreamTLS.HostAliases) > 0 {
		podSpec["hostAliases"] = eg.Spec.UpstreamTLS.HostAliases
	}

	patch := map[string]any{
		"spec": map[string]any{
			"template": map[string]any{
				"spec": podSpec,
			},
		},
	}
	raw, err := json.Marshal(patch)
	if err != nil {
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

// validateEgressGatewaySpec enforces invariants the CRD schema can't
// express on its own: distinct listener names, supported protocols
// only, and an upper bound on listener count so port allocation stays
// in a narrow reviewable range. Returns nil on success; the caller
// surfaces the returned error on Status.
func validateEgressGatewaySpec(eg *clrkv1alpha1.EgressGateway) error {
	if len(eg.Spec.Listeners) == 0 {
		return fmt.Errorf("spec.listeners must not be empty")
	}
	if len(eg.Spec.Listeners) > MaxEgressListeners {
		return fmt.Errorf("spec.listeners has %d entries; maximum is %d",
			len(eg.Spec.Listeners), MaxEgressListeners)
	}
	seen := make(map[string]struct{}, len(eg.Spec.Listeners))
	for _, l := range eg.Spec.Listeners {
		if _, dup := seen[l.Name]; dup {
			return fmt.Errorf("spec.listeners[%q]: duplicate name", l.Name)
		}
		seen[l.Name] = struct{}{}
		if _, err := clrkv1alpha1.ShapeForListener(l); err != nil {
			return err
		}
	}
	if eg.Spec.RequestTimeout != nil && eg.Spec.RequestTimeout.Duration <= 0 {
		return fmt.Errorf("spec.requestTimeout must be a positive duration, got %s", eg.Spec.RequestTimeout.Duration)
	}
	return nil
}

// markInvalid patches Status.Ready=False with the validation error and
// returns the reconcile error to retry on the next CRD update. We don't
// surface the validation failure as a reconcile error directly because
// invalid specs should not requeue indefinitely — the user has to fix
// the spec.
func (r *EgressGatewayReconciler) markInvalid(ctx context.Context, eg *clrkv1alpha1.EgressGateway, validationErr error) error {
	statusBase := eg.DeepCopy()
	cond := metav1.Condition{
		Type:               clrkv1alpha1.EgressGatewayConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             "InvalidSpec",
		Message:            validationErr.Error(),
		ObservedGeneration: eg.Generation,
	}
	if !meta.SetStatusCondition(&eg.Status.Conditions, cond) {
		return nil
	}
	if err := r.Status().Patch(ctx, eg, client.MergeFrom(statusBase)); err != nil {
		return fmt.Errorf("patching invalid-spec status: %w", err)
	}
	return nil
}

// ensureGateway materializes one Envoy-Gateway Listener per
// EgressListener, on a deterministic port per index. Every Gateway
// listener uses HTTPSProtocolType regardless of the EgressListener's
// declared protocol — this is the only way to make EG generate the
// HCM scaffolding that PostHTTPListenerModify can mutate. The
// extension reads the per-listener shape off the listener name and
// reshapes the chain (stripping TLS, swapping filters) accordingly.
//
// TLS placeholder cert (per-EG CA) is the same on every listener; the
// extension overwrites the handshaker on shapes that terminate TLS
// and strips the TransportSocket entirely on plain-TCP shapes.
// llmRevision hashes the LLM routing inputs the egextension reads when
// synthesizing this EG's routes and clusters: the specs of attached
// AIProviderRoutes, of the Backends those routes reference, and of the
// FallbackRoutingPolicies attached to those routes — in sorted
// namespace/name order so the hash is deterministic. Resources outside
// the EG's attachment graph don't perturb the hash, so unrelated CR
// churn doesn't force retranslation.
func (r *EgressGatewayReconciler) llmRevision(ctx context.Context, eg *clrkv1alpha1.EgressGateway) (string, error) {
	var routes clrkv1alpha1.AIProviderRouteList
	if err := r.List(ctx, &routes); err != nil {
		return "", fmt.Errorf("listing AIProviderRoutes: %w", err)
	}
	var attached []*clrkv1alpha1.AIProviderRoute
	for i := range routes.Items {
		if llmroute.RouteAttachesTo(routes.Items[i], eg.Namespace, eg.Name) {
			attached = append(attached, &routes.Items[i])
		}
	}
	sort.Slice(attached, func(i, j int) bool {
		if attached[i].Namespace != attached[j].Namespace {
			return attached[i].Namespace < attached[j].Namespace
		}
		return attached[i].Name < attached[j].Name
	})
	// No attached routes means the egextension synthesizes nothing for
	// this EG: skip the revision rule entirely so LLM-less gateways keep
	// a pristine catch-all. The transition out of the last attachment
	// still retranslates — removing the rule itself perturbs the IR.
	if len(attached) == 0 {
		return "", nil
	}

	h := sha256.New()
	hashSpec := func(kind, ns, name string, spec any) error {
		raw, err := json.Marshal(spec)
		if err != nil {
			return fmt.Errorf("marshaling %s %s/%s spec: %w", kind, ns, name, err)
		}
		fmt.Fprintf(h, "%s|%s/%s|", kind, ns, name)
		h.Write(raw)
		return nil
	}

	backendRefs := map[types.NamespacedName]bool{}
	for _, route := range attached {
		if err := hashSpec("apr", route.Namespace, route.Name, route.Spec); err != nil {
			return "", err
		}
		for _, rule := range route.Spec.Rules {
			for _, ref := range rule.BackendRefs {
				if !llmroute.BackendRefIsClrkBackend(ref) {
					continue
				}
				ns := route.Namespace
				if ref.Namespace != nil && *ref.Namespace != "" {
					ns = string(*ref.Namespace)
				}
				backendRefs[types.NamespacedName{Namespace: ns, Name: string(ref.Name)}] = true
			}
		}
	}

	refs := make([]types.NamespacedName, 0, len(backendRefs))
	for nn := range backendRefs {
		refs = append(refs, nn)
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Namespace != refs[j].Namespace {
			return refs[i].Namespace < refs[j].Namespace
		}
		return refs[i].Name < refs[j].Name
	})
	for _, nn := range refs {
		var be clrkv1alpha1.Backend
		if err := r.Get(ctx, nn, &be); err != nil {
			if apierrors.IsNotFound(err) {
				// A missing Backend is part of the state: when it
				// appears the hash changes and EG retranslates.
				continue
			}
			return "", fmt.Errorf("getting Backend %s: %w", nn, err)
		}
		if err := hashSpec("backend", be.Namespace, be.Name, be.Spec); err != nil {
			return "", err
		}
	}

	var policies clrkv1alpha1.FallbackRoutingPolicyList
	if err := r.List(ctx, &policies); err != nil {
		return "", fmt.Errorf("listing FallbackRoutingPolicies: %w", err)
	}
	sort.Slice(policies.Items, func(i, j int) bool {
		if policies.Items[i].Namespace != policies.Items[j].Namespace {
			return policies.Items[i].Namespace < policies.Items[j].Namespace
		}
		return policies.Items[i].Name < policies.Items[j].Name
	})
	for i := range policies.Items {
		p := &policies.Items[i]
		attachedToRoute := false
		for _, route := range attached {
			if llmroute.PolicyAttachesTo(*p, route.Namespace, route.Name) {
				attachedToRoute = true
				break
			}
		}
		if !attachedToRoute {
			continue
		}
		if err := hashSpec("frp", p.Namespace, p.Name, p.Spec); err != nil {
			return "", err
		}
	}

	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:8]), nil
}

func (r *EgressGatewayReconciler) ensureGateway(ctx context.Context, eg *clrkv1alpha1.EgressGateway) error {
	llmRev, err := r.llmRevision(ctx, eg)
	if err != nil {
		return fmt.Errorf("computing LLM revision: %w", err)
	}
	gw := &gwapiv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GatewayNamePrefix + eg.Name,
			Namespace: eg.Namespace,
		},
	}
	if err := createOrUpdateWithRetry(ctx, r.Client, gw, func() error {
		// Annotation bump → EG retranslation → egextension re-reads the
		// clrk CRs and re-synthesizes the LLM routes/clusters. Mutate
		// only our key; Get-then-Update preserves foreign annotations.
		if gw.Annotations == nil {
			gw.Annotations = map[string]string{}
		}
		gw.Annotations[LLMRevAnnotation] = llmRev
		caSecret := gwapiv1.ObjectName(EgressGatewayCASecretName(eg.Name))
		tlsMode := gwapiv1.TLSModeTerminate
		allRoutes := gwapiv1.NamespacesFromAll
		gw.Spec.GatewayClassName = gwapiv1.ObjectName(EgressGatewayClassName)
		listeners := make([]gwapiv1.Listener, 0, len(eg.Spec.Listeners))
		for i, el := range eg.Spec.Listeners {
			shape, err := clrkv1alpha1.ShapeForListener(el)
			if err != nil {
				// validateEgressGatewaySpec ran above; an error
				// here means a programmer slipped a new protocol
				// into ShapeForListener without updating the
				// validator. Surface to logs and skip the
				// listener rather than blow up the whole Gateway.
				log.FromContext(ctx).Error(err, "Skipping listener with unresolvable shape", "listener", el.Name)
				continue
			}
			listeners = append(listeners, gwapiv1.Listener{
				Name:     gwapiv1.SectionName(clrkv1alpha1.EncodeListenerSectionName(el.Name, shape)),
				Port:     gwapiv1.PortNumber(listenerPort(i)),
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
			})
		}
		gw.Spec.Listeners = listeners
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
	return r.ensureCatchAllRoute(ctx, eg, llmRev)
}

// ensureCatchAllRoute creates a placeholder HTTPRoute attached to every
// listener so EG generates the HCM scaffolding the extension needs to
// rewrite. The route's backendRef points at a synthetic service that
// PostTranslateModify replaces with a dynamic_forward_proxy cluster —
// the backend is never actually dialed.
//
// One ParentRef per listener (sectionName-scoped). For TCP /
// TLS-Passthrough listeners the extension nukes the HCM and substitutes
// network filters; the HTTPRoute is purely the trigger that gets EG to
// emit a listener at all.
//
// llmRev rides along as a second rule matching the LLMRevHeaderName
// header: it changes EG's translated IR whenever the LLM routing
// inputs change, which is the only thing that makes EG re-run the
// xds-translation pass (and therefore the egextension hooks that
// synthesize the per-rule LLM routes and clusters). The rule never
// serves traffic — rewriteHCM replaces the whole route configuration.
func (r *EgressGatewayReconciler) ensureCatchAllRoute(ctx context.Context, eg *clrkv1alpha1.EgressGateway, llmRev string) error {
	rt := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GatewayNamePrefix + eg.Name + "-catchall",
			Namespace: eg.Namespace,
		},
	}
	port := gwapiv1.PortNumber(80)
	return createOrUpdateWithRetry(ctx, r.Client, rt, func() error {
		gwName := gwapiv1.ObjectName(GatewayNamePrefix + eg.Name)
		parents := make([]gwapiv1.ParentReference, 0, len(eg.Spec.Listeners))
		for _, el := range eg.Spec.Listeners {
			shape, err := clrkv1alpha1.ShapeForListener(el)
			if err != nil {
				continue
			}
			section := gwapiv1.SectionName(clrkv1alpha1.EncodeListenerSectionName(el.Name, shape))
			parents = append(parents, gwapiv1.ParentReference{
				Name:        gwName,
				SectionName: &section,
			})
		}
		rt.Spec.ParentRefs = parents
		// Pin Request + BackendRequest so AI-provider streaming
		// responses don't hit Envoy Gateway's bare 15s default. Value
		// comes from spec.requestTimeout when set; otherwise the
		// clrk-side default in resolveRequestTimeout. time.Duration's
		// String() yields a GEP-2257-compatible value for
		// gwapiv1.Duration.
		gwTimeout := gwapiv1.Duration(resolveRequestTimeout(eg).String())
		placeholderRef := gwapiv1.HTTPBackendRef{
			BackendRef: gwapiv1.BackendRef{
				BackendObjectReference: gwapiv1.BackendObjectReference{
					Name: gwapiv1.ObjectName(GatewayNamePrefix + eg.Name + "-dfp-placeholder"),
					Port: &port,
				},
			},
		}
		rt.Spec.Rules = []gwapiv1.HTTPRouteRule{{
			BackendRefs: []gwapiv1.HTTPBackendRef{placeholderRef},
			Timeouts: &gwapiv1.HTTPRouteTimeouts{
				Request:        &gwTimeout,
				BackendRequest: &gwTimeout,
			},
		}}
		if llmRev != "" {
			// IR perturbation only — see the function doc.
			rt.Spec.Rules = append(rt.Spec.Rules, gwapiv1.HTTPRouteRule{
				Matches: []gwapiv1.HTTPRouteMatch{{
					Headers: []gwapiv1.HTTPHeaderMatch{{
						Name:  gwapiv1.HTTPHeaderName(LLMRevHeaderName),
						Value: llmRev,
					}},
				}},
				BackendRefs: []gwapiv1.HTTPBackendRef{placeholderRef},
			})
		}
		return ctrl.SetControllerReference(eg, rt, r.Scheme)
	})
}

func envoyProxyName(egName string) string { return "clrk-eg-envoyproxy-" + egName }

// updateStatus refreshes Status.Conditions[Ready] from the EG-managed
// Gateway's Programmed condition and Status.Listeners[*].BackendAddress
// from the EG-managed Service. Returns requeue=true while the data plane
// is still provisioning so the next reconcile picks it up without
// waiting for an unrelated event.
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

	listenerStatuses, anyPending, err := r.resolveListenerStatuses(ctx, eg, gwName)
	if err != nil {
		return false, err
	}

	dirty := false
	if !equalListenerStatuses(eg.Status.Listeners, listenerStatuses) {
		eg.Status.Listeners = listenerStatuses
		dirty = true
	}
	if eg.Status.ListenerCount != int32(len(listenerStatuses)) {
		eg.Status.ListenerCount = int32(len(listenerStatuses))
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
	return anyPending || ready.Status != metav1.ConditionTrue, nil
}

// resolveListenerStatuses produces one EgressListenerStatus per spec
// listener, populating BackendAddress from the EG-managed Service's
// matching ServicePort. anyPending=true while any listener's port is
// still missing.
func (r *EgressGatewayReconciler) resolveListenerStatuses(ctx context.Context, eg *clrkv1alpha1.EgressGateway, gwName string) ([]clrkv1alpha1.EgressListenerStatus, bool, error) {
	var svcs corev1.ServiceList
	if err := r.List(ctx, &svcs,
		client.InNamespace(r.EnvoyGatewayNamespace),
		client.MatchingLabels{
			"gateway.envoyproxy.io/owning-gateway-name":      gwName,
			"gateway.envoyproxy.io/owning-gateway-namespace": eg.Namespace,
		},
	); err != nil {
		return nil, false, fmt.Errorf("listing EG-managed services: %w", err)
	}

	prevByName := make(map[string]clrkv1alpha1.EgressListenerStatus, len(eg.Status.Listeners))
	for _, ls := range eg.Status.Listeners {
		prevByName[ls.Name] = ls
	}

	out := make([]clrkv1alpha1.EgressListenerStatus, 0, len(eg.Spec.Listeners))
	anyPending := false
	for i, el := range eg.Spec.Listeners {
		port := listenerPort(i)
		ls := clrkv1alpha1.EgressListenerStatus{
			Name: el.Name,
			Port: port,
		}
		// Preserve any existing per-listener Conditions /
		// AttachedRoutes set by other reconcile passes.
		if prev, ok := prevByName[el.Name]; ok {
			ls.Conditions = prev.Conditions
			ls.AttachedRoutes = prev.AttachedRoutes
		}
		var addr string
		if len(svcs.Items) > 0 {
			addr = r.computeBackendAddressForPort(&svcs.Items[0], port)
		}
		if addr == "" {
			anyPending = true
			// Hold the prior address (if any) so a transient
			// Service-list miss doesn't flap workers to direct-dial.
			if prev, ok := prevByName[el.Name]; ok {
				addr = prev.BackendAddress
			}
		}
		ls.BackendAddress = addr
		out = append(out, ls)
	}
	return out, anyPending, nil
}

// computeBackendAddressForPort picks the right address form depending
// on whether we're running under `clrk dev` (workers on docker network
// — NodePort on the dev host) or in-cluster (the Service's cluster DNS
// name). Returns "" when the requested port is not yet on the Service
// or the NodePort hasn't been assigned.
func (r *EgressGatewayReconciler) computeBackendAddressForPort(svc *corev1.Service, port int32) string {
	sp := findServicePort(svc, port)
	if sp == nil {
		return ""
	}
	if r.DevBackendHost != "" {
		if sp.NodePort == 0 {
			return ""
		}
		return fmt.Sprintf("%s:%d", r.DevBackendHost, sp.NodePort)
	}
	return fmt.Sprintf("%s.%s.svc.cluster.local:%d", svc.Name, svc.Namespace, sp.Port)
}

func findServicePort(svc *corev1.Service, port int32) *corev1.ServicePort {
	for i := range svc.Spec.Ports {
		if svc.Spec.Ports[i].Port == port {
			return &svc.Spec.Ports[i]
		}
	}
	return nil
}

func equalListenerStatuses(a, b []clrkv1alpha1.EgressListenerStatus) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name ||
			a[i].Port != b[i].Port ||
			a[i].BackendAddress != b[i].BackendAddress ||
			a[i].AttachedRoutes != b[i].AttachedRoutes {
			return false
		}
	}
	return true
}
