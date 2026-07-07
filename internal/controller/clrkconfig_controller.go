package controller

import (
	"context"
	"fmt"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/sentry"
)

const (
	// phoneHomeTokenKey is the Secret data key holding the bearer token.
	phoneHomeTokenKey = "token"

	// registeredRequeue is the periodic re-reconcile cadence once registered, so
	// a deleted token Secret is detected and re-registration kicks in.
	registeredRequeue = time.Hour

	// noSecretStoreRequeue backs off the (permanently failing) dev-mode case
	// where no core/v1 apiserver can persist the Secret.
	noSecretStoreRequeue = 10 * time.Minute

	// condRegistered is the CLRKConfig phone-home registration condition.
	condRegistered = "Registered"
)

// CLRKConfigReconciler drives the notifications phone-home registration for the
// CLRKConfig singleton. On signup (spec.notifications.email set) it registers
// the deployment with api.apoxy.dev, persists the returned bearer token to a
// core/v1 Secret (via the cluster client -- the embedded apiserver serves no
// core/v1), and records status.notifications (deploymentID +
// registrationTokenSecretRef + Registered condition). The browser sees only the
// Secret REFERENCE, never the token.
//
// It also exposes AuthState so the sentry Reporter/AdvisoryPoller can resolve
// the live token + gating flags without a second CLRKConfig/Secret client.
type CLRKConfigReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Phone is the api.apoxy.dev client; nil when --phone-home=false, which
	// severs all outbound calls and reports Registered=False/PhoneHomeDisabled.
	Phone *sentry.Client

	// Namespace is the clrk system namespace where the CLRKConfig singleton and
	// the token Secret live.
	Namespace string

	// SecretName names the token Secret written on registration.
	SecretName string

	// ClrkVersion is reported to api.apoxy.dev at registration.
	ClrkVersion string

	// Dropped, when set, returns the cumulative count of security reports the
	// phone-home reporter has dropped under backpressure; it is stamped into
	// status.notifications.reportsDropped each reconcile so the console health
	// panel reflects real loss. Nil when no reporter is running (leader-only).
	Dropped func() int64
}

// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=clrkconfigs,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=clrkconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch

func (r *CLRKConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Only the singleton in the system namespace is actionable.
	if req.Name != clrkv1alpha1.CLRKConfigSingletonName || req.Namespace != r.Namespace {
		return ctrl.Result{}, nil
	}
	var cc clrkv1alpha1.CLRKConfig
	if err := r.Get(ctx, req.NamespacedName, &cc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	statusBase := cc.DeepCopy()

	// Surface the live outbound-drop counter so the console health panel reflects
	// real backpressure loss rather than a permanent zero.
	if r.Dropped != nil {
		cc.Status.Notifications.ReportsDropped = r.Dropped()
	}

	setCond := func(status metav1.ConditionStatus, reason, msg string) {
		meta.SetStatusCondition(&cc.Status.Notifications.Conditions, metav1.Condition{
			Type:               condRegistered,
			Status:             status,
			Reason:             reason,
			Message:            msg,
			ObservedGeneration: cc.Generation,
		})
	}

	switch {
	case r.Phone == nil:
		setCond(metav1.ConditionFalse, "PhoneHomeDisabled", "Outbound phone-home is disabled (--phone-home=false).")
		return r.patchStatus(ctx, statusBase, &cc, ctrl.Result{})
	case cc.Spec.Notifications.Email == "":
		setCond(metav1.ConditionFalse, "NoSignup", "No notifications signup email is set.")
		return r.patchStatus(ctx, statusBase, &cc, ctrl.Result{})
	}

	// Already registered for the CURRENT email and the token Secret still exists
	// -> steady state. A changed signup email or a vanished Secret falls through
	// to (re-)register. The email is compared against the persisted
	// RegisteredEmail rather than metadata.generation, which this apiserver does
	// not reliably bump (APO-508).
	if cc.Status.Notifications.RegistrationTokenSecretRef != nil &&
		cc.Status.Notifications.RegisteredEmail == cc.Spec.Notifications.Email {
		var sec corev1.Secret
		err := r.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: r.SecretName}, &sec)
		switch {
		case err == nil:
			setCond(metav1.ConditionTrue, condRegistered, "Registered with api.apoxy.dev.")
			cc.Status.Notifications.ObservedGeneration = cc.Generation
			return r.patchStatus(ctx, statusBase, &cc, ctrl.Result{RequeueAfter: registeredRequeue})
		case !apierrors.IsNotFound(err):
			return ctrl.Result{}, fmt.Errorf("reading token Secret: %w", err)
		}
		// Secret vanished: fall through and re-register.
	}

	resp, err := r.Phone.Register(ctx, sentry.RegisterRequest{
		Email:       cc.Spec.Notifications.Email,
		ClrkVersion: r.ClrkVersion,
		ClusterUID:  string(cc.UID),
	})
	if err != nil {
		setCond(metav1.ConditionFalse, "RegisterFailed", fmt.Sprintf("Registration with api.apoxy.dev failed: %v", err))
		if _, perr := r.patchStatus(ctx, statusBase, &cc, ctrl.Result{}); perr != nil {
			return ctrl.Result{}, perr
		}
		return ctrl.Result{}, err // Workqueue backoff.
	}

	if err := r.upsertTokenSecret(ctx, resp.Token); err != nil {
		// In single-binary dev there is no core/v1 apiserver to hold the Secret;
		// this never succeeds, so surface it and back off long rather than spin.
		setCond(metav1.ConditionFalse, "NoSecretStore", fmt.Sprintf("Cannot persist the phone-home token Secret: %v", err))
		return r.patchStatus(ctx, statusBase, &cc, ctrl.Result{RequeueAfter: noSecretStoreRequeue})
	}

	now := metav1.Now()
	cc.Status.Notifications.DeploymentID = resp.DeploymentID
	cc.Status.Notifications.RegisteredEmail = cc.Spec.Notifications.Email
	cc.Status.Notifications.AdvisoryPollIntervalSeconds = int64(resp.AdvisoryPollIntervalSecs)
	cc.Status.Notifications.RegistrationTokenSecretRef = &clrkv1alpha1.SecretKeyReference{
		Name:      r.SecretName,
		Namespace: r.Namespace,
		Key:       phoneHomeTokenKey,
	}
	cc.Status.Notifications.RegisteredAt = &now
	cc.Status.Notifications.ObservedGeneration = cc.Generation
	setCond(metav1.ConditionTrue, condRegistered, "Registered with api.apoxy.dev.")
	return r.patchStatus(ctx, statusBase, &cc, ctrl.Result{RequeueAfter: registeredRequeue})
}

// patchStatus writes the status subresource only when it changed, returning the
// requested result.
func (r *CLRKConfigReconciler) patchStatus(ctx context.Context, base, cc *clrkv1alpha1.CLRKConfig, res ctrl.Result) (ctrl.Result, error) {
	if reflect.DeepEqual(base.Status, cc.Status) {
		return res, nil
	}
	if err := r.Status().Patch(ctx, cc, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating CLRKConfig status: %w", err)
	}
	return res, nil
}

// upsertTokenSecret create-or-updates the Opaque Secret holding the bearer
// token in the system namespace.
func (r *CLRKConfigReconciler) upsertTokenSecret(ctx context.Context, token string) error {
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: r.SecretName, Namespace: r.Namespace}}
	return createOrUpdateWithRetry(ctx, r.Client, sec, func() error {
		sec.Type = corev1.SecretTypeOpaque
		if sec.Data == nil {
			sec.Data = map[string][]byte{}
		}
		sec.Data[phoneHomeTokenKey] = []byte(token)
		return nil
	})
}

// AuthState resolves the current phone-home authorization by reading the
// CLRKConfig singleton and the token Secret. It is the AuthFunc for the sentry
// Reporter/AdvisoryPoller. A missing singleton or unset token yields an
// unauthorized (zero) state, not an error.
func (r *CLRKConfigReconciler) AuthState(ctx context.Context) (sentry.AuthState, error) {
	if r.Phone == nil {
		return sentry.AuthState{}, nil
	}
	var cc clrkv1alpha1.CLRKConfig
	if err := r.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: clrkv1alpha1.CLRKConfigSingletonName}, &cc); err != nil {
		return sentry.AuthState{}, client.IgnoreNotFound(err)
	}
	n := cc.Spec.Notifications
	ref := cc.Status.Notifications.RegistrationTokenSecretRef
	if n.Email == "" || ref == nil {
		return sentry.AuthState{DeploymentID: cc.Status.Notifications.DeploymentID}, nil
	}
	var sec corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, &sec); err != nil {
		return sentry.AuthState{}, client.IgnoreNotFound(err)
	}
	return sentry.AuthState{
		DeploymentID:         cc.Status.Notifications.DeploymentID,
		Token:                string(sec.Data[ref.Key]),
		AdviseEnabled:        n.AdvisoryPollEnabled == nil || *n.AdvisoryPollEnabled,
		AdvisoryPollInterval: time.Duration(cc.Status.Notifications.AdvisoryPollIntervalSeconds) * time.Second,
	}, nil
}

func (r *CLRKConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("clrkconfig").
		For(&clrkv1alpha1.CLRKConfig{}).
		Complete(r)
}
