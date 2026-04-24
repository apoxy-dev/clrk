package controller

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// AgentCASecretLabel marks a Secret as the MITM CA for a specific agent UID.
const AgentCASecretLabel = "clrk.apoxy.dev/agent-uid"

// AgentCASecretName returns the deterministic Secret name holding the MITM
// CA keypair for the agent with the given UID.
func AgentCASecretName(agentUID types.UID) string {
	return fmt.Sprintf("clrk-agent-ca-%s", string(agentUID))
}

// caValidity is the lifetime of the minted CA. Long enough that no agent pod
// would outlive it; we do not rotate within a pod's lifetime.
const caValidity = 10 * 365 * 24 * time.Hour

// DaemonAgentCAReconciler mints a per-agent CA for every DaemonAgent and
// stores it as a kubernetes.io/tls Secret owned by the agent. The handshaker
// reads the private key to sign leaf certs; the worker reads only the cert
// to build the sandbox's trust store.
type DaemonAgentCAReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=daemonagents,verbs=get;list;watch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=taskagents,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch

func (r *DaemonAgentCAReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var da clrkv1alpha1.DaemonAgent
	if err := r.Get(ctx, req.NamespacedName, &da); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, ensureAgentCA(ctx, r.Client, r.Scheme, &da, da.UID, "DaemonAgent")
}

func (r *DaemonAgentCAReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clrkv1alpha1.DaemonAgent{}).
		Owns(&corev1.Secret{}).
		Named("daemonagent-ca").
		Complete(r)
}

// TaskAgentCAReconciler is the TaskAgent equivalent of DaemonAgentCAReconciler.
type TaskAgentCAReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func (r *TaskAgentCAReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var ta clrkv1alpha1.TaskAgent
	if err := r.Get(ctx, req.NamespacedName, &ta); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, ensureAgentCA(ctx, r.Client, r.Scheme, &ta, ta.UID, "TaskAgent")
}

func (r *TaskAgentCAReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clrkv1alpha1.TaskAgent{}).
		Owns(&corev1.Secret{}).
		Named("taskagent-ca").
		Complete(r)
}

// ensureAgentCA is idempotent: creates the CA Secret if missing; leaves it
// untouched otherwise (we never rotate within an agent's lifetime).
func ensureAgentCA(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object, uid types.UID, kind string) error {
	logger := log.FromContext(ctx).WithValues("agent-uid", string(uid), "agent-kind", kind)

	name := AgentCASecretName(uid)
	ns := owner.GetNamespace()

	var existing corev1.Secret
	err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &existing)
	switch {
	case err == nil:
		return nil
	case !apierrors.IsNotFound(err):
		return fmt.Errorf("looking up agent CA secret: %w", err)
	}

	certPEM, keyPEM, err := mintAgentCA(kind, ns, owner.GetName(), uid)
	if err != nil {
		return fmt.Errorf("minting agent CA: %w", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels: map[string]string{
				AgentCASecretLabel: string(uid),
			},
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       certPEM,
			corev1.TLSPrivateKeyKey: keyPEM,
		},
	}
	if err := ctrl.SetControllerReference(owner, secret, scheme); err != nil {
		return fmt.Errorf("setting owner reference: %w", err)
	}

	if err := c.Create(ctx, secret); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("creating agent CA secret: %w", err)
	}

	logger.Info("Minted agent CA")
	return nil
}

// mintAgentCA generates a fresh ECDSA P-256 self-signed CA certificate
// whose subject carries the agent coordinates for operator debuggability.
func mintAgentCA(kind, ns, name string, uid types.UID) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generating key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, fmt.Errorf("generating serial: %w", err)
	}

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:         fmt.Sprintf("clrk %s %s/%s", kind, ns, name),
			Organization:       []string{"clrk"},
			OrganizationalUnit: []string{string(uid)},
		},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(caValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("signing certificate: %w", err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("marshalling key: %w", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return certPEM, keyPEM, nil
}
