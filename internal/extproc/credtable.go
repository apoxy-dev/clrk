package extproc

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/extproc/awssigv4"
)

// Parent-ref kind names CredentialInjectionPolicy may target.
const (
	aiProviderRouteKind = "AIProviderRoute"
	mcpRouteKind        = "MCPRoute"
)

// credInjection is one resolved CredentialInjectionPolicy ready for
// request-time application. headerName is lower-cased so callers can
// match against the captured Record map without re-normalizing.
type credInjection struct {
	policyName  string
	headerName  string
	headerValue string

	// sigv4, when non-nil, marks a ProviderAuth/AWSv4 policy: instead
	// of a static header pair, the request is signed at a point where
	// the final method/host/path/body are all known — applySigning,
	// called from the upstream attempt commit and the downstream
	// deferSign body-EOS path. headerName/headerValue are unused.
	sigv4 *sigv4Cred

	// section is the parentRef sectionName the CIP targeted, naming a
	// specific backend (BackendRef name) on the route. Empty means the
	// CIP is route-wide: injected at RequestHeaders for every request the
	// route matches (today's behavior). A non-empty section is injected
	// only when that backend is selected post-body, in that backend's
	// scheme — see lookup vs lookupForBackend.
	section string
}

// sigv4Cred is the resolved credential + signing scope of one
// ProviderAuth/AWSv4 CredentialInjectionPolicy.
type sigv4Cred struct {
	creds awssigv4.Credentials
	// region "" means derive from the target host at signing time.
	region  string
	service string
}

// headerNames returns the lowercase request header names this
// injection will set — the single configured header for Header
// targets, the SigV4 signature set for AWSv4. It drives capture
// redaction and the translated-request removal keep-set.
func (i credInjection) headerNames() []string {
	if i.sigv4 == nil {
		return []string{i.headerName}
	}
	names := []string{"authorization", "x-amz-date", "x-amz-content-sha256"}
	if i.sigv4.creds.SessionToken != "" {
		names = append(names, "x-amz-security-token")
	}
	return names
}

// credTable holds resolved CredentialInjectionPolicies attached to one
// EgressGateway (directly or via an APR that parent-refs this EG). Built
// alongside the routeTable in sinks.go and read on the request hot path
// by lookup().
//
// MVP only honors Target=Header. QueryParam and ProviderAuth policies
// are silently skipped at build time (file follow-ups).
type credTable struct {
	// byRoute keys credentials by (namespace, name) of the APR/MCPRoute
	// the policy parents to. Multiple policies on the same route stack.
	byRoute map[types.NamespacedName][]credInjection
	// gateway holds policies that parent directly to the EG. Always
	// applied on top of any matched-route credentials.
	gateway []credInjection
}

func newEmptyCredTable() *credTable {
	return &credTable{byRoute: map[types.NamespacedName][]credInjection{}}
}

// buildCredTable resolves the set of CredentialInjectionPolicies that
// attach to (egNamespace, egName) — directly or via an APR — reads their
// Secrets via the cached client, and returns the lookup table. Errors
// fetching individual Secrets (missing, malformed) are logged and the
// offending policy is skipped; one bad CIP must not break others on the
// same EG.
func buildCredTable(
	ctx context.Context,
	c client.Client,
	egNamespace, egName string,
	aprs []clrkv1alpha1.AIProviderRoute,
	cips []clrkv1alpha1.CredentialInjectionPolicy,
) *credTable {
	log := logr.FromContextOrDiscard(ctx).WithName("credtable")

	aprAttached := map[types.NamespacedName]bool{}
	for _, r := range aprs {
		if routeAttachesTo(r, egNamespace, egName) {
			aprAttached[types.NamespacedName{Namespace: r.Namespace, Name: r.Name}] = true
		}
	}

	tbl := newEmptyCredTable()
	for i := range cips {
		cip := &cips[i]
		for _, ref := range cip.Spec.ParentRefs {
			group, kind := refGroupKind(ref)
			if group != clrkAPIGroup {
				continue
			}
			refNS := cip.Namespace
			if ref.Namespace != nil && *ref.Namespace != "" {
				refNS = string(*ref.Namespace)
			}
			refName := string(ref.Name)

			switch kind {
			case egressGatewayKind:
				if refNS != egNamespace || refName != egName {
					continue
				}
				if inj := resolveCIP(ctx, c, cip, log); inj != nil {
					tbl.gateway = append(tbl.gateway, *inj)
				}
			case aiProviderRouteKind:
				key := types.NamespacedName{Namespace: refNS, Name: refName}
				if !aprAttached[key] {
					continue
				}
				if inj := resolveCIP(ctx, c, cip, log); inj != nil {
					// A sectionName on the route parentRef targets a
					// specific backend; it gates injection to
					// post-selection of that backend (see lookupForBackend).
					inj.section = sectionOf(ref)
					tbl.byRoute[key] = append(tbl.byRoute[key], *inj)
				}
			case mcpRouteKind:
				// CIPs targeting MCPRoute are a separate follow-up
				// item; APO-556 ships MCPRoute matching + ToolPolicy
				// enforcement but doesn't yet thread credential
				// injection through the MCP route table.
				continue
			}
		}
	}
	return tbl
}

// resolveCIP loads the referenced Secret and produces a credInjection.
// Returns nil for unsupported targets (QueryParam and non-AWSv4
// ProviderAuth are log-skipped follow-ups) or on Secret lookup failure.
func resolveCIP(ctx context.Context, c client.Client, cip *clrkv1alpha1.CredentialInjectionPolicy, log logr.Logger) *credInjection {
	spec := &cip.Spec
	switch spec.Target {
	case clrkv1alpha1.CredentialTargetHeader:
		if spec.HeaderName == nil || *spec.HeaderName == "" {
			log.Info("CredentialInjectionPolicy Target=Header missing HeaderName",
				"policy", cip.Namespace+"/"+cip.Name)
			return nil
		}
		secret := loadCIPSecret(ctx, c, cip, log)
		if secret == nil {
			return nil
		}
		dataKey := spec.SecretKey
		if dataKey == "" {
			dataKey = "token"
		}
		raw, ok := secret.Data[dataKey]
		if !ok || len(raw) == 0 {
			log.Info("CredentialInjectionPolicy Secret missing or empty key",
				"policy", cip.Namespace+"/"+cip.Name, "key", dataKey)
			return nil
		}
		return &credInjection{
			policyName:  cip.Namespace + "/" + cip.Name,
			headerName:  strings.ToLower(*spec.HeaderName),
			headerValue: string(raw),
		}

	case clrkv1alpha1.CredentialTargetProviderAuth:
		pa := spec.ProviderAuth
		if pa == nil {
			log.Info("CredentialInjectionPolicy Target=ProviderAuth missing providerAuth",
				"policy", cip.Namespace+"/"+cip.Name)
			return nil
		}
		if pa.Type != clrkv1alpha1.ProviderAuthTypeAWSv4 {
			log.V(1).Info("Skipping unsupported providerAuth type",
				"policy", cip.Namespace+"/"+cip.Name, "type", pa.Type)
			return nil
		}
		secret := loadCIPSecret(ctx, c, cip, log)
		if secret == nil {
			return nil
		}
		// SecretKey is ignored for this target: the AWSv4 credential
		// is a fixed-key triple, not a single value.
		akid := string(secret.Data["access_key_id"])
		sak := string(secret.Data["secret_access_key"])
		if akid == "" || sak == "" {
			log.Info("CredentialInjectionPolicy AWSv4 Secret missing access_key_id or secret_access_key",
				"policy", cip.Namespace+"/"+cip.Name)
			return nil
		}
		sc := &sigv4Cred{
			creds: awssigv4.Credentials{
				AccessKeyID:     akid,
				SecretAccessKey: sak,
				SessionToken:    string(secret.Data["session_token"]),
			},
			service: "bedrock",
		}
		if pa.Region != nil {
			sc.region = *pa.Region
		}
		if pa.Service != nil && *pa.Service != "" {
			sc.service = *pa.Service
		}
		return &credInjection{
			policyName: cip.Namespace + "/" + cip.Name,
			sigv4:      sc,
		}

	default:
		log.V(1).Info("Skipping unsupported credential target",
			"policy", cip.Namespace+"/"+cip.Name, "target", spec.Target)
		return nil
	}
}

// loadCIPSecret reads the policy's referenced Secret, logging and
// returning nil on failure. SecretRef.Namespace is intentionally
// ignored: a same-namespace constraint blocks a tenant who can author
// CIPs from pointing at any Secret in the cluster and exfiltrating it
// via header injection. Cross-namespace refs will gate on
// ReferenceGrant post-MVP (APO-577).
func loadCIPSecret(ctx context.Context, c client.Client, cip *clrkv1alpha1.CredentialInjectionPolicy, log logr.Logger) *corev1.Secret {
	secretKey := types.NamespacedName{Namespace: cip.Namespace, Name: string(cip.Spec.SecretRef.Name)}
	var secret corev1.Secret
	if err := c.Get(ctx, secretKey, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("CredentialInjectionPolicy Secret not found",
				"policy", cip.Namespace+"/"+cip.Name, "secret", secretKey.String())
		} else {
			log.Error(err, "Failed to read credential Secret",
				"policy", cip.Namespace+"/"+cip.Name, "secret", secretKey.String())
		}
		return nil
	}
	return &secret
}

// lookup returns the credentials applied at RequestHeaders time for a
// transaction matched against the given route: route-wide entries (no
// sectionName) first, gateway catch-all stacked on top. Backend-scoped
// entries (a sectionName naming a specific backend) are deliberately
// excluded here — they fire only after that backend is selected
// post-body (see lookupForBackend). Pass rr=nil for transactions that
// matched no APR (gateway-only). This preserves today's behavior exactly
// for routes that use no sectionName.
func (t *credTable) lookup(rr *routeRule) []credInjection {
	return t.lookupForBackend(rr, "")
}

// lookupForBackend returns the credentials to inject once `backend` (a
// BackendRef name) has been selected post-body: route-wide entries plus
// any whose sectionName matches `backend`, plus the gateway catch-all.
// Passing backend="" yields only the route-wide + gateway set, i.e. the
// header-time behavior of lookup.
func (t *credTable) lookupForBackend(rr *routeRule, backend string) []credInjection {
	if t == nil {
		return nil
	}
	var out []credInjection
	if rr != nil {
		for _, inj := range t.byRoute[types.NamespacedName{Namespace: rr.routeNamespace, Name: rr.routeName}] {
			if inj.section == "" || (backend != "" && inj.section == backend) {
				out = append(out, inj)
			}
		}
	}
	if len(t.gateway) > 0 {
		out = append(out, t.gateway...)
	}
	return out
}

// sectionOf returns the parentRef sectionName as a plain string, or ""
// when unset. For a route parentRef the sectionName names a backend.
func sectionOf(ref gwapiv1.ParentReference) string {
	if ref.SectionName != nil {
		return string(*ref.SectionName)
	}
	return ""
}

// applyInjections appends SetHeaders entries to (or builds) a
// HeaderMutation that injects the supplied static header credentials.
// SigV4 entries are skipped — they have no static pair and are applied
// by applySigning once the final request bytes are known. Returns nil
// when injs is empty and existing was nil.
func applyInjections(existing *extprocv3.HeaderMutation, injs []credInjection) *extprocv3.HeaderMutation {
	for _, inj := range injs {
		if inj.sigv4 != nil {
			continue
		}
		existing = setHeaderMut(existing, inj.headerName, inj.headerValue)
	}
	return existing
}

// setHeaderMut appends an OVERWRITE-flavored SetHeaders entry on
// existing (allocating one if nil). Two non-obvious choices live here
// so both callers (credential injection, traceparent injection) get
// them right:
//
//  1. RawValue, not Value: Envoy 1.36 ext_proc mutation_utils reads
//     only RawValue. Setting Value silently sets the header to empty.
//  2. OVERWRITE_IF_EXISTS_OR_ADD, not the default APPEND: APPEND
//     concatenates onto any agent-supplied value with a comma,
//     breaking auth schemes that don't tolerate multi-value headers
//     and (for traceparent) producing a malformed W3C header.
func setHeaderMut(existing *extprocv3.HeaderMutation, name, value string) *extprocv3.HeaderMutation {
	if existing == nil {
		existing = &extprocv3.HeaderMutation{}
	}
	existing.SetHeaders = append(existing.SetHeaders, &corev3.HeaderValueOption{
		Header: &corev3.HeaderValue{
			Key:      name,
			RawValue: []byte(value),
		},
		AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
	})
	return existing
}

// redactInjected replaces the value of every header an injection will
// set with "<redacted>" in the captured-record map. Called just after
// injection so OTLP records never carry the raw secret — for SigV4
// policies that covers the whole signature set (authorization,
// x-amz-*). The returned slice lists policies whose header names
// collided with an agent-supplied non-empty value — operators want to
// know an agent is trying to smuggle (or, for AWS SDK agents,
// self-sign) a credential.
func redactInjected(headers map[string]string, injs []credInjection) (collisions []string) {
	if len(headers) == 0 || len(injs) == 0 {
		return nil
	}
	for _, inj := range injs {
		collided := false
		for _, name := range inj.headerNames() {
			if existing, ok := headers[name]; ok && existing != "" && existing != "<redacted>" {
				collided = true
			}
			headers[name] = "<redacted>"
		}
		if collided {
			collisions = append(collisions, inj.policyName)
		}
	}
	return collisions
}

func refGroupKind(ref gwapiv1.ParentReference) (group, kind string) {
	if ref.Group != nil {
		group = string(*ref.Group)
	}
	if ref.Kind != nil {
		kind = string(*ref.Kind)
	}
	return
}

// CredPoliciesVersion fingerprints the CIPs that attach to the given
// EG (directly or via an APR). Mirrors aiproviderRoutesVersion in
// shape: sorted parts list, one per matched CIP, encoding the CIP's
// own ResourceVersion AND the ResourceVersion of its referenced
// Secret. Used by sinkRegistry to decide whether to rebuild the
// cached egSink.
//
// Including the Secret's RV is what makes credential rotation
// propagate without operator intervention: a Secret rotation bumps
// the underlying RV, the next ext_proc stream computes a different
// fingerprint, the sink rebuilds, and resolveCIP reads the fresh
// value. Secrets that don't exist yet (or vanished) get a `none`
// sentinel so the fingerprint changes again when the Secret
// (re)appears.
//
// Per-stream cost: one cached-client Get per matched CIP. The client
// is informer-backed so Gets are in-memory cache hits — no extra
// apiserver traffic.
func CredPoliciesVersion(
	ctx context.Context,
	c client.Client,
	cips []clrkv1alpha1.CredentialInjectionPolicy,
	aprs []clrkv1alpha1.AIProviderRoute,
	eg types.NamespacedName,
) string {
	aprAttached := map[types.NamespacedName]bool{}
	for _, r := range aprs {
		if routeAttachesTo(r, eg.Namespace, eg.Name) {
			aprAttached[types.NamespacedName{Namespace: r.Namespace, Name: r.Name}] = true
		}
	}

	var parts []string
	for i := range cips {
		cip := &cips[i]
		matched := false
		for _, ref := range cip.Spec.ParentRefs {
			group, kind := refGroupKind(ref)
			if group != clrkAPIGroup {
				continue
			}
			refNS := cip.Namespace
			if ref.Namespace != nil && *ref.Namespace != "" {
				refNS = string(*ref.Namespace)
			}
			switch kind {
			case egressGatewayKind:
				if refNS == eg.Namespace && string(ref.Name) == eg.Name {
					matched = true
				}
			case aiProviderRouteKind:
				if aprAttached[types.NamespacedName{Namespace: refNS, Name: string(ref.Name)}] {
					matched = true
				}
			}
			if matched {
				break
			}
		}
		if matched {
			parts = append(parts, fmt.Sprintf("%s/%s@%s+%s",
				cip.Namespace, cip.Name, cip.ResourceVersion,
				secretVersion(ctx, c, cip)))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// secretVersion returns the ResourceVersion of the Secret that cip
// references, or `none` when the Secret doesn't exist (yet). Errors
// other than NotFound also return `none` rather than failing the
// fingerprint — a transient apiserver hiccup shouldn't keep the cache
// stale forever; the next stream will retry.
func secretVersion(ctx context.Context, c client.Client, cip *clrkv1alpha1.CredentialInjectionPolicy) string {
	// SecretRef.Namespace is intentionally ignored — see resolveCIP.
	key := types.NamespacedName{Namespace: cip.Namespace, Name: string(cip.Spec.SecretRef.Name)}
	var s corev1.Secret
	if err := c.Get(ctx, key, &s); err != nil {
		return "none"
	}
	return s.ResourceVersion
}
