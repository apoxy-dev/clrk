// Package invoke serves the taskagents/{name}/invoke connect
// subresource: the write-side door the run-task CLI POSTs to. It
// reverse-proxies the request to the named TaskAgent's per-TA ingress
// Envoy -- resolved to a pod IP via EndpointSlices, never Service DNS,
// so it works when the controller-manager runs out-of-cluster under
// `clrk dev` -- where ext_proc admits the request (MaxConcurrent /
// Rejected), honors or mints the invocation id, and publishes the
// lifecycle. The Connecter is pure transport: it adds no admission of
// its own beyond Kubernetes connect-verb authorization, so an invoke is
// subject to exactly the same governance as any other HTTP caller.
package invoke

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"

	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	registryrest "k8s.io/apiserver/pkg/registry/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/ports"
)

// triggerCLI is the X-Clrk-Trigger wire value every invoke stamps;
// v1alpha1.TriggerTypeFromHeaderValue maps it to InvocationTriggerCLI.
const triggerCLI = "cli"

// ClientFunc resolves the controller-runtime client used to look up the
// per-TA ingress EndpointSlices. It is evaluated per request rather than
// captured at construction because the apiserver Storage is built before
// the controller-manager's client exists; by the time Connect runs the
// manager is up and its cache is warm.
type ClientFunc func() client.Client

// Connecter is the rest.Connecter backing taskagents/{name}/invoke.
type Connecter struct {
	singular string
	// systemNS is the controller-manager's runtime namespace, where Envoy
	// Gateway provisions every TaskAgent's data-plane Service
	// (ENVOY_GATEWAY_NAMESPACE) regardless of the TaskAgent's own
	// namespace -- so this, not the request namespace, is where the
	// clrk-<ta> Service and its EndpointSlices live.
	systemNS  string
	clientFn  ClientFunc
	transport http.RoundTripper // nil => http.DefaultTransport
}

var (
	_ registryrest.Storage              = (*Connecter)(nil)
	_ registryrest.Scoper               = (*Connecter)(nil)
	_ registryrest.Connecter            = (*Connecter)(nil)
	_ registryrest.SingularNameProvider = (*Connecter)(nil)
)

// New constructs the invoke Connecter. singular is the discovery
// singular name; systemNS is the controller-manager namespace EG
// deploys data planes into; clientFn resolves the kube client at request
// time.
func New(singular, systemNS string, clientFn ClientFunc) *Connecter {
	return &Connecter{singular: singular, systemNS: systemNS, clientFn: clientFn}
}

// NewProvider adapts New to the apoxy-cli builder StorageProvider shape.
// The *runtime.Scheme / RESTOptionsGetter args are unused: the Connecter
// stores nothing through the generic registry.
func NewProvider(singular, systemNS string, clientFn ClientFunc) func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
	return func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
		return New(singular, systemNS, clientFn), nil
	}
}

// New returns the kind the subresource produces -- POST .../invoke mints
// an Invocation -- which the installer surfaces as the subresource's
// discovery kind.
func (c *Connecter) New() runtime.Object     { return &clrkv1alpha1.Invocation{} }
func (c *Connecter) Destroy()                {}
func (c *Connecter) NamespaceScoped() bool   { return true }
func (c *Connecter) GetSingularName() string { return c.singular }

// ConnectMethods: invoke accepts POST (the request body becomes the
// agent's stdin via the CloudEvents envelope) and GET (a no-body
// trigger).
func (c *Connecter) ConnectMethods() []string {
	return []string{http.MethodPost, http.MethodGet}
}

// NewConnectOptions returns no typed options: Stage 1 reads everything it
// needs straight from the raw request (headers + body), and the Stage 2
// ?wait stream is likewise read from the request in the handler. A nil
// options object makes the apiserver pass nil to Connect.
func (c *Connecter) NewConnectOptions() (runtime.Object, bool, string) {
	return nil, false, ""
}

// Connect resolves the per-TA ingress Envoy endpoint and returns a
// reverse proxy that streams the agent response back unbuffered. name is
// the TaskAgent name from the request path; the namespace comes from the
// request context.
func (c *Connecter) Connect(ctx context.Context, name string, _ runtime.Object, responder registryrest.Responder) (http.Handler, error) {
	ns := request.NamespaceValue(ctx)
	if ns == "" {
		return nil, apierrors.NewBadRequest("namespace is required")
	}
	cl := c.clientFn()
	if cl == nil {
		return nil, apierrors.NewServiceUnavailable("invoke transport not ready: kube client unavailable")
	}
	// The data-plane Service lives in the controller's namespace (where EG
	// runs), not the TaskAgent's namespace; the TaskAgent's namespace is
	// only used to stamp X-Clrk-TaskAgent below.
	addr, err := resolveIngressEndpoint(ctx, cl, c.systemNS, name)
	if err != nil {
		return nil, apierrors.NewServiceUnavailable(err.Error())
	}

	taHeader := ns + "/" + name
	proxy := &httputil.ReverseProxy{
		// Stream the response unbuffered so the buffered Stage 1 body and a
		// future Stage 2 NDJSON stream both pass straight through.
		FlushInterval: -1,
		Transport:     c.transport,
		Director: func(r *http.Request) {
			r.URL.Scheme = "http"
			r.URL.Host = addr
			r.Host = addr
			// The per-TA HTTPRoute matches "/"; drop the aggregated
			// apiserver's subresource path. The query (e.g. a future ?wait)
			// is consumed here, not forwarded to the agent.
			r.URL.Path = "/"
			r.URL.RawPath = ""
			r.URL.RawQuery = ""
			// Stamp routing + trigger authoritatively from the
			// (authenticated, authorized) Kubernetes path so a caller can
			// neither target a different TaskAgent nor disguise the
			// trigger. A caller-supplied X-Clrk-Invocation-ID is left
			// intact: ingress honors it (APO-684) when canonical, which is
			// how run-task follows its own run to an exit code.
			r.Header.Set(ports.HeaderTaskAgent, taHeader)
			r.Header.Set(ports.HeaderTrigger, triggerCLI)
		},
		ErrorHandler: func(_ http.ResponseWriter, _ *http.Request, perr error) {
			// Fires only on a pre-response transport failure (dial/connect),
			// so writing a Status through the responder is safe.
			slog.Warn("invoke: proxy to per-TA ingress failed", "taskagent", taHeader, "addr", addr, "err", perr)
			responder.Error(apierrors.NewServiceUnavailable("invoke upstream unavailable: " + perr.Error()))
		},
	}
	return proxy, nil
}

// resolveIngressEndpoint resolves the per-TaskAgent ingress Envoy data
// plane to a "host:port" dial target via EndpointSlices in systemNS (the
// controller's namespace, where EG provisions every data plane). It
// deliberately does NOT use Service DNS (clrk-<ta>.<ns>.svc): resolving
// the pod IP directly keeps it working when the controller-manager runs
// out-of-cluster (no cluster DNS), the same reason the worker-dispatch
// HTTPInvoker resolves pod IPs. The Service name mirrors
// controller.taskAgentEnvoyProxyName ("clrk-"+ta), set as the Envoy
// Gateway EnvoyService.Name override so the data plane is addressable by
// that stable name.
func resolveIngressEndpoint(ctx context.Context, c client.Client, systemNS, taName string) (string, error) {
	svcName := "clrk-" + taName
	var slices discoveryv1.EndpointSliceList
	if err := c.List(ctx, &slices,
		client.InNamespace(systemNS),
		client.MatchingLabels{discoveryv1.LabelServiceName: svcName},
	); err != nil {
		return "", fmt.Errorf("list EndpointSlices for %s/%s: %w", systemNS, svcName, err)
	}
	for i := range slices.Items {
		s := &slices.Items[i]
		port := ingressPort(s.Ports)
		if port == 0 {
			continue
		}
		for _, ep := range s.Endpoints {
			if ep.Conditions.Ready != nil && !*ep.Conditions.Ready {
				continue
			}
			for _, ip := range ep.Addresses {
				if ip != "" {
					return net.JoinHostPort(ip, strconv.Itoa(int(port))), nil
				}
			}
		}
	}
	return "", fmt.Errorf("no ready ingress endpoint for Service %s/%s", systemNS, svcName)
}

// ingressPort picks the HTTP listener port from an EndpointSlice. The EG
// data-plane Service for our single HTTP listener exposes exactly one
// port, so the sole port is taken when present; a port explicitly named
// "http" is preferred so the lookup stays correct if EG ever adds a
// second (e.g. metrics) port. Returns 0 when no usable port is found.
func ingressPort(eps []discoveryv1.EndpointPort) int32 {
	if len(eps) == 1 && eps[0].Port != nil {
		return *eps[0].Port
	}
	for _, p := range eps {
		if p.Name != nil && *p.Name == "http" && p.Port != nil {
			return *p.Port
		}
	}
	return 0
}
