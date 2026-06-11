package egextension

// This file synthesizes the per-rule LLM data plane (APO-770-class
// work: inter-provider fallback): for every AIProviderRoute rule with
// resolvable clrk Backends, one Envoy route matched by the
// llmroute.PinHeader the downstream ext_proc sets at RequestBody EOS,
// and one multi-endpoint STRICT_DNS cluster whose endpoints are the
// rule's candidate Backends. Selection happens in Envoy's load
// balancer (weights without a FallbackRoutingPolicy; ordered priority
// tiers + a retry policy with one), and a cluster-level upstream
// ext_proc filter adapts each router attempt to whichever endpoint was
// picked — that filter runs fresh per retry attempt, which is what
// makes inter-provider fallback with per-attempt re-translation and
// re-auth possible at all (the DFP path cannot vary its host across
// attempts).

import (
	"context"
	"fmt"
	"strconv"
	"time"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	mutationrulesv3 "github.com/envoyproxy/go-control-plane/envoy/config/common/mutation_rules/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	upstreamcodecv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/upstream_codec/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	previoushostsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/retry/host/previous_hosts/v3"
	previousprioritiesv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/retry/priority/previous_priorities/v3"
	upstreamhttpv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/upstreams/http/v3"
	matcherv3 "github.com/envoyproxy/go-control-plane/envoy/type/matcher/v3"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"k8s.io/apimachinery/pkg/types"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	tlsv3 "github.com/apoxy-dev/envoy-go/envoy/extensions/transport_sockets/tls/v3"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egidentity"
	"github.com/apoxy-dev/clrk/internal/llmroute"
)

const (
	// upstreamCodecFilterName is the mandatory terminal filter of a
	// cluster-level (upstream) HTTP filter chain.
	upstreamCodecFilterName = "envoy.filters.http.upstream_codec"

	// extprocRoleMetadataKey / extprocRoleUpstream stamp the upstream
	// filter's gRPC streams so the controller-manager's single
	// ExternalProcessor service can tell the two filter positions
	// apart. Keep in sync with internal/extproc/server.go.
	extprocRoleMetadataKey = "x-clrk-extproc-role"
	extprocRoleUpstream    = "upstream"

	// llmDNSRefreshRate bounds STRICT_DNS re-resolution of the
	// synthesized clusters' endpoint hostnames. EG 1.4 lacks listener
	// context at PostTranslateModify, so every EG's Envoy receives
	// every clrk-llm cluster and resolves its hostnames eagerly — a
	// long refresh (plus RespectDnsTtl) keeps that fan-out cheap until
	// the EG dep bump makes emission listener-scoped.
	llmDNSRefreshRate = 5 * time.Minute

	// llmDNSFailureRefresh* bound the retry interval after a FAILED
	// resolution. Without them Envoy reuses DnsRefreshRate for failures
	// too, and one transient resolver error at cluster warm-up leaves
	// the cluster empty — pinned traffic 503s "no healthy upstream" —
	// for the full five minutes.
	llmDNSFailureRefreshBase = 1 * time.Second
	llmDNSFailureRefreshMax  = 10 * time.Second

	// llmRetryMaxAttempts caps the default retry count derived from
	// the candidate list length.
	llmRetryMaxAttempts = 5
)

// llmRule is the synthesized view of one (AIProviderRoute, rule,
// provider) tuple on one EgressGateway: the stable key the route
// matches on, the candidate endpoints in spec order, and the fallback
// policy if one attaches to the route.
type llmRule struct {
	key        string
	candidates []llmroute.Candidate

	// fallback is non-nil when a FallbackRoutingPolicy attaches to the
	// containing route: candidate order becomes Envoy priority tiers
	// and the synthesized route carries a retry policy.
	fallback *clrkv1alpha1.FallbackRoutingPolicy
}

// llmConfig is everything PostHTTPListenerModify resolves per EG for
// the LLM data plane, cached on s.llmByEG so PostTranslateModify
// (which has no listener context on EG 1.4) can emit the matching
// clusters.
type llmConfig struct {
	rules          []llmRule
	requestTimeout time.Duration
	lookupFamily   clusterv3.Cluster_DnsLookupFamily
}

// llmConfigFor resolves the LLM rule set for one EgressGateway from
// the live AIProviderRoute / Backend / FallbackRoutingPolicy state and
// caches it for cluster emission. Errors degrade to an empty rule set
// (no synthesized routes; the catch-all DFP path still serves) — the
// extension server runs on EG's translation path and must always
// produce a usable config.
func (s *Server) llmConfigFor(ctx context.Context, key egKey) *llmConfig {
	logger := ctrllog.FromContext(ctx).WithName("egextension")
	cfg := &llmConfig{
		requestTimeout: clrkv1alpha1.DefaultEgressRequestTimeout,
		lookupFamily:   s.lookupFamilyFor(ctx, key),
	}
	defer s.llmByEG.Store(key, cfg)
	if s.Client == nil {
		return cfg
	}

	var eg clrkv1alpha1.EgressGateway
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: key.namespace, Name: key.name}, &eg); err == nil {
		cfg.requestTimeout = eg.Spec.EffectiveRequestTimeout()
	}

	var routes clrkv1alpha1.AIProviderRouteList
	if err := s.Client.List(ctx, &routes); err != nil {
		logger.Error(err, "Listing AIProviderRoutes for LLM route synthesis failed", "eg", key)
		return cfg
	}
	var backends clrkv1alpha1.BackendList
	if err := s.Client.List(ctx, &backends); err != nil {
		logger.Error(err, "Listing Backends for LLM route synthesis failed", "eg", key)
		return cfg
	}
	var policies clrkv1alpha1.FallbackRoutingPolicyList
	if err := s.Client.List(ctx, &policies); err != nil {
		logger.Error(err, "Listing FallbackRoutingPolicies for LLM route synthesis failed", "eg", key)
		return cfg
	}

	egNN := types.NamespacedName{Namespace: key.namespace, Name: key.name}
	byKey := llmroute.IndexBackends(backends.Items)
	seen := map[string]bool{}
	for _, r := range routes.Items {
		if !llmroute.RouteAttachesTo(r, key.namespace, key.name) {
			continue
		}
		fallback := llmroute.FallbackFor(policies.Items, r.Namespace, r.Name)
		for ruleIdx, rule := range r.Spec.Rules {
			resolved := llmroute.ResolveCandidates(rule.BackendRefs, r.Namespace, byKey)
			if len(resolved) == 0 {
				continue
			}
			// One synthesized (route, cluster) pair per provider the
			// rule matches on: the translatable candidate set depends
			// on the source schema, and the flattened pin-side
			// routeRule carries exactly one provider. Multiple matches
			// sharing a provider dedupe onto one key.
			for _, m := range rule.Matches {
				prov := llmroute.CanonicalProvider(m.Provider)
				rk := llmroute.RuleKey(egNN, types.NamespacedName{Namespace: r.Namespace, Name: r.Name}, ruleIdx, prov)
				if seen[rk] {
					continue
				}
				cands := servableCandidates(llmroute.TranslatableCandidates(resolved, prov))
				if len(cands) == 0 {
					continue
				}
				seen[rk] = true
				cfg.rules = append(cfg.rules, llmRule{key: rk, candidates: cands, fallback: fallback})
			}
		}
	}
	return cfg
}

// servableCandidates drops candidates that cannot be cluster
// endpoints: Refuse (InferencePool) entries — the downstream 501 path
// handles those — and anything without a resolvable host.
func servableCandidates(cands []llmroute.Candidate) []llmroute.Candidate {
	out := make([]llmroute.Candidate, 0, len(cands))
	for _, c := range cands {
		if c.Refuse || c.Host == "" {
			continue
		}
		out = append(out, c)
	}
	return out
}

// buildLLMRoutes renders the synthesized routes, one per llmRule, to
// be installed AHEAD of the catch-all DFP route. Each matches any path
// plus an exact PinHeader value, strips that header before the request
// leaves, pins the rule's cluster, and carries the EG request timeout
// (the catch-all HTTPRoute's translated timeouts die when rewriteHCM
// replaces EG's route config, so synthesized routes must self-assert).
// A retry policy is stamped only when the rule's route has an attached
// FallbackRoutingPolicy.
func buildLLMRoutes(cfg *llmConfig) []*routev3.Route {
	out := make([]*routev3.Route, 0, len(cfg.rules))
	for _, rule := range cfg.rules {
		ra := &routev3.RouteAction{
			ClusterSpecifier: &routev3.RouteAction_Cluster{
				Cluster: llmroute.ClusterName(rule.key),
			},
			Timeout: durationpb.New(cfg.requestTimeout),
		}
		if rule.fallback != nil {
			ra.RetryPolicy = buildLLMRetryPolicy(rule)
		}
		out = append(out, &routev3.Route{
			Name: llmroute.ClusterName(rule.key),
			Match: &routev3.RouteMatch{
				PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: "/"},
				Headers: []*routev3.HeaderMatcher{{
					Name: llmroute.PinHeader,
					HeaderMatchSpecifier: &routev3.HeaderMatcher_StringMatch{
						StringMatch: &matcherv3.StringMatcher{
							MatchPattern: &matcherv3.StringMatcher_Exact{Exact: rule.key},
						},
					},
				}},
			},
			// The pin header is clrk-internal routing state; it must
			// not leak to the provider.
			RequestHeadersToRemove: []string{llmroute.PinHeader},
			Action:                 &routev3.Route_Route{Route: ra},
			// Router-side retry buffer: bodies above this stay
			// servable but silently lose retry eligibility. Distinct
			// knob from the connection buffer that bounds the
			// downstream ext_proc's BUFFERED body delivery.
			RequestBodyBufferLimit: wrapperspb.UInt64(llmroute.RetryBodyBufferBytes),
		})
	}
	return out
}

// buildLLMRetryPolicy renders the Envoy retry policy for a rule whose
// route has an attached FallbackRoutingPolicy. previous_priorities +
// previous_hosts make each retry attempt walk to a not-yet-tried
// endpoint, exhausting priority tiers in candidate order. Retries only
// fire before upstream response headers are forwarded downstream, so
// streams in flight are never cut by a retry.
func buildLLMRetryPolicy(rule llmRule) *routev3.RetryPolicy {
	var retry *clrkv1alpha1.FallbackRetry
	if rule.fallback != nil {
		retry = rule.fallback.Spec.Retry
	}

	numRetries := len(rule.candidates) - 1
	if numRetries > llmRetryMaxAttempts {
		numRetries = llmRetryMaxAttempts
	}
	if retry != nil && retry.NumRetries != nil && *retry.NumRetries >= 0 {
		numRetries = int(*retry.NumRetries)
	}
	codes := []uint32{429, 503}
	if retry != nil && len(retry.RetriableStatusCodes) > 0 {
		codes = make([]uint32, 0, len(retry.RetriableStatusCodes))
		for _, c := range retry.RetriableStatusCodes {
			codes = append(codes, uint32(c))
		}
	}

	prevPriorities, _ := anypb.New(&previousprioritiesv3.PreviousPrioritiesConfig{UpdateFrequency: 1})
	prevHosts, _ := anypb.New(&previoushostsv3.PreviousHostsPredicate{})
	rp := &routev3.RetryPolicy{
		RetryOn:              "connect-failure,refused-stream,reset,retriable-status-codes",
		NumRetries:           wrapperspb.UInt32(uint32(numRetries)),
		RetriableStatusCodes: codes,
		RetryPriority: &routev3.RetryPolicy_RetryPriority{
			Name: "envoy.retry_priorities.previous_priorities",
			ConfigType: &routev3.RetryPolicy_RetryPriority_TypedConfig{
				TypedConfig: prevPriorities,
			},
		},
		RetryHostPredicate: []*routev3.RetryPolicy_RetryHostPredicate{{
			Name: "envoy.retry_host_predicates.previous_hosts",
			ConfigType: &routev3.RetryPolicy_RetryHostPredicate_TypedConfig{
				TypedConfig: prevHosts,
			},
		}},
		HostSelectionRetryMaxAttempts: 5,
	}
	if retry != nil && retry.PerTryTimeout != nil && retry.PerTryTimeout.Duration > 0 {
		rp.PerTryTimeout = durationpb.New(retry.PerTryTimeout.Duration)
	}
	return rp
}

// buildLLMCluster renders one rule's multi-endpoint cluster.
//
// Topology by policy attachment: without a FallbackRoutingPolicy every
// endpoint sits at priority 0 carrying its BackendRef weight — Envoy's
// LB does the weighted pick natively (replacing the in-extproc
// staticSelector). With one, the candidate index becomes the priority
// tier and weights are ignored.
//
// Each endpoint carries three metadata namespaces: envoy.lb (subset
// key, lets the downstream ext_proc pin a single backend when
// per-request gates shrink the viable set), envoy.transport_socket_match
// (per-endpoint SNI/SAN — these are real per-provider hostnames, NOT
// the DFP's authority-derived auto_sni, which would validate the
// per-attempt-mutated authority against the wrong cert), and
// clrk.apoxy.dev (the backend identity the upstream ext_proc reads via
// xds.upstream_host_metadata to know which Backend the attempt
// targets).
func (s *Server) buildLLMCluster(rule llmRule, key egKey, lookupFamily clusterv3.Cluster_DnsLookupFamily) (*clusterv3.Cluster, error) {
	var (
		localities []*endpointv3.LocalityLbEndpoints
		tsMatches  []*clusterv3.Cluster_TransportSocketMatch
		defaultTLS *anypb.Any
	)
	flat := &endpointv3.LocalityLbEndpoints{}
	for i, c := range rule.candidates {
		tlsAny, err := buildLLMEndpointTLS(c)
		if err != nil {
			return nil, fmt.Errorf("endpoint TLS for backend %s/%s: %w", c.Namespace, c.Name, err)
		}
		// Envoy validates AutoConfig (ALPN-negotiated h2/h1) against the
		// cluster's DEFAULT transport socket, and transport_socket_matches
		// alone leave that default at raw_buffer — the cluster gets
		// NACKed with "ALPN configured ... non-ALPN transport socket".
		// Use the first endpoint's TLS context as the default: every
		// endpoint carries a match so the default never serves, and if a
		// future bug ever routed through it the wrong-SNI handshake fails
		// closed instead of skipping SAN verification.
		if defaultTLS == nil {
			defaultTLS = tlsAny
		}
		tsMatchKey := c.Namespace + "/" + c.Name
		tsMatches = append(tsMatches, &clusterv3.Cluster_TransportSocketMatch{
			Name: "tls-" + strconv.Itoa(i),
			Match: &structpb.Struct{Fields: map[string]*structpb.Value{
				"clrk_tls": structpb.NewStringValue(tsMatchKey),
			}},
			TransportSocket: &corev3.TransportSocket{
				Name:       tlsTransportSocketName,
				ConfigType: &corev3.TransportSocket_TypedConfig{TypedConfig: tlsAny},
			},
		})

		ep := &endpointv3.LbEndpoint{
			HostIdentifier: &endpointv3.LbEndpoint_Endpoint{
				Endpoint: &endpointv3.Endpoint{
					Address: &corev3.Address{
						Address: &corev3.Address_SocketAddress{
							SocketAddress: &corev3.SocketAddress{
								Address: c.Host,
								PortSpecifier: &corev3.SocketAddress_PortValue{
									PortValue: uint32(c.Port),
								},
							},
						},
					},
				},
			},
			Metadata: &corev3.Metadata{FilterMetadata: map[string]*structpb.Struct{
				"envoy.lb": {Fields: map[string]*structpb.Value{
					llmroute.SubsetKeyBackend: structpb.NewStringValue(c.Name),
				}},
				"envoy.transport_socket_match": {Fields: map[string]*structpb.Value{
					"clrk_tls": structpb.NewStringValue(tsMatchKey),
				}},
				llmroute.EndpointMetaNamespace: {Fields: map[string]*structpb.Value{
					llmroute.EndpointMetaBackendNamespace: structpb.NewStringValue(c.Namespace),
					llmroute.EndpointMetaBackendName:      structpb.NewStringValue(c.Name),
					llmroute.EndpointMetaBackendSchema:    structpb.NewStringValue(c.System),
					llmroute.EndpointMetaBackendHost:      structpb.NewStringValue(c.Host),
					llmroute.EndpointMetaBackendPort:      structpb.NewStringValue(strconv.Itoa(c.Port)),
				}},
			}},
		}
		if rule.fallback != nil {
			// Ordered fallback: one endpoint per priority tier.
			localities = append(localities, &endpointv3.LocalityLbEndpoints{
				Priority:    uint32(i),
				LbEndpoints: []*endpointv3.LbEndpoint{ep},
			})
			continue
		}
		// Weighted distribution: one flat tier. Gateway API weight 0
		// means "no traffic" — those endpoints are simply omitted.
		if c.Weight <= 0 {
			continue
		}
		ep.LoadBalancingWeight = wrapperspb.UInt32(uint32(c.Weight))
		flat.LbEndpoints = append(flat.LbEndpoints, ep)
	}
	if rule.fallback == nil {
		localities = []*endpointv3.LocalityLbEndpoints{flat}
	}

	httpOptsAny, err := s.buildLLMUpstreamHTTPOptions(key)
	if err != nil {
		return nil, err
	}
	return &clusterv3.Cluster{
		Name:                 llmroute.ClusterName(rule.key),
		ConnectTimeout:       durationpb.New(5 * time.Second),
		ClusterDiscoveryType: &clusterv3.Cluster_Type{Type: clusterv3.Cluster_STRICT_DNS},
		DnsLookupFamily:      lookupFamily,
		RespectDnsTtl:        true,
		DnsRefreshRate:       durationpb.New(llmDNSRefreshRate),
		DnsFailureRefreshRate: &clusterv3.Cluster_RefreshRate{
			BaseInterval: durationpb.New(llmDNSFailureRefreshBase),
			MaxInterval:  durationpb.New(llmDNSFailureRefreshMax),
		},
		LbPolicy: clusterv3.Cluster_ROUND_ROBIN,
		// Subset selection on the backend name. Inert until a request
		// carries envoy.lb dynamic metadata: the downstream ext_proc
		// sets it only when per-request gates (model-rewrite coverage,
		// schema capability) shrink the rule's viable set below the
		// cluster's full membership, pinning the attempt to a backend
		// that can actually serve. The two fallback levels encode the
		// two failure shapes: a request with NO pin must use the whole
		// cluster (cluster-wide ANY_ENDPOINT — with NO_FALLBACK there,
		// metadata-less requests get the empty fallback subset and 503
		// "no healthy upstream"), while a pinned request whose backend
		// no longer exists must stay an honest 503 instead of silently
		// mis-routing (selector-level NO_FALLBACK).
		LbSubsetConfig: &clusterv3.Cluster_LbSubsetConfig{
			FallbackPolicy: clusterv3.Cluster_LbSubsetConfig_ANY_ENDPOINT,
			SubsetSelectors: []*clusterv3.Cluster_LbSubsetConfig_LbSubsetSelector{{
				Keys:           []string{llmroute.SubsetKeyBackend},
				FallbackPolicy: clusterv3.Cluster_LbSubsetConfig_LbSubsetSelector_NO_FALLBACK,
			}},
		},
		// Passive health: consecutive upstream 5xx ejects an endpoint
		// for a cooldown, so under an attached fallback policy a dead
		// primary stops eating the first attempt of every request.
		// MaxEjectionPercent 100 deliberately allows ejecting whole
		// tiers — panic routing still serves if everything is sick.
		OutlierDetection: &clusterv3.OutlierDetection{
			Consecutive_5Xx:    wrapperspb.UInt32(5),
			BaseEjectionTime:   durationpb.New(30 * time.Second),
			MaxEjectionPercent: wrapperspb.UInt32(100),
		},
		LoadAssignment: &endpointv3.ClusterLoadAssignment{
			ClusterName: llmroute.ClusterName(rule.key),
			Endpoints:   localities,
		},
		TransportSocket: &corev3.TransportSocket{
			Name:       tlsTransportSocketName,
			ConfigType: &corev3.TransportSocket_TypedConfig{TypedConfig: defaultTLS},
		},
		TransportSocketMatches: tsMatches,
		TypedExtensionProtocolOptions: map[string]*anypb.Any{
			"envoy.extensions.upstreams.http.v3.HttpProtocolOptions": httpOptsAny,
		},
	}, nil
}

// buildLLMEndpointTLS renders one endpoint's UpstreamTlsContext: SNI
// pinned to the Backend host, SAN-verified against the same name, the
// system trust bundle (which the EG controller's init container
// overlays with any AdditionalTrustBundle at the same path), and h2
// ALPN.
func buildLLMEndpointTLS(c llmroute.Candidate) (*anypb.Any, error) {
	tlsCtx := &tlsv3.UpstreamTlsContext{
		Sni: c.Host,
		CommonTlsContext: &tlsv3.CommonTlsContext{
			AlpnProtocols: []string{"h2", "http/1.1"},
			ValidationContextType: &tlsv3.CommonTlsContext_ValidationContext{
				ValidationContext: &tlsv3.CertificateValidationContext{
					TrustedCa: &corev3.DataSource{
						Specifier: &corev3.DataSource_Filename{
							Filename: "/etc/ssl/certs/ca-certificates.crt",
						},
					},
					MatchTypedSubjectAltNames: []*tlsv3.SubjectAltNameMatcher{{
						SanType: tlsv3.SubjectAltNameMatcher_DNS,
						Matcher: &matcherv3.StringMatcher{
							MatchPattern: &matcherv3.StringMatcher_Exact{Exact: c.Host},
						},
					}},
				},
			},
		},
	}
	return anypb.New(tlsCtx)
}

// buildLLMUpstreamHTTPOptions renders the cluster-level HTTP options
// carrying the upstream filter chain: the per-attempt ext_proc filter
// followed by the mandatory terminal upstream_codec. The ext_proc
// targets the same controller-manager gRPC service as the downstream
// filter, stamped with the per-EG authority (so the stream resolves
// its EG registry/credentials) plus the upstream role marker.
func (s *Server) buildLLMUpstreamHTTPOptions(key egKey) (*anypb.Any, error) {
	extProc, err := buildLLMUpstreamExtProcFilter(s.ExtProcTargetURI, egidentity.AuthorityFor(types.NamespacedName{
		Namespace: key.namespace,
		Name:      key.name,
	}))
	if err != nil {
		return nil, err
	}
	codecAny, err := anypb.New(&upstreamcodecv3.UpstreamCodec{})
	if err != nil {
		return nil, fmt.Errorf("marshal upstream codec: %w", err)
	}
	httpOpts := &upstreamhttpv3.HttpProtocolOptions{
		UpstreamProtocolOptions: &upstreamhttpv3.HttpProtocolOptions_AutoConfig{
			AutoConfig: &upstreamhttpv3.HttpProtocolOptions_AutoHttpConfig{
				Http2ProtocolOptions: &corev3.Http2ProtocolOptions{},
				HttpProtocolOptions:  &corev3.Http1ProtocolOptions{},
			},
		},
		HttpFilters: []*hcmv3.HttpFilter{
			extProc,
			{
				Name:       upstreamCodecFilterName,
				ConfigType: &hcmv3.HttpFilter_TypedConfig{TypedConfig: codecAny},
			},
		},
	}
	return anypb.New(httpOpts)
}

// buildLLMUpstreamExtProcFilter constructs the cluster-level ext_proc
// filter that adapts each router attempt's REQUEST to the endpoint
// Envoy picked: it learns the backend from xds.upstream_host_metadata,
// rewrites :authority/:path/body for that backend's schema, and
// injects its credentials. The request body mode is BUFFERED (the
// router replays the buffered body through the filter per attempt,
// bounded by the route's retry buffer).
//
// The RESPONSE side is deliberately not processed here (header mode
// SKIP, body mode NONE, no overrides): any upstream filter that holds
// response headers for a gRPC round trip races the router's retry
// decision — a retriable non-200's body piles up behind the held
// headers, the router pops the upstream request when they release, and
// the late body delivery violates the router's
// upstream_requests_.size() == 1 invariant (a fatal abort on
// assert-enabled Envoy builds, found on the dev spike; undefined
// behavior upstream considers a bug either way). Response adaptation
// doesn't need to run per attempt at all: translation and capture key
// off the FINAL serving attempt, which the downstream filter handles
// with the backend identity recorded by this filter's request phase.
func buildLLMUpstreamExtProcFilter(targetURI, authority string) (*hcmv3.HttpFilter, error) {
	any, err := anypb.New(&extprocv3.ExternalProcessor{
		GrpcService: &corev3.GrpcService{
			TargetSpecifier: &corev3.GrpcService_GoogleGrpc_{
				GoogleGrpc: &corev3.GrpcService_GoogleGrpc{
					TargetUri:  targetURI,
					StatPrefix: "clrk_ext_proc_upstream",
					ChannelArgs: &corev3.GrpcService_GoogleGrpc_ChannelArgs{
						Args: map[string]*corev3.GrpcService_GoogleGrpc_ChannelArgs_Value{
							"grpc.default_authority": {
								ValueSpecifier: &corev3.GrpcService_GoogleGrpc_ChannelArgs_Value_StringValue{
									StringValue: authority,
								},
							},
						},
					},
				},
			},
			Timeout: durationpb.New(defaultExtProcTimeout),
			InitialMetadata: []*corev3.HeaderValue{{
				Key:   extprocRoleMetadataKey,
				Value: extprocRoleUpstream,
			}},
		},
		ProcessingMode: &extprocv3.ProcessingMode{
			RequestHeaderMode:   extprocv3.ProcessingMode_SEND,
			ResponseHeaderMode:  extprocv3.ProcessingMode_SKIP,
			RequestBodyMode:     extprocv3.ProcessingMode_BUFFERED,
			ResponseBodyMode:    extprocv3.ProcessingMode_NONE,
			RequestTrailerMode:  extprocv3.ProcessingMode_SKIP,
			ResponseTrailerMode: extprocv3.ProcessingMode_SKIP,
		},
		MessageTimeout: durationpb.New(5 * time.Second),
		// The selected endpoint's metadata is the attempt's backend
		// identity — the whole point of running upstream.
		RequestAttributes: []string{"xds.upstream_host_metadata"},
		// :authority/:path are mutated per attempt; routing has already
		// happened so this is safe by construction, but mutation_rules
		// must still allow it.
		MutationRules: &mutationrulesv3.HeaderMutationRules{
			AllowAllRouting: wrapperspb.Bool(true),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("marshal upstream ext_proc filter: %w", err)
	}
	return &hcmv3.HttpFilter{
		Name:       extProcFilterName,
		ConfigType: &hcmv3.HttpFilter_TypedConfig{TypedConfig: any},
	}, nil
}
