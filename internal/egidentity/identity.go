// Package egidentity carries the EgressGateway identity from a calling
// Envoy proxy to the controller-manager's gRPC services (certprovider,
// ext_proc).
//
// The default (secure) path derives identity from the verified client
// certificate presented over mTLS: the per-EG client cert carries a single
// DNS SAN of the form `<eg-name>.<eg-namespace>.eg.clrk.local`, and the
// server interceptor maps that verified SAN to the calling EG. Because the
// SAN is signed by the control-plane root CA it cannot be forged — unlike
// the HTTP/2 :authority header, which any caller can set.
//
// The legacy path (behind --insecure-grpc-no-mtls, dev only) parses the
// per-EG authority off :authority instead. Peer-IP introspection is avoided
// either way — peer IP breaks under NAT (docker-bridge dev environments,
// ingress gateways) and doesn't scale to multiple EGs in one cluster.
package egidentity

import (
	"context"
	"crypto/x509"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"k8s.io/apimachinery/pkg/types"
)

// authoritySuffix terminates the synthetic per-EG authority. Anything
// landing on the gRPC server with a different suffix is treated as a
// non-EG caller (controller-manager's apiserver tools, health probes,
// etc.) and gets no EG identity attached.
const authoritySuffix = ".eg.clrk.local"

// AuthorityFor returns the synthetic :authority value Envoy should send
// when calling the controller-manager on behalf of the given EG.
func AuthorityFor(eg types.NamespacedName) string {
	return eg.Name + "." + eg.Namespace + authoritySuffix
}

// ParseAuthority is the inverse of AuthorityFor. It returns ok=false
// when the value doesn't match the synthetic format — caller should
// treat the request as non-EG (or reject it, depending on context).
func ParseAuthority(authority string) (types.NamespacedName, bool) {
	// Strip an optional :port — gRPC clients sometimes append the
	// dialed port to :authority and we only care about the host.
	host := authority
	if i := strings.LastIndex(host, ":"); i >= 0 && !strings.ContainsAny(host[i+1:], ".") {
		host = host[:i]
	}
	if !strings.HasSuffix(host, authoritySuffix) {
		return types.NamespacedName{}, false
	}
	prefix := strings.TrimSuffix(host, authoritySuffix)
	dot := strings.Index(prefix, ".")
	if dot <= 0 || dot == len(prefix)-1 {
		return types.NamespacedName{}, false
	}
	return types.NamespacedName{
		Name:      prefix[:dot],
		Namespace: prefix[dot+1:],
	}, true
}

type ctxKey struct{}

// WithEG stamps the EG identity on ctx. Used by tests and by the
// server interceptor below.
func WithEG(ctx context.Context, eg types.NamespacedName) context.Context {
	return context.WithValue(ctx, ctxKey{}, eg)
}

// FromContext returns the EG identity recorded by the interceptor. The
// second return is false when no EG was attached (non-EG caller, or
// the interceptor chain didn't run — bare unit tests, for example).
func FromContext(ctx context.Context) (types.NamespacedName, bool) {
	v, ok := ctx.Value(ctxKey{}).(types.NamespacedName)
	return v, ok
}

// authorityFromIncomingContext looks up :authority in the incoming
// gRPC metadata. grpc-go exposes the HTTP/2 pseudo-header verbatim as
// a metadata entry under that exact key.
func authorityFromIncomingContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if vs := md.Get(":authority"); len(vs) > 0 {
		return vs[0]
	}
	return ""
}

// LegacyAuthorityUnaryServerInterceptor returns a grpc.UnaryServerInterceptor
// that parses :authority and stashes the resolved EG identity on ctx. This is
// the dev-only path behind --insecure-grpc-no-mtls; the secure default is
// PeerCertUnaryServerInterceptor. Requests with non-EG :authority pass through
// unchanged — handlers using FromContext will see ok=false.
func LegacyAuthorityUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if eg, ok := ParseAuthority(authorityFromIncomingContext(ctx)); ok {
			ctx = WithEG(ctx, eg)
		}
		return handler(ctx, req)
	}
}

// LegacyAuthorityStreamServerInterceptor mirrors
// LegacyAuthorityUnaryServerInterceptor for streaming RPCs (ext_proc, the
// FetchCertificate-style streams).
func LegacyAuthorityStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		if eg, ok := ParseAuthority(authorityFromIncomingContext(ctx)); ok {
			return handler(srv, &wrappedStream{ServerStream: ss, ctx: WithEG(ctx, eg)})
		}
		return handler(srv, ss)
	}
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

// FromPeerCert returns the EG identity encoded in a verified client
// certificate: the first DNS SAN that parses as a synthetic EG authority.
func FromPeerCert(cert *x509.Certificate) (types.NamespacedName, bool) {
	for _, dns := range cert.DNSNames {
		if eg, ok := ParseAuthority(dns); ok {
			return eg, true
		}
	}
	return types.NamespacedName{}, false
}

// peerCertEG extracts the EG identity from the verified client-cert chain on
// ctx. It reads only VerifiedChains (the chain the TLS stack validated against
// the control-plane root under RequireAndVerifyClientCert), never the
// unverified PeerCertificates.
func peerCertEG(ctx context.Context) (types.NamespacedName, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return types.NamespacedName{}, false
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return types.NamespacedName{}, false
	}
	chains := tlsInfo.State.VerifiedChains
	if len(chains) == 0 || len(chains[0]) == 0 {
		return types.NamespacedName{}, false
	}
	return FromPeerCert(chains[0][0])
}

// PeerCertUnaryServerInterceptor returns a grpc.UnaryServerInterceptor that
// resolves the EG identity from the verified client certificate and stashes it
// on ctx. This is the secure default; the :authority header is ignored.
func PeerCertUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if eg, ok := peerCertEG(ctx); ok {
			ctx = WithEG(ctx, eg)
		}
		return handler(ctx, req)
	}
}

// PeerCertStreamServerInterceptor mirrors PeerCertUnaryServerInterceptor for
// streaming RPCs.
func PeerCertStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		if eg, ok := peerCertEG(ctx); ok {
			return handler(srv, &wrappedStream{ServerStream: ss, ctx: WithEG(ctx, eg)})
		}
		return handler(srv, ss)
	}
}

// MustFromContext is a convenience for handlers that require an EG
// identity. Returns a clean gRPC error suitable for surfacing through
// to the calling Envoy.
func MustFromContext(ctx context.Context) (types.NamespacedName, error) {
	eg, ok := FromContext(ctx)
	if !ok {
		return types.NamespacedName{}, fmt.Errorf("no EgressGateway identity on request — client certificate SAN did not match %q (or, under --insecure-grpc-no-mtls, :authority did not)", "*"+authoritySuffix)
	}
	return eg, nil
}
