// Package egidentity carries the EgressGateway identity from a calling
// Envoy proxy to the controller-manager's gRPC services (certprovider,
// ext_proc) over the HTTP/2 :authority pseudo-header.
//
// The handshaker and ext_proc filters are programmed with a per-EG
// authority of the form `<eg-name>.<eg-namespace>.eg.clrk.local`. On
// the server side, the gRPC interceptor in this package parses
// :authority off incoming metadata and stashes the resolved EG identity
// on the request context. certprovider and ext_proc read it from there
// instead of doing peer-IP introspection — peer IP breaks under NAT
// (docker-bridge dev environments, ingress gateways) and doesn't scale
// to multiple EGs in one cluster.
package egidentity

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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

// UnaryServerInterceptor returns a grpc.UnaryServerInterceptor that
// parses :authority and stashes the resolved EG identity on ctx.
// Requests with non-EG :authority pass through unchanged — handlers
// using FromContext will see ok=false.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if eg, ok := ParseAuthority(authorityFromIncomingContext(ctx)); ok {
			ctx = WithEG(ctx, eg)
		}
		return handler(ctx, req)
	}
}

// StreamServerInterceptor mirrors UnaryServerInterceptor for streaming
// RPCs (ext_proc, the FetchCertificate-style streams).
func StreamServerInterceptor() grpc.StreamServerInterceptor {
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

// MustFromContext is a convenience for handlers that require an EG
// identity. Returns a clean gRPC error suitable for surfacing through
// to the calling Envoy.
func MustFromContext(ctx context.Context) (types.NamespacedName, error) {
	eg, ok := FromContext(ctx)
	if !ok {
		return types.NamespacedName{}, fmt.Errorf("no EgressGateway identity on request — :authority did not match %q", "*"+authoritySuffix)
	}
	return eg, nil
}
