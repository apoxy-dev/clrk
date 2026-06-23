// Package capture holds the body-capture and HTTP request-line primitives
// shared by the egress sink (internal/extproc) and the ingress dispatch
// span (internal/extproc/ingress). Both surfaces capture request/response
// bodies into OTLP span events under the same contract -- the byte cap, the
// content-type allow-list, keep-first-N truncation, and :authority/url
// parsing -- so these primitives live in one place instead of being forked,
// which previously let the two drift (e.g. a url.full escaping fix landing
// on only one side).
package capture

import (
	"net"
	"net/url"
	"strconv"
	"strings"
)

// MaxBytesDefault bounds buffered request/response body bytes per stream
// when no per-EgressGateway CaptureBody override applies. The ingress path
// has no such override and always uses this default.
const MaxBytesDefault = 64 * 1024

// DefaultIncludedContentTypes is the body-capture content-type allow-list
// applied when no override is set, matched case-insensitively by prefix on
// the request/response Content-Type header.
var DefaultIncludedContentTypes = []string{
	"application/json",
	"application/x-ndjson",
	"text/event-stream",
}

// ContentTypeIncluded reports whether contentType matches any prefix in
// includedTypes. An empty includedTypes slice means "capture everything"
// (used when EG resolution fell back and no per-EG allow-list was derived).
func ContentTypeIncluded(contentType string, includedTypes []string) bool {
	if len(includedTypes) == 0 {
		return true
	}
	ct := strings.ToLower(contentType)
	for _, t := range includedTypes {
		if strings.HasPrefix(ct, t) {
			return true
		}
	}
	return false
}

// AppendBounded appends src to dst up to *left bytes, truncating the
// remainder; returns whether truncation occurred. Keep-first-N: the salient
// head of a body (method/model/prompt, or a provider's leading JSON) sits at
// the front, so first-N is the right bound for request capture.
func AppendBounded(dst, src []byte, left *int) ([]byte, bool) {
	if *left <= 0 {
		return dst, len(src) > 0
	}
	if len(src) <= *left {
		*left -= len(src)
		return append(dst, src...), false
	}
	dst = append(dst, src[:*left]...)
	*left = 0
	return dst, true
}

// SplitAuthority returns the host and port portions of an HTTP/2 :authority,
// stripping IPv6 brackets; port 0 indicates no port was set (or it was
// malformed). Uses net.SplitHostPort so IPv6 literals like `[::1]:443` parse.
func SplitAuthority(authority string) (string, int) {
	if authority == "" {
		return "", 0
	}
	host, portStr, err := net.SplitHostPort(authority)
	if err != nil {
		// No port present -- common for HTTP/2 :authority. Strip IPv6
		// brackets if any and return port 0.
		return strings.Trim(authority, "[]"), 0
	}
	port, _ := strconv.Atoi(portStr)
	return host, port
}

// SplitPathQuery splits an HTTP/2 :path into the path and the raw query
// (without the leading '?'); query is "" when absent.
func SplitPathQuery(p string) (path, query string) {
	if i := strings.IndexByte(p, '?'); i >= 0 {
		return p[:i], p[i+1:]
	}
	return p, ""
}

// BuildURL composes url.full from PRE-SPLIT request-line components, so the
// path and query are escaped independently. Passing a path that still
// carries the query string would make url.URL percent-escape the '?' into
// the path (e.g. /v1/messages%3Fbeta=true) -- split first with
// SplitPathQuery. Returns "" when authority is empty.
func BuildURL(scheme, authority, path, query string) string {
	if authority == "" {
		return ""
	}
	u := url.URL{Scheme: scheme, Host: authority, Path: path, RawQuery: query}
	return u.String()
}
