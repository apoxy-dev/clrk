// Package awssigv4 implements AWS Signature Version 4 request signing
// for credential injection at the egress proxy. It is hand-rolled on
// the stdlib rather than delegating to aws-sdk-go-v2 because the
// signature must cover the exact bytes Envoy forwards: the SDK signer
// re-parses and re-normalizes an *http.Request URL, which hides the
// double-encoding rule the verifier applies to paths carrying raw or
// percent-encoded reserved characters (bedrock model IDs contain `:`,
// ARN-form IDs contain `%2F`). Signing the wire bytes directly keeps
// the signer and AWS's verifier canonicalizing the same input.
//
// The signed header set is fixed and minimal —
// host;x-amz-content-sha256;x-amz-date(;x-amz-security-token) — so
// nothing agent-controlled enters the signature.
//
// SigV4 tolerates roughly +/-5 minutes of clock skew; a skewed
// controller-manager node manifests as blanket AWS 403s.
package awssigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Credentials is a static AWS credential set, typically read from a
// CredentialInjectionPolicy Secret.
type Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
	// SessionToken is set for STS temporary credentials; it adds
	// x-amz-security-token to the signed header set.
	SessionToken string
}

// Header is one lowercase header name/value pair to set on the signed
// request. Sign returns an ordered slice (not a map) so callers emit
// deterministic mutations.
type Header struct {
	Name  string
	Value string
}

// Sign computes an AWS Signature Version 4 over the request and
// returns the headers to set, in deterministic order: x-amz-date,
// x-amz-content-sha256, [x-amz-security-token,] authorization.
//
// host must be the :authority value exactly as it will leave Envoy
// (port suffix included, if any) — the signed `host` header has to
// byte-match what the upstream receives. pathAndQuery must be the
// final :path exactly as it will leave Envoy, already URL-encoded.
// body must be the final byte-exact payload after every mutation.
func Sign(method, host, pathAndQuery string, body []byte, creds Credentials, region, service string, now time.Time) []Header {
	amzDate := now.UTC().Format("20060102T150405Z")
	dateStamp := now.UTC().Format("20060102")
	payloadHash := hexSHA256(body)

	path, query, _ := strings.Cut(pathAndQuery, "?")

	canonicalHeaders := "host:" + host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	if creds.SessionToken != "" {
		canonicalHeaders += "x-amz-security-token:" + creds.SessionToken + "\n"
		signedHeaders += ";x-amz-security-token"
	}

	canonicalRequest := method + "\n" +
		canonicalURI(path) + "\n" +
		canonicalQuery(query) + "\n" +
		canonicalHeaders + "\n" +
		signedHeaders + "\n" +
		payloadHash

	scope := dateStamp + "/" + region + "/" + service + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" +
		amzDate + "\n" +
		scope + "\n" +
		hexSHA256([]byte(canonicalRequest))

	kDate := hmacSHA256([]byte("AWS4"+creds.SecretAccessKey), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))

	out := []Header{
		{Name: "x-amz-date", Value: amzDate},
		{Name: "x-amz-content-sha256", Value: payloadHash},
	}
	if creds.SessionToken != "" {
		out = append(out, Header{Name: "x-amz-security-token", Value: creds.SessionToken})
	}
	out = append(out, Header{
		Name: "authorization",
		Value: "AWS4-HMAC-SHA256 Credential=" + creds.AccessKeyID + "/" + scope +
			", SignedHeaders=" + signedHeaders +
			", Signature=" + signature,
	})
	return out
}

// RegionFromHost extracts the region from an AWS regional endpoint
// hostname:
//
//	bedrock-runtime.us-east-1.amazonaws.com               -> us-east-1
//	bedrock-runtime-fips.us-gov-west-1.amazonaws.com      -> us-gov-west-1
//	vpce-123-abc.bedrock-runtime.us-east-1.vpce.amazonaws.com -> us-east-1
//
// A port suffix is tolerated. Returns "" when the host doesn't parse
// as a regional amazonaws.com endpoint.
func RegionFromHost(host string) string {
	host, _, _ = strings.Cut(strings.ToLower(host), ":")
	labels := strings.Split(host, ".")
	n := len(labels)
	if n < 4 || labels[n-1] != "com" || labels[n-2] != "amazonaws" {
		return ""
	}
	// VPC endpoints insert a "vpce" label between the region and the
	// amazonaws.com suffix.
	region := labels[n-3]
	if region == "vpce" {
		if n < 6 {
			return ""
		}
		region = labels[n-4]
	}
	// Regions are always multi-label-with-dashes (us-east-1); a bare
	// label here means a non-regional endpoint shape.
	if !strings.Contains(region, "-") {
		return ""
	}
	return region
}

// canonicalURI applies SigV4's non-S3 path rule to the wire path: each
// slash-delimited segment is URI-encoded once MORE. The wire path is
// already single-encoded, so this produces the spec's double encoding
// (a raw `:` in a bedrock model ID becomes %3A, an already-escaped
// %3A becomes %253A) — self-consistent with AWS's verifier, which
// canonicalizes the received path bytes with the same function. No
// dot-segment normalization: ext_proc signs exactly the bytes Envoy
// forwards.
func canonicalURI(path string) string {
	if path == "" {
		return "/"
	}
	segs := strings.Split(path, "/")
	for i, s := range segs {
		segs[i] = awsURIEncode(s)
	}
	return strings.Join(segs, "/")
}

// canonicalQuery normalizes the raw query string: pairs are
// percent-decoded, re-encoded with the AWS rule, and sorted by encoded
// key then encoded value. A pair without `=` canonicalizes to `key=`.
func canonicalQuery(query string) string {
	if query == "" {
		return ""
	}
	var pairs [][2]string
	for _, kv := range strings.Split(query, "&") {
		if kv == "" {
			continue
		}
		k, v, _ := strings.Cut(kv, "=")
		pairs = append(pairs, [2]string{
			awsURIEncode(queryUnescape(k)),
			awsURIEncode(queryUnescape(v)),
		})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i][0] != pairs[j][0] {
			return pairs[i][0] < pairs[j][0]
		}
		return pairs[i][1] < pairs[j][1]
	})
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = p[0] + "=" + p[1]
	}
	return strings.Join(parts, "&")
}

// queryUnescape decodes a query component, treating `+` as space per
// form encoding (matching how AWS's verifier and net/url parse query
// strings). Undecodable input is canonicalized as-is rather than
// dropped — the signature must still cover those bytes.
func queryUnescape(s string) string {
	if u, err := url.QueryUnescape(s); err == nil {
		return u
	}
	return s
}

// awsURIEncode percent-encodes every byte outside the RFC 3986
// unreserved set [A-Za-z0-9-._~], with uppercase hex digits, per the
// SigV4 UriEncode definition.
func awsURIEncode(s string) string {
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' ||
			c == '-' || c == '.' || c == '_' || c == '~' {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hexDigits[c>>4])
		b.WriteByte(hexDigits[c&0xf])
	}
	return b.String()
}

func hexSHA256(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, msg string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(msg))
	return h.Sum(nil)
}
