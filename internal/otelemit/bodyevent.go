package otelemit

import (
	"encoding/base64"

	"go.opentelemetry.io/otel/attribute"
)

// SpanEvent* are the wire-frozen span-event names that carry captured HTTP
// headers and bodies. The console's span inspector keys off these names
// (and, for bodies, the clrk.body.* attributes BodyEventAttrs stamps) to
// locate and render the request/response headers and payload, so every
// producer -- the egress sink (internal/extproc) and the ingress dispatch
// span (internal/extproc/ingress) -- must emit the identical names.
const (
	SpanEventHTTPRequestHeaders  = "http.request.headers"
	SpanEventHTTPResponseHeaders = "http.response.headers"
	SpanEventHTTPRequestBody     = "http.request.body"
	SpanEventHTTPResponseBody    = "http.response.body"
)

// BodyEventAttrs builds the attribute set for an http.request.body /
// http.response.body span event. body is the bytes to ship -- content-
// decoded when the producer inflated the on-wire encoding, else the raw
// capture -- so clrk.body.bytes is its length and clrk.body.b64 the base64
// the console decodes and renders. contentEncoding, when non-empty, records
// the wire content-encoding the body arrived in: a breadcrumb on a decoded
// body, and the signal that a truncated/undecodable body's b64 is still the
// raw compressed bytes (consumers detect that via clrk.body.truncated and
// UTF-8/JSON validity). Shared by every body-event producer so they never
// drift on the contract the console depends on.
func BodyEventAttrs(body []byte, truncated bool, contentEncoding string) []attribute.KeyValue {
	out := []attribute.KeyValue{
		attribute.Int(AttrBodyBytes, len(body)),
		attribute.Bool(AttrBodyTruncated, truncated),
		attribute.String(AttrBodyB64, base64.StdEncoding.EncodeToString(body)),
	}
	if contentEncoding != "" {
		out = append(out, attribute.String(AttrBodyContentEncoding, contentEncoding))
	}
	return out
}
