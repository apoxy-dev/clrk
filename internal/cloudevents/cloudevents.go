// Package cloudevents builds CloudEvents (CNCF) attribute sets for
// TaskAgent dispatches. The same construction is used by ingress
// ext_proc (when it sets ce-* request headers) and by the worker
// dispatcher (which reads those headers and falls back to building
// the same set if ext_proc was bypassed). Centralizing it here keeps
// the two places in sync.
package cloudevents

import (
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/ports"
)

// HeaderLookup is a minimal adapter over a normalized (lowercase-keyed)
// header map. ext_proc operates on the gRPC `HeaderMap` representation
// rather than `*http.Request`; both call sites resolve attributes
// through this interface so the fallback chain stays in one place.
type HeaderLookup interface {
	// Get returns the first value for the (case-insensitive) header
	// name, or "" if absent.
	Get(string) string
}

// HeaderMap is a trivial map[string]string lookup with case-folded
// keys. ext_proc constructs one of these from a gRPC HeaderMap; the
// dispatcher can pass an http.Header via the http.Header adapter.
type HeaderMap map[string]string

func (h HeaderMap) Get(k string) string { return h[strings.ToLower(k)] }

// HTTPHeader wraps stdlib http.Header behind the HeaderLookup
// interface so the dispatcher can share the construction code with
// ext_proc.
type HTTPHeader http.Header

func (h HTTPHeader) Get(k string) string { return http.Header(h).Get(k) }

// CE attribute names per the CNCF CloudEvents 1.0 spec. Lowercase
// per the HTTP binding rules; we keep them as constants to avoid
// scattering string literals across producers.
const (
	AttrID              = "id"
	AttrSource          = "source"
	AttrType            = "type"
	AttrSpecVersion     = "specversion"
	AttrTime            = "time"
	AttrDataContentType = "datacontenttype"
	AttrSubject         = "subject"

	// AttrClrkRevision is the AgentSandboxRevision name the dispatch
	// resolved against. CE extension attributes must be lowercase
	// alphanumeric per the spec — no separators.
	AttrClrkRevision = "clrkrevision"

	// AttrClrkIdentity holds joined audiences from spec.identity
	// (comma-separated). Empty if the TaskAgent has no identity.
	AttrClrkIdentity = "clrkidentity"

	// HTTP-binding extension attributes per the CloudEvents `http`
	// extension. We carry these so an agent receiving the envelope can
	// distinguish e.g. GET /webhook vs POST / vs DELETE /sessions/X
	// — without them, the envelope is method/path agnostic and the
	// agent has no way to act on the request line.
	//
	// httpmethod : original request method (GET, POST, DELETE, …).
	// httpurl    : path-only (no scheme/host); query string lives in
	//              httpquery so each piece round-trips through the
	//              CE binary-mode header form without escaping.
	// httpquery  : raw query string ("a=1&b=2"), no leading "?".
	AttrHTTPMethod = "httpmethod"
	AttrHTTPURL    = "httpurl"
	AttrHTTPQuery  = "httpquery"
)

const (
	// SpecVersion is the CloudEvents spec version we emit.
	SpecVersion = "1.0"

	// TypeInvoke is the ce-type for inbound TaskAgent dispatches.
	TypeInvoke = "dev.apoxy.clrk.taskagent.invoke"

	// TypeResponse is the ce-type for response envelopes.
	TypeResponse = "dev.apoxy.clrk.taskagent.response"

	// AttrRelationID names the CE extension carrying the request's
	// ce-id on a response envelope (binding the response back to the
	// invocation for idempotency / correlation).
	AttrRelationID = "relationid"

	// MediaType is the MIME type for structured-mode CE JSON.
	MediaType = "application/cloudevents+json"
)

// AttrsFromRequest builds the canonical CE attribute set (binary
// mode keys, no `ce-` prefix) for an HTTP request and TaskAgent.
// Convenience wrapper around AttrsFromHeaders.
func AttrsFromRequest(r *http.Request, ta *clrkv1alpha1.TaskAgent) map[string]string {
	passThrough := ceHeaderIter(r.Header)
	// HTTP-extension attrs come from the request line, not headers —
	// fold them into the passThrough map so AttrsFromHeaders treats
	// them as already-resolved (and any caller-set ce-httpmethod /
	// ce-httpurl / ce-httpquery still wins, mirroring the precedence
	// rule for the rest of the attrs).
	if r.Method != "" {
		if _, ok := passThrough[AttrHTTPMethod]; !ok {
			passThrough[AttrHTTPMethod] = r.Method
		}
	}
	if r.URL != nil {
		if _, ok := passThrough[AttrHTTPURL]; !ok && r.URL.Path != "" {
			passThrough[AttrHTTPURL] = r.URL.Path
		}
		if _, ok := passThrough[AttrHTTPQuery]; !ok && r.URL.RawQuery != "" {
			passThrough[AttrHTTPQuery] = r.URL.RawQuery
		}
	}
	return AttrsFromHeaders(HTTPHeader(r.Header), ta, passThrough)
}

// AttrsFromHeaders is the shared construction path used by both
// ext_proc (which carries headers as a normalized map keyed
// case-foldedly) and the worker dispatcher (which uses
// http.Header). Existing ce-* headers passed in via passThrough win
// — for any missing attribute the helper fills in the same value
// ext_proc would.
func AttrsFromHeaders(h HeaderLookup, ta *clrkv1alpha1.TaskAgent, passThrough map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range passThrough {
		out[k] = v
	}

	if _, ok := out[AttrID]; !ok {
		out[AttrID] = ResolveID(h)
	}
	if _, ok := out[AttrSource]; !ok && ta != nil {
		out[AttrSource] = "clrk://" + ta.Namespace + "/" + ta.Name
	}
	if _, ok := out[AttrType]; !ok {
		out[AttrType] = TypeInvoke
	}
	if _, ok := out[AttrTime]; !ok {
		out[AttrTime] = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if _, ok := out[AttrDataContentType]; !ok {
		if ct := h.Get("Content-Type"); ct != "" {
			out[AttrDataContentType] = ct
		}
	}
	if _, ok := out[AttrSubject]; !ok {
		if sub := h.Get(ports.HeaderTrigger); sub != "" {
			out[AttrSubject] = sub
		}
	}
	if _, ok := out[AttrClrkRevision]; !ok && ta != nil {
		if rev := ta.Status.LatestReadyRevisionName; rev != "" {
			out[AttrClrkRevision] = rev
		}
	}
	if _, ok := out[AttrClrkIdentity]; !ok && ta != nil && ta.Spec.Identity != nil {
		if aud := identityAudiences(ta.Spec.Identity); aud != "" {
			out[AttrClrkIdentity] = aud
		}
	}
	// HTTP-binding extension attrs: ext_proc has the request line in
	// the HTTP/2 pseudo-headers (`:method`, `:path`). The dispatcher
	// path pre-populates these into passThrough from r.Method / r.URL,
	// so this branch is the ext_proc fallback.
	if _, ok := out[AttrHTTPMethod]; !ok {
		if m := h.Get(":method"); m != "" {
			out[AttrHTTPMethod] = m
		}
	}
	if _, ok := out[AttrHTTPURL]; !ok {
		if p := h.Get(":path"); p != "" {
			path, query, _ := strings.Cut(p, "?")
			out[AttrHTTPURL] = path
			if query != "" {
				if _, qok := out[AttrHTTPQuery]; !qok {
					out[AttrHTTPQuery] = query
				}
			}
		}
	}
	return out
}

// ResolveID applies the documented ce-id fallback chain:
//   - X-Clrk-Execution-ID (caller-supplied, idempotent across retries)
//   - x-request-id (Envoy-injected, fresh per request)
//   - generated UUID (truly rare — Envoy was bypassed)
//
// Caller-controlled idempotency lives in the first slot.
func ResolveID(h HeaderLookup) string {
	if v := h.Get(ports.HeaderExecutionID); v != "" {
		return v
	}
	if v := h.Get("X-Request-Id"); v != "" {
		return v
	}
	return uuid.NewString()
}

// ceHeaderIter pulls existing ce-* headers off an http.Header into
// a passThrough map suitable for AttrsFromHeaders. ext_proc has its
// own equivalent because it uses a non-http.Header type.
func ceHeaderIter(h http.Header) map[string]string {
	out := map[string]string{}
	for k, vs := range h {
		lower := strings.ToLower(k)
		if !strings.HasPrefix(lower, "ce-") || len(vs) == 0 {
			continue
		}
		attr := lower[len("ce-"):]
		if attr == "" {
			continue
		}
		out[attr] = vs[0]
	}
	return out
}

// CEHeaderIterMap mirrors ceHeaderIter for the case-folded HeaderMap
// representation that ext_proc uses (no per-key list of values).
// Exported because ext_proc lives in a different package.
func CEHeaderIterMap(m HeaderMap) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		if !strings.HasPrefix(k, "ce-") || v == "" {
			continue
		}
		attr := k[len("ce-"):]
		if attr == "" {
			continue
		}
		out[attr] = v
	}
	return out
}

// identityAudiences best-effort joins the TaskAgent's configured
// audiences. We don't have access to the upstream identity payload at
// dispatch time, so this is a static configured-set encoding rather
// than a per-request claim list. Returns "" when no audiences are
// configured.
func identityAudiences(id *clrkv1alpha1.AgentIdentity) string {
	if id == nil {
		return ""
	}
	// AgentIdentity is currently a placeholder shape; expose
	// audiences when/if the type adds them. Keeping the helper
	// here means consumers don't have to special-case the absent
	// field at every call site.
	return ""
}
