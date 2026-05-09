package cloudevents

import (
	"net/http"
	"net/url"
	"testing"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// Both call sites — ext_proc (HeaderMap) and the dispatcher
// (HTTPHeader+request) — should produce httpmethod / httpurl /
// httpquery so an agent can act on the inbound verb and path.
// These are the bare-minimum invariants that make GETs (and any
// non-POST verb) carry usable context to the agent.

func TestAttrsFromHeaders_PseudoHeadersPopulateHTTPExtension(t *testing.T) {
	ta := &clrkv1alpha1.TaskAgent{}
	ta.Namespace = "ns1"
	ta.Name = "ta1"

	hdrs := HeaderMap{
		":method": "GET",
		":path":   "/v1/things?id=42&debug=1",
	}
	out := AttrsFromHeaders(hdrs, ta, nil)

	if got := out[AttrHTTPMethod]; got != "GET" {
		t.Fatalf("httpmethod: got %q want GET", got)
	}
	if got := out[AttrHTTPURL]; got != "/v1/things" {
		t.Fatalf("httpurl: got %q want /v1/things", got)
	}
	if got := out[AttrHTTPQuery]; got != "id=42&debug=1" {
		t.Fatalf("httpquery: got %q want id=42&debug=1", got)
	}
}

func TestAttrsFromHeaders_PathWithoutQueryLeavesHTTPQueryUnset(t *testing.T) {
	hdrs := HeaderMap{
		":method": "DELETE",
		":path":   "/sessions/abc",
	}
	out := AttrsFromHeaders(hdrs, nil, nil)

	if got := out[AttrHTTPMethod]; got != "DELETE" {
		t.Fatalf("httpmethod: got %q want DELETE", got)
	}
	if got := out[AttrHTTPURL]; got != "/sessions/abc" {
		t.Fatalf("httpurl: got %q want /sessions/abc", got)
	}
	if _, ok := out[AttrHTTPQuery]; ok {
		t.Fatalf("httpquery should be absent for query-less path; got %q", out[AttrHTTPQuery])
	}
}

func TestAttrsFromHeaders_PassThroughCEHTTPWins(t *testing.T) {
	// A caller that pre-stamps ce-httpmethod (e.g. a replay tool
	// constructing a synthetic dispatch) should win over what the
	// pseudo-headers say. Mirrors the precedence rule for every
	// other attr in AttrsFromHeaders.
	hdrs := HeaderMap{
		":method": "GET",
		":path":   "/real",
	}
	pass := map[string]string{
		AttrHTTPMethod: "POST",
		AttrHTTPURL:    "/replayed",
	}
	out := AttrsFromHeaders(hdrs, nil, pass)

	if got := out[AttrHTTPMethod]; got != "POST" {
		t.Fatalf("httpmethod: passThrough should win; got %q", got)
	}
	if got := out[AttrHTTPURL]; got != "/replayed" {
		t.Fatalf("httpurl: passThrough should win; got %q", got)
	}
}

func TestAttrsFromRequest_PopulatesHTTPExtensionFromRequestLine(t *testing.T) {
	ta := &clrkv1alpha1.TaskAgent{}
	ta.Namespace = "ns1"
	ta.Name = "ta1"

	r := &http.Request{
		Method: "PUT",
		URL:    &url.URL{Path: "/items/7", RawQuery: "fmt=json"},
		Header: http.Header{},
	}
	out := AttrsFromRequest(r, ta)

	if got := out[AttrHTTPMethod]; got != "PUT" {
		t.Fatalf("httpmethod: got %q want PUT", got)
	}
	if got := out[AttrHTTPURL]; got != "/items/7" {
		t.Fatalf("httpurl: got %q want /items/7", got)
	}
	if got := out[AttrHTTPQuery]; got != "fmt=json" {
		t.Fatalf("httpquery: got %q want fmt=json", got)
	}
}

func TestAttrsFromRequest_EmptyURLLeavesHTTPURLUnset(t *testing.T) {
	// Synthetic requests with no URL (rare but possible) shouldn't
	// stamp httpurl="" — leave it absent so the envelope doesn't
	// suggest the request had an empty path.
	r := &http.Request{
		Method: "GET",
		URL:    &url.URL{},
		Header: http.Header{},
	}
	out := AttrsFromRequest(r, nil)

	if got := out[AttrHTTPMethod]; got != "GET" {
		t.Fatalf("httpmethod: got %q want GET", got)
	}
	if _, ok := out[AttrHTTPURL]; ok {
		t.Fatalf("httpurl should be absent for empty URL.Path; got %q", out[AttrHTTPURL])
	}
	if _, ok := out[AttrHTTPQuery]; ok {
		t.Fatalf("httpquery should be absent for empty RawQuery; got %q", out[AttrHTTPQuery])
	}
}
