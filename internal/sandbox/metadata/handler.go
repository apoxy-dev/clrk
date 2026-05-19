package metadata

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

const contentTypeCloudEventsJSON = "application/cloudevents+json"

// ctxKey types the request-context lookup for the SandboxID extracted
// from the PROXY v2 frame on the per-connection wrapper. Unexported so
// only the metadata package can stash/read it.
type ctxKey struct{}

// WithSandboxID returns ctx tagged with sandboxID. The PROXY v2-aware
// listener installs it on each request context so the HTTP handler
// can resolve the live Entry without keying on r.RemoteAddr.
func WithSandboxID(ctx context.Context, sandboxID string) context.Context {
	return context.WithValue(ctx, ctxKey{}, sandboxID)
}

// SandboxIDFromContext returns the SandboxID stashed by the wrapper.
// Empty when no frame was parsed (e.g. tests using httptest with a
// plain net.Conn) — the handler answers 404 in that case.
func SandboxIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxKey{}).(string)
	return v
}

// NewHandler returns the IMDS HTTP routes. lookup resolves the live
// *Entry for the requesting sandbox from the SandboxID carried in the
// request context (set by the PROXY v2 listener wrapper). Returns 404
// when no dispatch is in progress for that SandboxID — e.g. a warm
// sandbox polling /v1/event before the dispatcher has registered it.
//
// The linux-only Server in server.go uses this to assemble its
// net.Listen-backed http.Server; tests can use it directly with
// httptest.NewServer by passing a closure-backed EntryLookup and
// either setting r = r.WithContext(WithSandboxID(...)) or driving
// the wrapped listener through the same Accept path.
func NewHandler(lookup EntryLookup) http.Handler {
	h := &handler{lookup: lookup}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/event", h.handleEvent)
	mux.HandleFunc("/v1/response", h.handleResponse)
	return mux
}

type handler struct {
	lookup EntryLookup
}

// resolveEntry returns the live *Entry for the requesting sandbox or
// nil. Reads the SandboxID stashed on the request context by the
// PROXY v2 listener wrapper.
func (h *handler) resolveEntry(r *http.Request) *Entry {
	if h.lookup == nil {
		return nil
	}
	id := SandboxIDFromContext(r.Context())
	if id == "" {
		return nil
	}
	return h.lookup(id)
}

// handleEvent serves the request envelope. Binary mode by default
// (ce-* response headers + raw body). Structured mode when the
// caller advertises Accept: application/cloudevents+json.
func (h *handler) handleEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}

	entry := h.resolveEntry(r)
	if entry == nil {
		http.Error(w, "no live dispatch", http.StatusNotFound)
		return
	}

	if accepts(r, contentTypeCloudEventsJSON) {
		env := buildStructuredEnvelope(entry)
		w.Header().Set("Content-Type", contentTypeCloudEventsJSON)
		_ = json.NewEncoder(w).Encode(env)
		return
	}

	for k, v := range entry.Attrs {
		w.Header().Set("ce-"+k, v)
	}
	w.Header().Set("ce-id", entry.CEID)
	if entry.ContentType != "" {
		w.Header().Set("Content-Type", entry.ContentType)
	}
	_, _ = w.Write(entry.Body)
}

// handleResponse records the agent's response and signals Done.
// 204 on first delivery, 409 on duplicates so the agent's
// retry-after-failure path doesn't accidentally clobber a delivery
// that already shipped.
func (h *handler) handleResponse(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	entry := h.resolveEntry(r)
	if entry == nil {
		http.Error(w, "no live dispatch", http.StatusNotFound)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if !entry.SetResponse(body, r.Header.Get("Content-Type")) {
		http.Error(w, "response already delivered", http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// buildStructuredEnvelope produces a structured-mode CloudEvents
// JSON object. `data` is inlined for JSON (as raw JSON) and text/*
// (as a string); everything else is base64-encoded into
// `data_base64` per the CE JSON binding.
func buildStructuredEnvelope(e *Entry) map[string]any {
	env := map[string]any{
		"specversion": "1.0",
		"id":          e.CEID,
	}
	for k, v := range e.Attrs {
		// Skip reserved attrs we set explicitly.
		if k == "specversion" || k == "id" {
			continue
		}
		env[k] = v
	}
	switch {
	case len(e.Body) == 0:
		// Empty body — leave data / data_base64 unset entirely so
		// agents can distinguish "no payload" from "empty payload".
	case isJSONContentType(e.ContentType):
		env["data"] = json.RawMessage(e.Body)
	case isTextContentType(e.ContentType):
		env["data"] = string(e.Body)
	default:
		env["data_base64"] = base64.StdEncoding.EncodeToString(e.Body)
	}
	return env
}

// isJSONContentType reports whether ct is application/json or a
// JSON-suffixed type (per RFC 6838 +json suffix).
func isJSONContentType(ct string) bool {
	mt := strings.SplitN(ct, ";", 2)[0]
	mt = strings.ToLower(strings.TrimSpace(mt))
	return mt == "application/json" || strings.HasSuffix(mt, "+json")
}

// isTextContentType reports whether ct is text/* (text/plain, etc.)
// — UTF-8 string-able payloads.
func isTextContentType(ct string) bool {
	mt := strings.SplitN(ct, ";", 2)[0]
	mt = strings.ToLower(strings.TrimSpace(mt))
	return strings.HasPrefix(mt, "text/")
}

// accepts is a Q-value-ignorant Accept-header check. Sufficient for
// our cloudevents+json toggle — caller is the agent inside the
// sandbox, not a browser.
func accepts(r *http.Request, mediaType string) bool {
	for _, h := range r.Header.Values("Accept") {
		for _, part := range strings.Split(h, ",") {
			mt := strings.SplitN(strings.TrimSpace(part), ";", 2)[0]
			if strings.EqualFold(mt, mediaType) {
				return true
			}
		}
	}
	return false
}
