package metadata

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

const contentTypeCloudEventsJSON = "application/cloudevents+json"

// NewHandler returns the IMDS HTTP routes wired against entry. The
// linux-only Server in server.go uses this to assemble its
// gVisor-bound http.Server; tests can use it directly with
// httptest.NewServer.
func NewHandler(entry *Entry) http.Handler {
	h := &handler{entry: entry}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/event", h.handleEvent)
	mux.HandleFunc("/v1/response", h.handleResponse)
	return mux
}

type handler struct {
	entry *Entry
}

// handleEvent serves the request envelope. Binary mode by default
// (ce-* response headers + raw body). Structured mode when the
// caller advertises Accept: application/cloudevents+json.
func (h *handler) handleEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}

	if accepts(r, contentTypeCloudEventsJSON) {
		env := buildStructuredEnvelope(h.entry)
		w.Header().Set("Content-Type", contentTypeCloudEventsJSON)
		_ = json.NewEncoder(w).Encode(env)
		return
	}

	for k, v := range h.entry.Attrs {
		w.Header().Set("ce-"+k, v)
	}
	w.Header().Set("ce-id", h.entry.CEID)
	if h.entry.ContentType != "" {
		w.Header().Set("Content-Type", h.entry.ContentType)
	}
	_, _ = w.Write(h.entry.Body)
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
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if !h.entry.SetResponse(body, r.Header.Get("Content-Type")) {
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
