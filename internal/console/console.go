// Package console embeds the clrk web console (a Vite/React SPA) and
// serves it from the controller-manager over a single plain-HTTP origin.
//
// The console is a same-origin SPA: it fetches the Kubernetes API on its
// own window.location.origin. NewHandler therefore serves the embedded
// bundle AND reverse-proxies the API surface (/api, /apis, and
// /console/watch) to the loopback apiserver. Serving plain HTTP avoids the
// apiserver's self-signed-cert browser wall, and the same-origin proxy avoids
// CORS and a build-time API URL. Native Kubernetes watches remain available;
// the console uses /console/watch to multiplex them over one WebSocket.
//
// Auth posture: none. The v1 apiserver runs with authentication disabled
// (--insecure-allow-public), so the proxy needs no credentials; gate the
// console by network reachability exactly like /admin.
//
// The bundle is staged into dist/ by the image build's pnpm step; a
// committed placeholder keeps go:embed/go build green when that step
// hasn't run. PlaceholderOnly reports that state so the controller-manager
// can warn instead of silently serving a stub.
package console

import (
	"bytes"
	"crypto/tls"
	"embed"
	"encoding/json"
	"io"
	"io/fs"
	"log/slog"
	"mime"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strings"
	"time"
)

// distFS holds the built console bundle. all: includes dotfiles (e.g.
// Vite's .vite/ metadata) so nothing the bundler emits is dropped.
//
//go:embed all:dist
var distFS embed.FS

// placeholderMarker is embedded in the committed dist/index.html stub.
// Its presence means the real pnpm build never ran for this binary.
const placeholderMarker = "clrk-console-placeholder"

// PlaceholderOnly reports whether the embedded bundle is the committed
// placeholder rather than a real `pnpm build` output. The controller-manager
// warns on it so a console served from an image built without the pnpm step
// is diagnosable instead of silently broken.
func PlaceholderOnly() bool {
	b, err := distFS.ReadFile("dist/index.html")
	if err != nil {
		return true
	}
	return bytes.Contains(b, []byte(placeholderMarker))
}

// distRoot returns the bundle rooted at dist/ (so request paths map
// directly onto file names).
func distRoot() fs.FS {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		// dist/ is embedded unconditionally (placeholder committed), so a
		// failure here is a build-time packaging bug, not a runtime input.
		panic("console: embedded dist subtree missing: " + err.Error())
	}
	return sub
}

// NewHandler returns the console HTTP handler: the embedded SPA plus a
// reverse proxy of the Kubernetes API to apiUpstream (the loopback
// apiserver). apiUpstream is typically https://127.0.0.1:8443 with a
// self-signed cert, so the proxy transport skips verification.
func NewHandler(apiUpstream *url.URL) http.Handler {
	proxy := newAPIProxy(apiUpstream)
	spa := spaHandler(distRoot())

	mux := http.NewServeMux()
	// The aggregated API + discovery all live under these prefixes. clrk does
	// not serve a Kubernetes core group, so answer exact /api discovery with an
	// empty aggregated-discovery document. This keeps discovery complete without
	// claiming core resources or producing a browser 404. Core resource paths
	// under /api/ still route to the apiserver and return its authoritative result.
	mux.HandleFunc("/api", emptyCoreDiscovery)
	mux.Handle("/api/", proxy)
	mux.Handle("/apis", proxy)
	mux.Handle("/apis/", proxy)
	mux.Handle("/console/watch", proxy)
	mux.Handle("/openapi/", proxy)
	mux.Handle("/version", proxy)
	mux.Handle("/", spa)
	return mux
}

// emptyCoreDiscovery reports that clrk serves no Kubernetes core API group.
func emptyCoreDiscovery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	_ = json.NewEncoder(w).Encode(struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Items      []any  `json:"items"`
	}{
		APIVersion: "apidiscovery.k8s.io/v2",
		Kind:       "APIGroupDiscoveryList",
		Items:      []any{},
	})
}

// newAPIProxy builds the reverse proxy to the loopback apiserver. It
// preserves the request path and query verbatim (the apiserver serves the
// same paths the console requests) and streams responses unbuffered so
// Kubernetes watches (chunked NDJSON) flow straight through. ReverseProxy
// also tunnels WebSocket upgrades for the multiplexed console watcher.
func newAPIProxy(upstream *url.URL) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		FlushInterval: -1,
		Transport: &http.Transport{
			// The upstream is the in-process apiserver on loopback with a
			// self-signed in-memory cert; the cm's own loopback client dials
			// it with Insecure:true for the same reason.
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			// Bound time-to-first-response-header so a stalled apiserver
			// surfaces as a 502 instead of a forever-spinning request. Watches
			// send their 200 promptly and then stream, so this doesn't cut
			// long-lived watch bodies (that would need WriteTimeout, which we
			// deliberately leave unset).
			ResponseHeaderTimeout: 30 * time.Second,
		},
		Director: func(r *http.Request) {
			r.URL.Scheme = upstream.Scheme
			r.URL.Host = upstream.Host
			r.Host = upstream.Host
		},
		ModifyResponse: func(resp *http.Response) error {
			// Rewrite an absolute self-redirect (Location pointing at the
			// loopback apiserver) to same-origin so the browser stays on the
			// console rather than trying to reach 127.0.0.1:<port> directly.
			if loc := resp.Header.Get("Location"); loc != "" {
				if u, err := url.Parse(loc); err == nil && u.Host == upstream.Host {
					u.Scheme, u.Host = "", ""
					resp.Header.Set("Location", u.String())
				}
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			slog.Warn("Console apiserver proxy failed", "upstream", upstream.String(), "err", err)
			http.Error(w, "apiserver unavailable: "+err.Error(), http.StatusBadGateway)
		},
	}
}

// spaHandler serves the embedded bundle with client-side-routing fallback:
//   - an existing file is served with its content-type; hashed assets get a
//     long immutable Cache-Control, index.html is always revalidated.
//   - a missing path under the hashed-asset dir (assets/) returns 404 — never
//     an HTML body — so a stale-deploy bug surfaces instead of being masked.
//   - any other missing path is treated as a client-side route and returns
//     index.html. Keying the 404 on the assets/ prefix (not "the path has a
//     dot") means client routes that contain dots — e.g. a resource named
//     with a dot — still resolve to the SPA shell instead of 404ing.
func spaHandler(root fs.FS) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		if name == "" {
			serveIndex(w, r, root)
			return
		}
		if f, err := root.Open(name); err == nil {
			defer f.Close()
			if st, serr := f.Stat(); serr == nil && !st.IsDir() {
				serveAsset(w, r, name, f)
				return
			}
		}
		if strings.HasPrefix(name, "assets/") {
			// A hashed asset that isn't in the bundle: a real 404, not the SPA.
			http.NotFound(w, r)
			return
		}
		serveIndex(w, r, root)
	}
}

// serveAsset writes an embedded file with the right content-type and a
// cache policy keyed on whether the name is a content-hashed asset.
func serveAsset(w http.ResponseWriter, r *http.Request, name string, f fs.File) {
	if strings.HasPrefix(name, "assets/") {
		// Vite content-hashes asset filenames, so they're safe to cache hard.
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "no-cache")
	}
	if ct := contentType(name); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	// embed.FS files always implement io.ReadSeeker and Stat never errors, so
	// ServeContent (range requests + conditional GETs) is the only path. The
	// embed FS has no real modtimes (zero time), which ServeContent treats as
	// "no Last-Modified".
	rs, ok := f.(io.ReadSeeker)
	if !ok {
		http.Error(w, "console asset not seekable", http.StatusInternalServerError)
		return
	}
	st, err := f.Stat()
	if err != nil {
		http.Error(w, "console asset stat failed", http.StatusInternalServerError)
		return
	}
	http.ServeContent(w, r, name, st.ModTime(), rs)
}

// contentType resolves a response Content-Type for the bundle's file types.
// It pins the web-critical types explicitly so a base image that ships an
// /etc/mime.types mapping (e.g. .js -> text/plain) can't make the browser
// reject the console's ES modules; it falls back to mime.TypeByExtension for
// anything else.
func contentType(name string) string {
	switch path.Ext(name) {
	case ".html":
		return "text/html; charset=utf-8"
	case ".js", ".mjs":
		return "text/javascript; charset=utf-8"
	case ".css":
		return "text/css; charset=utf-8"
	case ".json", ".map":
		return "application/json"
	case ".svg":
		return "image/svg+xml"
	case ".woff2":
		return "font/woff2"
	case ".woff":
		return "font/woff"
	}
	return mime.TypeByExtension(path.Ext(name))
}

// serveIndex writes index.html as the SPA entrypoint / client-route
// fallback. It's always revalidated so a new deploy's hashed-asset
// references are picked up immediately.
func serveIndex(w http.ResponseWriter, r *http.Request, root fs.FS) {
	b, err := fs.ReadFile(root, "index.html")
	if err != nil {
		http.Error(w, "console bundle missing index.html", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, _ = w.Write(b)
}
