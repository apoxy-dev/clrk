// Package console embeds the clrk web console (a Vite/React SPA) and
// serves it from the controller-manager over a single plain-HTTP origin.
//
// The console is a same-origin SPA: it fetches the Kubernetes API on its
// own window.location.origin. NewHandler therefore serves the embedded
// bundle AND reverse-proxies the API surface (/api, /apis — discovery,
// LIST/WATCH/SSA, the metrics group, and the /traces subresource all ride
// under those two prefixes) to the loopback apiserver. Serving plain HTTP
// avoids the apiserver's self-signed-cert browser wall, and the
// same-origin proxy avoids CORS and a build-time API URL.
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
	"io"
	"io/fs"
	"log/slog"
	"mime"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strings"
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
	// The aggregated API + discovery all live under these two prefixes.
	// Exact ("/api") and subtree ("/api/") patterns both route to the
	// proxy; everything else is served from the embedded bundle.
	mux.Handle("/api", proxy)
	mux.Handle("/api/", proxy)
	mux.Handle("/apis", proxy)
	mux.Handle("/apis/", proxy)
	mux.Handle("/", spa)
	return mux
}

// newAPIProxy builds the reverse proxy to the loopback apiserver. It
// preserves the request path and query verbatim (the apiserver serves the
// same paths the console requests) and streams responses unbuffered so
// Kubernetes watches (chunked NDJSON) flow straight through.
func newAPIProxy(upstream *url.URL) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		FlushInterval: -1,
		Transport: &http.Transport{
			// The upstream is the in-process apiserver on loopback with a
			// self-signed in-memory cert; the cm's own loopback client dials
			// it with Insecure:true for the same reason.
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Director: func(r *http.Request) {
			r.URL.Scheme = upstream.Scheme
			r.URL.Host = upstream.Host
			r.Host = upstream.Host
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			slog.Warn("console: apiserver proxy failed", "upstream", upstream.String(), "err", err)
			http.Error(w, "apiserver unavailable: "+err.Error(), http.StatusBadGateway)
		},
	}
}

// spaHandler serves the embedded bundle with client-side-routing fallback:
//   - an existing file is served with its content-type; hashed assets get a
//     long immutable Cache-Control, index.html is always revalidated.
//   - a path that doesn't exist and looks like a client route (no file
//     extension) returns index.html so the SPA router can handle it.
//   - a path that doesn't exist but looks like an asset (has an extension)
//     returns 404 — never an HTML body — so a stale-deploy bug surfaces
//     instead of being masked.
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
		if path.Ext(name) != "" {
			// Looks like a static asset but isn't in the bundle.
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
	if ct := mime.TypeByExtension(path.Ext(name)); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	// embed.FS files implement io.ReadSeeker, so ServeContent handles
	// range requests and conditional GETs. The embed FS has no real
	// modtimes (zero time), which ServeContent treats as "no Last-Modified".
	if rs, ok := f.(io.ReadSeeker); ok {
		if st, serr := f.Stat(); serr == nil {
			http.ServeContent(w, r, name, st.ModTime(), rs)
			return
		}
	}
	if r.Method == http.MethodHead {
		return
	}
	_, _ = io.Copy(w, f)
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
