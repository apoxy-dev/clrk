package otelemit

import "net/url"

// EndpointForSignal appends the signal-specific path when the
// configured endpoint has no path or only the root `/`. Required
// because otlploghttp/otlptracehttp WithEndpointURL uses the URL's
// Path verbatim — a bare `http://host:4318` ends up POSTing to `/`,
// which collectors return 404 for.
func EndpointForSignal(endpoint, signalPath string) string {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return endpoint
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = signalPath
		return u.String()
	}
	return endpoint
}
