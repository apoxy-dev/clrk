package apiserver

import (
	"net/http"
	"strconv"

	apirequest "k8s.io/apiserver/pkg/endpoints/request"
)

// IsTelemetryFollowRequest reports whether r is a streaming follow read of
// the {logs,traces} read subresources (taskagents|daemonagents/{name}/
// {logs,traces}?follow=true). Such a request streams for as long as the
// client watches -- serveFollow polls ClickHouse on an interval and
// writes NDJSON chunks -- so it must be exempted from the apiserver's
// non-long-running request timeout. Without the exemption that timeout
// (60s by default) fires mid-stream, after the 200 and headers are
// already flushed, and aborts the handler with http.ErrAbortHandler,
// resetting the HTTP/2 stream (the client sees "stream error ...
// INTERNAL_ERROR").
//
// A non-follow GET of the same subresource is a one-shot paged query and
// is deliberately NOT long-running, so it keeps the request timeout as a
// safety bound. The check mirrors telemetry.parseFilters' ParseBool of
// the follow param.
func IsTelemetryFollowRequest(r *http.Request, ri *apirequest.RequestInfo) bool {
	if ri == nil || !ri.IsResourceRequest {
		return false
	}
	if ri.Subresource != "logs" && ri.Subresource != "traces" {
		return false
	}
	follow, _ := strconv.ParseBool(r.URL.Query().Get("follow"))
	return follow
}
