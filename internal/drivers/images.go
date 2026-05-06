package drivers

import (
	"runtime/debug"
	"strings"
)

// Public-GAR repo paths where every clrk main commit lands. The
// .github/workflows/clrk.yml workflow in apoxy-cloud (triggered by a
// repository_dispatch from this repo) builds the bazel OCI tarballs
// and pushes them with the first 8 hex chars of the clrk SHA as the
// tag. ImageTag() below mirrors that derivation so a `clrk` binary
// built from a given SHA defaults to the matching pushed image.
const (
	ClrkControllerManagerImagePath = "us-west1-docker.pkg.dev/apoxy-dev/public/clrk-controller-manager"
	ClrkWorkerImagePath            = "us-west1-docker.pkg.dev/apoxy-dev/public/clrk-worker"

	// BazelLocalControllerManagerTag is the tag the bazel OCI tarball
	// at //clrk:controller-manager_oci_tarball stamps via its
	// `repo_tags` attribute. `clrk dev --reload-tar=controller-manager=…`
	// docker-loads a tarball with this tag, so the dev path overrides
	// the GAR default with this value.
	BazelLocalControllerManagerTag = "clrk/controller-manager:latest"
	// BazelLocalWorkerTag is the matching value for the worker tarball.
	BazelLocalWorkerTag = "clrk/worker:latest"
)

// ImageTag returns the GAR tag matching this clrk binary, derived
// from the binary's embedded VCS info. Two cases:
//
//  1. Local build (`go install ./cmd/clrk` from a clean checkout):
//     debug.BuildInfo.Settings carries `vcs.revision` = full SHA. We
//     truncate to 8 hex chars to match the publish workflow.
//  2. Remote install (`go install github.com/apoxy-dev/clrk/cmd/clrk@<rev>`):
//     debug.BuildInfo.Main.Version is a Go pseudo-version of the form
//     `v0.0.0-<timestamp>-<sha-12>`. We pull the last 12 hex chars
//     and truncate to 8.
//
// Falls back to "latest" if neither is reachable — the publish
// workflow doesn't push a `:latest` tag, so that fallback surfaces a
// clear `manifest unknown` from `docker pull` instead of silently
// running stale code.
func ImageTag() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "latest"
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" && len(s.Value) >= 8 {
			return s.Value[:8]
		}
	}
	if v := info.Main.Version; strings.HasPrefix(v, "v0.0.0-") {
		if i := strings.LastIndex(v, "-"); i >= 0 && i+9 <= len(v) {
			return v[i+1 : i+9]
		}
	}
	return "latest"
}

// Default image refs are package vars (not consts) because the tag is
// derived from the binary's embedded VCS info at process start. Cobra
// reads these as flag defaults; the drivers fall back to them when no
// override is supplied.
var (
	DefaultControllerManagerImage = ClrkControllerManagerImagePath + ":" + ImageTag()
	DefaultWorkerImage            = ClrkWorkerImagePath + ":" + ImageTag()
)
