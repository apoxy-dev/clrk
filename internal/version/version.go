// Package version exposes the clrk binary's own build version. It is the single
// source of truth for "what version am I", consumed by the installer's upgrade
// gate (the target of a `clrk upgrade`) and stamped onto the control plane.
package version

// version is injected at link time:
//
//	-ldflags "-X github.com/apoxy-dev/clrk/internal/version.version=v1.2.3"
//
// It is empty for binaries built without that flag (a plain `go build`, dev
// builds). An orderable semver here is what lets GateUpgrade compare an
// upgrade's target against the installed annotation; an empty or non-semver
// value degrades the gate to equal/not-equal + --force.
var version = ""

// Current returns the injected build version, or "" if none was injected.
func Current() string { return version }
