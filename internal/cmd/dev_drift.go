package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	k3dclient "github.com/k3d-io/k3d/v5/pkg/client"
	"github.com/k3d-io/k3d/v5/pkg/runtimes"
	k3d "github.com/k3d-io/k3d/v5/pkg/types"

	"github.com/apoxy-dev/clrk/internal/drivers"
)

// computeConfigHash hashes the dev invocation's cluster-shaping inputs
// — the things that, if changed, demand a new cluster rather than an
// attach. Resolved after applyRegistryImageOverrides so registry-mode
// rewrites count. Excludes runtime-only inputs (--apply, --secret) and
// the host/port we'd allocate dynamically (registryPort=0).
//
// Truncated to 12 hex chars: enough collision resistance for a single
// host's dev sessions, short enough to fit in the diff message.
func computeConfigHash(o *devOpts) string {
	return hashShape(configShape{
		controllerImage: o.controllerImage,
		workerImage:     o.workerImage,
		k3sImage:        o.k3sImage,
		workers:         o.workers,
		pull:            o.pull,
		registry:        o.registryEnabled(),
		registryPort:    o.registryPort,
	})
}

// configShape is the projection of devOpts (live invocation) or
// devSession (on-disk marker) that computeConfigHash actually hashes.
// Keeping it as a separate struct lets us re-derive a hash from an
// older dev.json that predates the ConfigHash field — used by
// classifyDrift to attach silently across binary upgrades when the
// stored shape happens to match the current invocation.
type configShape struct {
	controllerImage string
	workerImage     string
	k3sImage        string
	workers         int
	pull            string
	registry        bool
	registryPort    int
}

func hashShape(s configShape) string {
	k3sImg := s.k3sImage
	if k3sImg == "" {
		k3sImg = drivers.DefaultK3sImage
	}
	pull := s.pull
	if pull == "" {
		pull = "missing"
	}
	parts := []string{
		"controller=" + s.controllerImage,
		"worker=" + s.workerImage,
		"k3s=" + k3sImg,
		"workers=" + strconv.Itoa(s.workers),
		"pull=" + pull,
		"registry=" + strconv.FormatBool(s.registry),
	}
	if s.registryPort != 0 {
		parts = append(parts, "registryPort="+strconv.Itoa(s.registryPort))
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])[:12]
}

// driftState captures everything runDev needs to decide whether to
// attach or recreate. clusterExists is set when k3d reports a cluster
// with our name; prior is the session marker the previous invocation
// stamped (nil when missing). want is the current invocation's hash.
type driftState struct {
	clusterExists bool
	prior         *devSession
	want          string
}

// inspectDrift probes the on-disk session marker and the docker side
// (does a k3d cluster with our name exist?). The combination — not
// either alone — drives the gate: a missing cluster means a clean
// start regardless of stale marker, and a missing marker against an
// existing cluster always counts as drift because we don't know what
// shape the orphan was built for.
func inspectDrift(ctx context.Context, o *devOpts) (driftState, error) {
	ds := driftState{want: o.cfgHash}
	rt := runtimes.SelectedRuntime
	if cl, err := k3dclient.ClusterGet(ctx, rt, &k3d.Cluster{Name: drivers.ClusterName}); err == nil && cl != nil {
		ds.clusterExists = true
	}
	if prior, err := readDevSession(o.dataDir); err == nil {
		ds.prior = prior
	} else if !errors.Is(err, errNoSession) {
		return ds, fmt.Errorf("reading prior dev session: %w", err)
	}
	return ds, nil
}

// driftDecision classifies what runDev should do given inspectDrift.
type driftDecision int

const (
	decisionAttach driftDecision = iota
	decisionFreshStart
	decisionDrift
)

// classifyDrift maps a (clusterExists, prior, want) tuple to a decision.
// Split from inspectDrift so it's pure and testable by inspection.
//
// Upgrade path: a dev.json written by a binary that predates ConfigHash
// has the field empty but still carries the shape fields. Re-derive a
// hash from those fields and treat it as a match if it lines up — that
// way users don't get a one-time drift prompt the first time they run
// the new binary against an existing healthy session.
func classifyDrift(ds driftState) (driftDecision, string) {
	if !ds.clusterExists {
		return decisionFreshStart, ""
	}
	if ds.prior == nil {
		return decisionDrift, "no session marker on disk (cluster left over from a session that died before stamping)"
	}
	priorHash := ds.prior.ConfigHash
	if priorHash == "" {
		priorHash = hashShape(configShape{
			controllerImage: ds.prior.ControllerImage,
			workerImage:     ds.prior.WorkerImage,
			k3sImage:        ds.prior.K3sImage,
			workers:         ds.prior.Workers,
			pull:            ds.prior.Pull,
			registry:        ds.prior.RegistryEnabled,
			// Stored RegistryHostPort is the *bound* port, not the
			// --registry-port flag value, so leave it zero — the
			// invocation hash also drops the dynamically-allocated case.
		})
	}
	if priorHash == ds.want {
		return decisionAttach, ""
	}
	return decisionDrift, driftSummary(ds.prior, ds.want)
}

// driftSummary renders a short, human-readable diff of the fields
// computeConfigHash hashes. Only includes lines that actually changed.
func driftSummary(prior *devSession, wantHash string) string {
	var lines []string
	lines = append(lines, fmt.Sprintf("config hash: %s → %s", prior.ConfigHash, wantHash))
	if prior.StartedAt.IsZero() {
		lines = append(lines, "prior session: (no start time recorded)")
	} else {
		lines = append(lines, "prior session started: "+prior.StartedAt.Local().Format("2006-01-02 15:04:05 MST"))
	}
	return strings.Join(lines, "\n  ")
}

// resolveDrift implements the user-facing drift gate. Returns true when
// runDev should proceed (either no drift, or the user authorized the
// recreate); false means runDev should exit non-zero. On a confirmed
// recreate, the cluster is fully torn down before returning so the
// subsequent ClusterDriver.Start hits a clean slate.
func resolveDrift(ctx context.Context, o *devOpts, ds driftState) (bool, error) {
	decision, reason := classifyDrift(ds)
	switch decision {
	case decisionFreshStart, decisionAttach:
		return true, nil
	}

	if o.forceRecreate {
		slog.Warn("Cluster config drift — tearing down per --force-recreate", "reason", reason)
		return true, tearDownForRecreate(ctx, o)
	}

	fmt.Fprintf(os.Stderr,
		"Existing dev cluster doesn't match the requested config:\n  %s\n",
		reason,
	)
	ok, err := confirm(ctx, "Recreate cluster (destroys all dev state)? [y/N]: ")
	if err != nil {
		if errors.Is(err, errNoTTY) {
			fmt.Fprintln(os.Stderr, hintForceRecreate)
			return false, nil
		}
		return false, err
	}
	if !ok {
		fmt.Fprintln(os.Stderr, hintForceRecreate)
		return false, nil
	}
	return true, tearDownForRecreate(ctx, o)
}

// hintForceRecreate is the one-liner we print when the user declines
// (or can't be prompted). Same text either way so users learn the same
// recovery path regardless of which branch they hit.
const hintForceRecreate = "Re-run with --force-recreate to drop dev state and rebuild, or pass flags that match the existing session."

// tearDownForRecreate removes the orphaned cluster + registry + the
// shared docker network. Done synchronously so the next
// drivers.EnsureNetwork hits a clean slate; the stale stamp is removed
// too so a failure between teardown and the next stamp doesn't leave a
// confusing marker.
func tearDownForRecreate(ctx context.Context, o *devOpts) error {
	cluster := drivers.NewClusterDriver(o.dataDir, o.k3sImage, o.registryPort)
	if err := cluster.Reset(ctx); err != nil {
		return fmt.Errorf("resetting cluster for recreate: %w", err)
	}
	_ = os.Remove(filepath.Join(o.dataDir, devSessionFileName))
	return nil
}
