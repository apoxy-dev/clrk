package sentry

import (
	"context"
	"time"
)

// AuthState is the current phone-home authorization snapshot, resolved fresh per
// use so a re-registration or an operator toggling the flags takes effect
// without a restart. The token is read from the k8s Secret; the deployment id
// and flags from CLRKConfig.
type AuthState struct {
	DeploymentID  string
	Token         string
	AdviseEnabled bool
	// AdvisoryPollInterval is the server-driven advisory cadence persisted at
	// registration (CLRKConfig.status.notifications). Zero => use the poller's
	// configured default. Surfaced here so the poller adopts the server value
	// without a second CLRKConfig client.
	AdvisoryPollInterval time.Duration
}

// Authorized reports whether the state permits any phone-home call.
func (a AuthState) Authorized() bool { return a.DeploymentID != "" && a.Token != "" }

// AuthFunc resolves the current AuthState (reads CLRKConfig + the token Secret).
// An error means "unknown -- skip this cycle", not fatal.
type AuthFunc func(ctx context.Context) (AuthState, error)
