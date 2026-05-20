package egress

import (
	"errors"
	"strings"
	"syscall"
)

// IsBenignClose distinguishes routine half-shutdown noise (ECONNRESET
// after the peer finished writing, EPIPE in the reverse direction)
// from real splice failures. Both the Sentry-side TCP forwarder and
// the worker-side egress bridge consult it before deciding whether
// to log a peer-close at warn level.
func IsBenignClose(err error) bool {
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe")
}
