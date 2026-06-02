package extproc

import "hash/fnv"

// selectorInput carries the request facts a backend selector may key on.
// The static path uses only invocationID/model/path (deterministic
// weighting); the classifier path (APO-480) will also read the body and
// headers, which is why they are threaded here now.
type selectorInput struct {
	provider     string
	model        string
	path         string
	invocationID string
	reqHeaders   map[string]string
	reqBody      []byte
}

// backendSelector picks one backend from a rule's candidate set at
// RequestBody end-of-stream. staticSelector implements the no-classifier
// path (weighted by Gateway API BackendRef.Weight); the ExtensionRef
// classifier path is APO-480 and will be a separate implementation of
// this interface, swapped in at the call site.
type backendSelector interface {
	Select(candidates []resolvedBackend, in selectorInput) (resolvedBackend, bool)
}

// staticSelector picks by relative BackendRef.Weight. With one candidate
// (or all weights equal) it returns the first; otherwise it draws
// deterministically from the cumulative weight distribution keyed off the
// invocation id (falling back to model+path), so retries of the same
// logical request always land on the same backend — required for stable
// budget attribution and idempotency.
type staticSelector struct{}

func (staticSelector) Select(candidates []resolvedBackend, in selectorInput) (resolvedBackend, bool) {
	switch len(candidates) {
	case 0:
		return resolvedBackend{}, false
	case 1:
		return candidates[0], true
	}

	total := 0
	for _, c := range candidates {
		if c.weight > 0 {
			total += c.weight
		}
	}
	if total == 0 {
		// All weights zero — no defined split; fall back to the first.
		return candidates[0], true
	}

	key := in.invocationID
	if key == "" {
		key = in.model + "\x00" + in.path
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	pick := int(h.Sum32() % uint32(total))
	for _, c := range candidates {
		if c.weight <= 0 {
			continue
		}
		if pick < c.weight {
			return c, true
		}
		pick -= c.weight
	}
	return candidates[0], true
}
