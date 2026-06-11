package extproc

import "hash/fnv"

// weightedPick draws one backend from candidates by relative
// BackendRef.Weight, deterministically keyed (so retries of the same
// logical request land on the same backend — stable budget attribution
// and idempotency). With one candidate (or all weights zero) it
// returns the first.
//
// Envoy's load balancer owns selection over a rule's FULL candidate
// set now (the synthesized cluster carries the weights); this helper
// survives only for the partial-viable-set case, where per-request
// gates (translatability, model-rewrite coverage) shrink the set below
// the cluster's membership and the downstream must pin ONE servable
// backend via the envoy.lb subset key.
func weightedPick(candidates []resolvedBackend, key string) (resolvedBackend, bool) {
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
