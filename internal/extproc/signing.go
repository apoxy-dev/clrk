package extproc

import (
	"slices"
	"strings"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/go-logr/logr"

	"github.com/apoxy-dev/clrk/internal/extproc/awssigv4"
)

// signInput carries the request facts a SigV4 signature covers. All
// values must be the FINAL wire values — signing is always the last
// header mutation decided for a request, after every body rewrite and
// path/authority repoint.
type signInput struct {
	// method is the request :method.
	method string
	// authority is the final :authority (host[:port]) exactly as it
	// will leave Envoy — the signed `host` header must byte-match what
	// the upstream receives.
	authority string
	// path is the final :path, query included.
	path string
	// body is the final byte-exact payload.
	body []byte
	// headers is the agent's request header map (lowercased), used to
	// shed unsigned x-amz-* headers.
	headers map[string]string
}

// splitInjections partitions a credential lookup result into static
// header injections and SigV4 signing injections.
func splitInjections(injs []credInjection) (hdrs, sigs []credInjection) {
	for _, inj := range injs {
		if inj.sigv4 != nil {
			sigs = append(sigs, inj)
		} else {
			hdrs = append(hdrs, inj)
		}
	}
	return hdrs, sigs
}

// applySigning signs the request for the first AWSv4 injection in sigs
// and appends the signature headers as OVERWRITE SetHeaders, plus
// RemoveHeaders shedding every agent-supplied x-amz-* header the
// signature does not itself set — AWS rejects requests carrying
// x-amz-* headers absent from SignedHeaders, and agents running AWS
// SDKs routinely send them (x-amz-user-agent, a stale self-signed
// x-amz-date). Multiple AWSv4 policies matching one request are a
// configuration conflict: the first wins, the rest are logged. No-op
// when sigs is empty.
func applySigning(existing *extprocv3.HeaderMutation, sigs []credInjection, in signInput, log logr.Logger) *extprocv3.HeaderMutation {
	if len(sigs) == 0 {
		return existing
	}
	sig := sigs[0]
	if len(sigs) > 1 {
		skipped := make([]string, 0, len(sigs)-1)
		for _, s := range sigs[1:] {
			skipped = append(skipped, s.policyName)
		}
		log.Info("Multiple AWSv4 credential policies matched one request; first wins",
			"applied", sig.policyName, "skipped", skipped)
	}
	region := sig.sigv4.region
	if region == "" {
		region = awssigv4.RegionFromHost(in.authority)
	}
	if region == "" {
		log.Info("AWSv4 credential policy has no region and none derivable from the target host; skipping signing",
			"policy", sig.policyName, "host", in.authority)
		return existing
	}
	set := map[string]bool{}
	for _, h := range awssigv4.Sign(in.method, in.authority, in.path, in.body, sig.sigv4.creds, region, sig.sigv4.service, time.Now()) {
		existing = setHeaderMut(existing, h.Name, h.Value)
		set[h.Name] = true
	}
	var shed []string
	for name := range in.headers {
		if strings.HasPrefix(name, "x-amz-") && !set[name] {
			shed = append(shed, name)
		}
	}
	slices.Sort(shed)
	return removeHeadersMut(existing, shed)
}
