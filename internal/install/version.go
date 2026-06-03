package install

import (
	"fmt"
	"strings"

	"golang.org/x/mod/semver"
)

// UpgradeVerdict is GateUpgrade's top-level decision.
type UpgradeVerdict int

const (
	// VerdictProceed: apply under the normal plan + single confirm gate; no
	// extra acknowledgement is required.
	VerdictProceed UpgradeVerdict = iota
	// VerdictConfirm: proceed only after an explicit acknowledgement beyond the
	// plan confirm (a major-version jump, or an install whose version can't be
	// read/ordered). The caller auto-accepts this under --yes/--force.
	VerdictConfirm
	// VerdictRefuse: do not proceed (a downgrade without --allow-downgrade, or a
	// non-orderable version change without --force).
	VerdictRefuse
)

func (v UpgradeVerdict) String() string {
	switch v {
	case VerdictProceed:
		return "proceed"
	case VerdictConfirm:
		return "confirm"
	default:
		return "refuse"
	}
}

// UpgradeDecision is GateUpgrade's verdict plus an operator-facing reason.
type UpgradeDecision struct {
	Verdict UpgradeVerdict
	Reason  string
}

// GateUpgrade decides whether an in-place upgrade from the installed version to
// the target version may proceed. `installed` is the version stamped on the
// existing control plane (DetectInstall; may be "" when an older install never
// recorded one); `target` is the version being applied (the upgrading binary's
// version.Current(), or an explicit --version).
//
// When both are valid semver they are ordered with golang.org/x/mod/semver:
// equal is a repair re-apply, newer proceeds (a major jump asks for an extra
// confirm), older is refused unless --allow-downgrade. When either side isn't
// orderable semver the gate degrades to equal/not-equal and any change requires
// --force. --force overrides every refusal and confirm.
func GateUpgrade(installed, target string, allowDowngrade, force bool) UpgradeDecision {
	iv, tv := canonical(installed), canonical(target)

	// An install with no recorded version can't be compatibility-checked.
	if installed == "" {
		return forceable(force, UpgradeDecision{
			Verdict: VerdictConfirm,
			Reason: fmt.Sprintf(
				"the existing install has no recorded version; upgrading to %s cannot be compatibility-checked",
				show(target)),
		})
	}

	bothSemver := semver.IsValid(iv) && semver.IsValid(tv)
	if !bothSemver {
		if installed == target {
			return UpgradeDecision{Verdict: VerdictProceed,
				Reason: fmt.Sprintf("re-applying %s (repair / reconcile drift)", show(target))}
		}
		return forceable(force, UpgradeDecision{
			Verdict: VerdictRefuse,
			Reason: fmt.Sprintf(
				"cannot order non-semver versions %s -> %s; pass --force to change anyway",
				show(installed), show(target)),
		})
	}

	switch cmp := semver.Compare(iv, tv); {
	case cmp == 0:
		return UpgradeDecision{Verdict: VerdictProceed,
			Reason: fmt.Sprintf("re-applying %s (repair / reconcile drift, no version change)", iv)}
	case cmp < 0: // target newer
		majorJump := semver.Major(iv) != semver.Major(tv)
		// Pre-1.0, semver permits breaking changes on every minor bump, and
		// semver.Major collapses all 0.x to "v0" — so a v0.2 -> v0.3 jump must be
		// treated as potentially breaking too, which is exactly the population most
		// likely running during early clrk releases.
		zeroMinorJump := semver.Major(iv) == "v0" && semver.Major(tv) == "v0" &&
			semver.MajorMinor(iv) != semver.MajorMinor(tv)
		if majorJump || zeroMinorJump {
			reason := fmt.Sprintf(
				"major-version upgrade %s -> %s may carry breaking changes; the CRD bundle is force-applied before the controller-manager rolls",
				iv, tv)
			if zeroMinorJump && !majorJump {
				reason = fmt.Sprintf(
					"pre-1.0 minor upgrade %s -> %s may carry breaking changes (semver allows breaking 0.x minors); the CRD bundle is force-applied before the controller-manager rolls",
					iv, tv)
			}
			return forceable(force, UpgradeDecision{Verdict: VerdictConfirm, Reason: reason})
		}
		return UpgradeDecision{Verdict: VerdictProceed,
			Reason: fmt.Sprintf("upgrade %s -> %s", iv, tv)}
	default: // target older -> downgrade
		if allowDowngrade || force {
			return UpgradeDecision{Verdict: VerdictProceed,
				Reason: fmt.Sprintf("downgrade %s -> %s (operator-acknowledged)", iv, tv)}
		}
		return UpgradeDecision{Verdict: VerdictRefuse,
			Reason: fmt.Sprintf("refusing downgrade %s -> %s; pass --allow-downgrade to proceed", iv, tv)}
	}
}

// forceable collapses a refusal/confirm to a proceed when --force is set,
// annotating the reason so the override is legible in the output.
func forceable(force bool, d UpgradeDecision) UpgradeDecision {
	if force && d.Verdict != VerdictProceed {
		return UpgradeDecision{Verdict: VerdictProceed, Reason: "forced: " + d.Reason}
	}
	return d
}

// canonical returns the semver-canonical form of a version string (adding the
// leading "v" semver requires), or "" if it isn't valid semver.
func canonical(s string) string {
	if s == "" {
		return ""
	}
	if !strings.HasPrefix(s, "v") {
		s = "v" + s
	}
	return semver.Canonical(s)
}

// show renders a version for operator-facing messages: the raw string, or
// "(unset)" for an empty one.
func show(s string) string {
	if s == "" {
		return "(unset)"
	}
	return s
}
