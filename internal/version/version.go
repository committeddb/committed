// Package version exposes the build-time identity of the running
// binary. Version, Commit, and BuildDate are placeholders overridden
// by `-ldflags "-X ..."` in the Makefile; an unstamped `go build`
// yields the "dev"/"unknown" defaults, which is useful in local
// development but should never appear in a release artifact.
package version

import "runtime"

// These variables are overridden at link time. They are not constants
// so the linker's -X flag can set them.
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

// FeatureLevel is the highest cluster feature level this binary supports. It is
// a monotonic integer — the compatibility axis for *semantic* version skew,
// distinct from the marketing Version string: unlike "an older binary can
// decode the wire", it answers "can this node correctly APPLY a feature's
// entries". Each node self-announces its FeatureLevel (see db.announceVersion),
// and a feature that an older binary would mis-apply gates its emission on the
// cluster-agreed minimum (see db.featureEnabled), so its entries are only ever
// committed once every member can apply them.
//
// Bump this — and add a `featureLevel*` requirement constant at the emitting
// site — whenever you introduce state an older peer cannot correctly apply (a
// new built-in system type, a new semantic marker). NEVER renumber or reuse a
// level; it only ever increases. See docs/api-compatibility.md.
//
// Level 1: the baseline for the first binary carrying this mechanism —
// includes the refresh-boundary marker (featureLevelRefreshBoundary). A binary
// that predates the mechanism announces nothing and is treated as level 0, so
// the gate holds any level-1 emission until every such node is upgraded.
const FeatureLevel uint64 = 1

// Info is the JSON shape returned by /version and printed by the
// --version flag. GoVersion is derived from runtime rather than
// stamped, since the Go toolchain already records it in the binary.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"buildDate"`
	GoVersion string `json:"goVersion"`
}

// Get returns the current build's Info. Intended for use by the HTTP
// handler, the --version flag, and the startup log line so they all
// read from the same source.
func Get() Info {
	return Info{
		Version:   Version,
		Commit:    Commit,
		BuildDate: BuildDate,
		GoVersion: runtime.Version(),
	}
}
