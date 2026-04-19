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
