package cmd

import (
	"encoding/json"
	"os"

	"github.com/spf13/cobra"

	"github.com/committeddb/committed/internal/version"
)

var rootCmd = &cobra.Command{
	Use:   "committed",
	Short: "A single-binary, Raft-backed distributed commit log",
	Long: `Committed is a single-binary, Raft-backed distributed commit log that is its
own source of truth: write events in, sync them out to systems built for
querying. It is designed for building distributed CQRS / event-sourced systems.

Start a node with "committed node". Documentation:
https://github.com/committeddb/committed`,
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

// versionString returns the build Info as a JSON one-liner. The same
// shape is served by GET /version so scripts can diff the two without
// worrying about formatting drift.
func versionString() string {
	bs, err := json.Marshal(version.Get())
	if err != nil {
		// Info contains only strings, so Marshal cannot fail in
		// practice — but fall back to a plain Version rather than
		// panicking if it ever does.
		return version.Version
	}
	return string(bs)
}

func init() {
	// Cobra auto-registers --version when Version is non-empty, and
	// handles the flag before Run is invoked (prints + exits 0). The
	// template prints just the version string so the stdout payload
	// is the same JSON shape as GET /version.
	rootCmd.Version = versionString()
	rootCmd.SetVersionTemplate("{{.Version}}\n")
}
