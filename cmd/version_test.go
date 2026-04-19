package cmd

import (
	"bytes"
	"encoding/json"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/version"
)

// TestVersionFlag verifies `committed --version` exits 0 and prints
// the build Info as JSON. Cobra handles the flag before Run, so this
// doesn't trigger node startup — we're only asserting the flag wiring
// in cmd/root.go. Output is routed through SetOut, not os.Stdout, so
// the test can capture it without touching file descriptors.
func TestVersionFlag(t *testing.T) {
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetArgs([]string{"--version"})

	err := rootCmd.Execute()
	require.Nil(t, err)

	var got version.Info
	require.Nil(t, json.Unmarshal(bytes.TrimSpace(out.Bytes()), &got))

	require.Equal(t, version.Version, got.Version)
	require.Equal(t, version.Commit, got.Commit)
	require.Equal(t, version.BuildDate, got.BuildDate)
	require.Equal(t, runtime.Version(), got.GoVersion)
}
