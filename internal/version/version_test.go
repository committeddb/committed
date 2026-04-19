package version_test

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/version"
)

// TestGet verifies Info reflects the current package-level variables
// and that GoVersion is sourced from runtime. An unstamped test
// binary uses the package defaults.
func TestGet(t *testing.T) {
	got := version.Get()

	require.Equal(t, version.Version, got.Version)
	require.Equal(t, version.Commit, got.Commit)
	require.Equal(t, version.BuildDate, got.BuildDate)
	require.Equal(t, runtime.Version(), got.GoVersion)
}
