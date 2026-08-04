package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/wal"
	"go.uber.org/zap/zapcore"

	"github.com/committeddb/committed/internal/cluster"
)

// TestMisTiledSegmentReadDoesNotFatal pins the structural flavor of the
// corrupt-read policy: a mis-tiled segment (the forked wal's ErrCorrupt — a
// hollow or truncated segment file) must not fatal-exit the node, and must
// take the generic warn-and-retry rather than the CRC branch's Error-level
// repair guidance — the two corruption flavors carry different diagnoses
// (damaged segment FILE vs damaged entry BYTES) and must stay distinguishable
// in the logs. Fataling on the first cold historical read was the post-scrub
// hollow-segment boot crashloop with no API window; the wedge keeps the node
// up while the reader holds position.
func TestMisTiledSegmentReadDoesNotFatal(t *testing.T) {
	d, logs := observedDB()

	// Reader.Read wraps the storage error with %w; the policy must hold
	// through the wrap.
	err := fmt.Errorf("event log read seq %d: %w", 25096, wal.ErrCorrupt)
	require.NotPanics(t, func() { d.logSyncReadError("movie-sync", err) })

	require.Empty(t, logs.FilterLevelExact(zapcore.ErrorLevel).All(),
		"structural corruption must not take the CRC branch")
	warns := logs.FilterLevelExact(zapcore.WarnLevel).All()
	require.Len(t, warns, 1)
	require.Equal(t, "sync read error", warns[0].Message)

	// Sanity: the two corruption sentinels remain distinct types.
	if fmt.Sprintf("%v", cluster.ErrCorruptEntry) == fmt.Sprintf("%v", wal.ErrCorrupt) {
		t.Fatal("the CRC sentinel and the fork's structural-corruption sentinel must stay distinguishable")
	}
}
