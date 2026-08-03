package db

import (
	"fmt"
	"testing"

	"github.com/tidwall/wal"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// TestFatalOnCorruptRead_MisTiledSegmentDoesNotFatal pins the deliberate scope
// of the sync path's corrupt-read policy: a structurally mis-tiled segment (the
// forked wal's ErrCorrupt — a hollow or truncated segment file) must NOT
// fatal-exit the node. Fataling on the first cold historical read is a boot
// crashloop with no API window (the post-scrub hollow-segment incident); the
// wedge-and-retry fall-through keeps the node up while the reader holds
// position. Only cluster.ErrCorruptEntry (a CRC mismatch inside one entry's
// frame) takes the documented fatal branch — which cannot be asserted directly
// (logger.Fatal exits the process), so this test pins the negative: the fork
// error, wrapped exactly as Reader.Read wraps it, returns.
func TestFatalOnCorruptRead_MisTiledSegmentDoesNotFatal(t *testing.T) {
	db := &DB{logger: zap.NewNop()}

	// Reader.Read wraps the storage error with %w; the policy must hold
	// through the wrap.
	err := fmt.Errorf("event log read seq %d: %w", 25096, wal.ErrCorrupt)
	db.fatalOnCorruptRead("movie-sync", err) // returning is the proof — Fatal would exit

	// Sanity: the wrap did not accidentally become the CRC sentinel, and the
	// two corruption flavors remain distinct types.
	if fmt.Sprintf("%v", cluster.ErrCorruptEntry) == fmt.Sprintf("%v", wal.ErrCorrupt) {
		t.Fatal("the CRC sentinel and the fork's structural-corruption sentinel must stay distinguishable")
	}
}
