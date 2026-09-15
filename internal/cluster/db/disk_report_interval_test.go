package db_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db"
)

// The cluster-admission cadence is the other operator switch: a zero
// interval DISABLES the coordinator (the propose gate runs node-local), and
// an unset one takes the default. The option owns the sentinel — any
// non-positive value maps to "off" — so the command passes zero through
// rather than restating the mapping. Pinned at the engine, as the scrub
// interval is, because cmd/node.go used to disable only on the literal "0".
func TestDiskReportInterval_ZeroDisablesTheCoordinator(t *testing.T) {
	t.Run("zero disables", func(t *testing.T) {
		d := newScrubDB(t, db.WithDiskReportInterval(0))
		require.Negative(t, d.DiskReportIntervalForTest(),
			"zero must reach the engine as the off sentinel, not as the default")
	})
	t.Run("unset takes the default", func(t *testing.T) {
		d := newScrubDB(t)
		require.Equal(t, db.DefaultDiskReportInterval, d.DiskReportIntervalForTest())
	})
	t.Run("positive passes through", func(t *testing.T) {
		d := newScrubDB(t, db.WithDiskReportInterval(3*time.Second))
		require.Equal(t, 3*time.Second, d.DiskReportIntervalForTest())
	})
}
