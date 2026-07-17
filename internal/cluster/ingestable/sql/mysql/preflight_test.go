package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGtidPreflightWarning covers the advisory classifier: a fully GTID-enabled
// source warns about nothing; gtid_mode off (any non-ON value) warns about the
// file:pos fallback; enforce off while gtid_mode on warns about consistency.
func TestGtidPreflightWarning(t *testing.T) {
	require.Empty(t, gtidPreflightWarning("ON", "ON"))
	require.Empty(t, gtidPreflightWarning("on", "on")) // case-insensitive

	require.Contains(t, gtidPreflightWarning("OFF", "OFF"), "file:position")
	require.Contains(t, gtidPreflightWarning("OFF_PERMISSIVE", "ON"), "file:position")

	// gtid_mode ON but enforce not ON → the consistency warning, not the fallback one.
	w := gtidPreflightWarning("ON", "WARN")
	require.NotEmpty(t, w)
	require.Contains(t, w, "enforce_gtid_consistency")
	require.NotContains(t, w, "file:position")
}

// TestUnsupportedColumnTypes pins the spatial/VECTOR set committed rejects rather
// than silently corrupt (these have no lossless form on the binary CDC/snapshot
// paths). Asserted here so VECTOR is covered without a MySQL 9.0 container.
func TestUnsupportedColumnTypes(t *testing.T) {
	for _, ty := range []string{
		"geometry", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon", "geometrycollection",
		"vector",
	} {
		require.Truef(t, unsupportedColumnTypes[ty], "%q should be unsupported", ty)
	}
	for _, ty := range []string{"int", "bigint", "varchar", "text", "blob", "json", "decimal", "datetime", "enum"} {
		require.Falsef(t, unsupportedColumnTypes[ty], "%q should be supported", ty)
	}
}

// TestBinlogRetentionWarning covers the no-hold retention heuristic: 0 (never
// auto-purged) is safest and silent, a long retention is silent, and a short
// positive retention warns.
func TestBinlogRetentionWarning(t *testing.T) {
	require.Empty(t, binlogRetentionWarning(0))       // never auto-purged
	require.Empty(t, binlogRetentionWarning(-1))      // defensive: non-positive
	require.Empty(t, binlogRetentionWarning(2592000)) // 30 days — ample

	w := binlogRetentionWarning(600) // 10 minutes — risky
	require.NotEmpty(t, w)
	require.Contains(t, w, "re-snapshot")

	// Exactly at the floor is not below it (no warning); just under it warns.
	require.Empty(t, binlogRetentionWarning(int64(minSafeBinlogRetention.Seconds())))
	require.NotEmpty(t, binlogRetentionWarning(int64(minSafeBinlogRetention.Seconds())-1))
}
