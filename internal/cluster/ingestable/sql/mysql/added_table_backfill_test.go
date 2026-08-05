package mysql

import (
	"testing"

	"go.uber.org/zap/zapcore"

	"github.com/stretchr/testify/require"
)

// addedTables drives the added-table backfill: configured-but-never-
// snapshotted tables, in config order (deterministic scan order).
func TestAddedTables(t *testing.T) {
	cases := []struct {
		name        string
		configured  []string
		snapshotted []string
		want        []string
	}{
		{"nothing added", []string{"a", "b"}, []string{"a", "b"}, nil},
		{"one added", []string{"a", "b"}, []string{"a"}, []string{"b"}},
		{"added preserves config order", []string{"c", "a", "b"}, []string{"a"}, []string{"c", "b"}},
		{"all new (fresh registry, no position) ", []string{"a"}, nil, []string{"a"}},
		{"registry superset tolerated", []string{"a"}, []string{"a", "gone"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, addedTables(tc.configured, tc.snapshotted))
		})
	}
}

// gtidFallbackLog picks the message for an empty captured GTID set: mode=ON
// is the benign self-healing case (Info — a freshly-enabled/brand-new server
// with no post-enable transactions, found in the field as "lag: null with no
// GTID lines despite gtid_mode=ON"); anything else is the documented degraded
// mode and must WARN, per the cdc-setup promise that file:pos positioning is
// visible rather than silent.
func TestGtidFallbackLog(t *testing.T) {
	msg, level := gtidFallbackLog("ON")
	require.Equal(t, zapcore.InfoLevel, level)
	require.Contains(t, msg, "upgrading to GTID automatically")

	for _, mode := range []string{"OFF", "OFF_PERMISSIVE", "ON_PERMISSIVE", "unknown"} {
		msg, level := gtidFallbackLog(mode)
		require.Equal(t, zapcore.WarnLevel, level, "mode %s must warn", mode)
		require.Contains(t, msg, "not", "mode %s message must state the degradation", mode)
	}
}
