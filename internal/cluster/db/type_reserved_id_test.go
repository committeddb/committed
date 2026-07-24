package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

// A user must not be able to author a type whose id lands in committed's
// reserved system-type namespace — an older node would treat it as a system
// record (skippable or must-gate) rather than user topic data. The config is
// otherwise valid (name-only, NoValidation), so the rejection is the reserved-id
// guard, not a parse failure.
func TestParseType_RejectsReservedSystemID(t *testing.T) {
	// Reserved namespace: ungated (state 1), index 4000.
	cfg := &cluster.Configuration{
		ID:       "c01177ed-0000-0000-0000-000000001fa0",
		MimeType: "text/toml",
		Data:     []byte("[type]\nname = \"sneaky\""),
	}
	_, _, err := db.ParseType(cfg, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reserved", "the rejection must name the reserved namespace")
}
