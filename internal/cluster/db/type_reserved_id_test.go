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

// A user must also not author a type whose id is a grandfathered BUILT-IN system
// type (e.g. databaseType) — not just the reserved namespace. The apply path
// resolves such an id to the system type (systemType-first), so a user type
// sitting in the bucket under that id lets a later proposal's bytes reach an
// internal config handler that Fatals on the decode mismatch — crash-looping
// every node on a deterministic committed entry.
func TestParseType_RejectsBuiltinSystemID(t *testing.T) {
	// databaseType's grandfathered built-in id (frozen; cluster.IsInternal == true).
	const databaseTypeID = "4698b77e-9a7c-41a2-aae4-984da0cd33c1"
	require.True(t, cluster.IsInternal(databaseTypeID), "guard test needs a real built-in system id")

	cfg := &cluster.Configuration{
		ID:       databaseTypeID,
		MimeType: "text/toml",
		Data:     []byte("[type]\nname = \"sneaky\""),
	}
	_, _, err := db.ParseType(cfg, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "system-type", "the rejection must name the system-type collision")
}
