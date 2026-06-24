package sql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// TestCheckKeyCoverage covers the dialect-agnostic guard logic: every configured
// primary-key column must be present in the columns that survive a delete.
func TestCheckKeyCoverage(t *testing.T) {
	// Covered.
	require.NoError(t, sql.CheckKeyCoverage([]string{"id"}, []string{"id"}, "t", "fix"))
	require.NoError(t, sql.CheckKeyCoverage([]string{"a", "b"}, []string{"b", "a", "c"}, "t", "fix"),
		"order doesn't matter and extra surviving columns are fine")
	require.NoError(t, sql.CheckKeyCoverage([]string{"ID"}, []string{"id"}, "t", "fix"),
		"comparison is case-insensitive")
	require.NoError(t, sql.CheckKeyCoverage(nil, nil, "t", "fix"),
		"no configured key → nothing to cover")

	// Not covered — the error names the missing column, the table, and the fix.
	err := sql.CheckKeyCoverage([]string{"movie_id", "billing"}, []string{"movie_id"}, "credit",
		"set binlog_row_image=FULL")
	require.Error(t, err)
	require.Contains(t, err.Error(), "silently drop deletes")
	require.Contains(t, err.Error(), "billing")                   // the missing column
	require.Contains(t, err.Error(), "credit")                    // the table
	require.Contains(t, err.Error(), "set binlog_row_image=FULL") // the fix

	// Nothing survives (e.g. REPLICA IDENTITY NOTHING) → the whole key is missing.
	require.Error(t, sql.CheckKeyCoverage([]string{"id"}, nil, "t", "fix"))
}
