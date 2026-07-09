package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWatches_CaseInsensitive is the B1 regression: a mixed-case `tables` config
// must still match the binlog's table name (which arrives in the source's stored
// case), or the row filter drops every streamed change — total, silent CDC data
// loss after the initial snapshot. Exercises the full fix: lowerAll at
// construction plus the case-insensitive match.
func TestWatches_CaseInsensitive(t *testing.T) {
	h := &MySQLEventHandler{tables: lowerAll([]string{"Users", "OrderItems"})}

	require.True(t, h.watches("Users"), "exact config case matches")
	require.True(t, h.watches("users"), "lowercase binlog name matches the uppercase config")
	require.True(t, h.watches("ORDERITEMS"), "any case matches")
	require.False(t, h.watches("payments"), "an unwatched table does not match")
}
