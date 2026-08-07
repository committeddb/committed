package sqlserver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The SourceSeq encoding must be strictly monotonic across (version, sub)
// pairs — the ingest dedup contract — and refuse (ok=false → DedupUnsafe
// freeze) rather than wrap on any bound overflow.
func TestEncodeCTSeq(t *testing.T) {
	a, ok := encodeCTSeq(100, 0)
	require.True(t, ok)
	b, ok := encodeCTSeq(100, 1)
	require.True(t, ok)
	c, ok := encodeCTSeq(101, 0)
	require.True(t, ok)
	require.Less(t, a, b, "sub orders within one version")
	require.Less(t, b, c, "a later version outranks every sub of an earlier one")

	_, ok = encodeCTSeq(100, ctSeqSubMax+1)
	require.False(t, ok, "sub overflow must refuse, not wrap")
	_, ok = encodeCTSeq(ctSeqVerMax+1, 0)
	require.False(t, ok, "version overflow must refuse, not wrap")
	_, ok = encodeCTSeq(100, -1)
	require.False(t, ok)

	top, ok := encodeCTSeq(ctSeqVerMax, ctSeqSubMax)
	require.True(t, ok)
	require.Equal(t, ^uint64(0), top, "the bounds exactly fill uint64")
}

// Bare table names scope to dbo (mirroring MySQL's DSN-database scoping);
// schema-qualified names keep their schema.
func TestResolveTableRef(t *testing.T) {
	r := resolveTableRef("orders")
	require.Equal(t, tableRef{schema: "dbo", name: "orders"}, r)
	require.Equal(t, `"dbo"."orders"`, r.qualified())
	require.Equal(t, "dbo.orders", r.objectID())

	r = resolveTableRef("sales.orders")
	require.Equal(t, tableRef{schema: "sales", name: "orders"}, r)
	require.Equal(t, `"sales"."orders"`, r.qualified())
}

// addedTables diffs configured tables against the snapshotted registry — the
// added-table backfill trigger — with the empty registry grandfathered as
// all-snapshotted (the pre-registry compat contract shared with MySQL).
func TestAddedTables(t *testing.T) {
	require.Nil(t, addedTables([]string{"a", "b"}, nil),
		"empty registry is grandfathered, never a backfill")
	require.Equal(t, []string{"c"}, addedTables([]string{"a", "b", "c"}, []string{"a", "b"}))
	require.Nil(t, addedTables([]string{"a"}, []string{"a", "b"}),
		"removed tables are not 'added'")
}

// stringifyKeyValue is the ONE key-stringification for snapshot, upsert, and
// delete paths — []byte as raw string (not fmt's byte-slice rendering) so
// the same row keys identically everywhere.
func TestStringifyKeyValue(t *testing.T) {
	require.Equal(t, "abc", stringifyKeyValue([]byte("abc")))
	require.Equal(t, "42", stringifyKeyValue(int64(42)))
	require.Equal(t, "x", stringifyKeyValue("x"))
	require.Equal(t, "<nil>", stringifyKeyValue(nil))
}

func TestPollInterval(t *testing.T) {
	require.Equal(t, defaultPollInterval, pollInterval(nil))
	require.Equal(t, 10*time.Second, pollInterval(map[string]string{"poll_interval": "10s"}))
	require.Equal(t, defaultPollInterval, pollInterval(map[string]string{"poll_interval": "-1s"}),
		"a non-positive cadence falls back rather than busy-looping")
}
