package sqlserver

import (
	"testing"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/require"

	sql "github.com/committeddb/committed/internal/cluster/ingestable/sql"
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

// The poll cadence defaults at the accessor; a non-positive value never
// reaches here (parseOptions refuses it at admission).
func TestPollInterval(t *testing.T) {
	require.Equal(t, defaultPollInterval, sql.Options{}.PollIntervalOr(defaultPollInterval))
	require.Equal(t, 10*time.Second, sql.Options{PollInterval: 10 * time.Second}.PollIntervalOr(defaultPollInterval))
}

func TestRenderUniqueidentifier(t *testing.T) {
	var u mssql.UniqueIdentifier
	require.NoError(t, u.Scan("3E11FA47-71CA-11E1-9E33-C80AA9429562"))
	require.Equal(t, "3E11FA47-71CA-11E1-9E33-C80AA9429562", renderUniqueidentifier(u, false),
		"legacy is the driver's uppercase GUID — the pre-0.8.0 bytes, byte-for-byte")
	require.Equal(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562", renderUniqueidentifier(u, true),
		"canonical is RFC 4122 lowercase — what PostgreSQL's uuid ingests as")
}

func TestSessionRendering(t *testing.T) {
	cases := []struct {
		name          string
		checkpoint    uint32
		hasCheckpoint bool
		gateOpen      bool
		rendering     uint32
		rerender      bool
	}{
		{"fresh ingestable, gate closed: legacy", uuidRenderingLegacy, false, false, uuidRenderingLegacy, false},
		{"fresh ingestable, gate open: canonical, nothing to re-key", uuidRenderingLegacy, false, true, uuidRenderingCanonical, false},
		{"legacy checkpoint, gate closed: stay legacy (mixed-version cluster)", uuidRenderingLegacy, true, false, uuidRenderingLegacy, false},
		{"legacy checkpoint, gate open: promote and re-snapshot once", uuidRenderingLegacy, true, true, uuidRenderingCanonical, true},
		{"canonical checkpoint, gate open: no-op", uuidRenderingCanonical, true, true, uuidRenderingCanonical, false},
		{"canonical checkpoint, gate reads closed (member mid-join): one-way, never back", uuidRenderingCanonical, true, false, uuidRenderingCanonical, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rendering, rerender := sessionRendering(c.checkpoint, c.hasCheckpoint, c.gateOpen)
			require.Equal(t, c.rendering, rendering)
			require.Equal(t, c.rerender, rerender)
		})
	}
}

// renderTemporal spells each date/time type as the source shows it — the
// renderings PostgreSQL and MySQL ingest the same values as.
func TestRenderTemporal(t *testing.T) {
	utc := func(h, m, s, ns int) time.Time { return time.Date(2024, 1, 15, h, m, s, ns, time.UTC) }
	ist := time.FixedZone("", 5*3600+1800)
	cases := []struct {
		name string
		v    time.Time
		col  temporalCol
		want string
	}{
		{"date", utc(0, 0, 0, 0), temporalCol{typ: "DATE"}, "2024-01-15"},
		{"time(1)", time.Date(1, 1, 1, 12, 30, 45, 500000000, time.UTC), temporalCol{typ: "TIME", scale: 1}, "12:30:45.5"},
		{"time(0)", time.Date(1, 1, 1, 23, 59, 59, 0, time.UTC), temporalCol{typ: "TIME"}, "23:59:59"},
		{"datetime2(7)", utc(12, 30, 45, 123456700), temporalCol{typ: "DATETIME2", scale: 7}, "2024-01-15 12:30:45.1234567"},
		{"datetime2(3) pads to its scale", utc(12, 30, 45, 780000000), temporalCol{typ: "DATETIME2", scale: 3}, "2024-01-15 12:30:45.780"},
		{"datetime2(0)", utc(12, 30, 45, 0), temporalCol{typ: "DATETIME2"}, "2024-01-15 12:30:45"},
		{"datetime rounds its 1/300 s tick", utc(12, 30, 45, 126666667), temporalCol{typ: "DATETIME", scale: 3}, "2024-01-15 12:30:45.127"},
		{"datetime tick carrying into the next day", time.Date(2024, 1, 15, 23, 59, 59, 999666667, time.UTC), temporalCol{typ: "DATETIME", scale: 3}, "2024-01-16 00:00:00.000"},
		{"smalldatetime", utc(12, 31, 0, 0), temporalCol{typ: "SMALLDATETIME"}, "2024-01-15 12:31:00"},
		{"datetimeoffset keeps its offset", time.Date(2024, 1, 15, 12, 30, 45, 120000000, ist), temporalCol{typ: "DATETIMEOFFSET", scale: 2}, "2024-01-15 12:30:45.12+05:30"},
		{"datetimeoffset at UTC is +00:00, not Z", utc(12, 30, 45, 0), temporalCol{typ: "DATETIMEOFFSET"}, "2024-01-15 12:30:45+00:00"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, renderTemporal(c.v, c.col))
		})
	}
}
