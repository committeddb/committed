//go:build docker || integration

package sqlserver_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/ingesttest"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/sqlserver"
)

// gateLevel is a FeatureReader for a cluster whose minimum feature level is
// fixed: every level up to it is enabled.
type gateLevel uint64

func (g gateLevel) FeatureEnabled(level uint64) bool { return level <= uint64(g) }

// TestSQLServerTemporalRenderingUpgrade pins the feature-level-3 transition
// end to end, on a table keyed by a DATE. Run 1 (minimum level 2 — a
// 0.8.1/0.8.2 mixed cluster) keeps the legacy RFC 3339 renderings in keys and
// payloads. Run 2 (level 3) resuming that checkpoint re-snapshots once at a
// bumped epoch, every key and payload by declared type, closing with the
// marker that sweeps the legacy rows on keyed sinks. Run 3 resuming run 2's
// checkpoint with the gate reading lower does NOT re-snapshot, and its live
// upsert and delete — keys through the change-tracking path — render typed.
func TestSQLServerTemporalRenderingUpgrade(t *testing.T) {
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(`DROP TABLE IF EXISTS dbo.ct_temporal`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE dbo.ct_temporal (
		d DATE NOT NULL PRIMARY KEY, t TIME(1), dt DATETIME, dt2 DATETIME2(3),
		sdt SMALLDATETIME, dto DATETIMEOFFSET(2), dto7 DATETIMEOFFSET)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO dbo.ct_temporal VALUES
		('2024-01-15', '12:30:45.5', '2024-01-15 12:30:45.127', '2024-01-15 12:30:45.78',
		 '2024-01-15 12:30:40', '2024-01-15 12:30:45.12 +05:30', '2024-01-15 12:30:45 +00:00'),
		('2024-01-16', '00:00:00', '2024-01-16 00:00:00', '2024-01-16 00:00:00',
		 '2024-01-16 00:00:00', '2024-01-16 00:00:00 -08:00', NULL)`)
	require.NoError(t, err)

	config := &sql.Config{
		Type: &cluster.Type{ID: "ct-temporal", Name: "ct-temporal"},
		Mappings: []sql.Mapping{
			{JsonName: "d", SQLColumn: "d"},
			{JsonName: "t", SQLColumn: "t"},
			{JsonName: "dt", SQLColumn: "dt"},
			{JsonName: "dt2", SQLColumn: "dt2"},
			{JsonName: "sdt", SQLColumn: "sdt"},
			{JsonName: "dto", SQLColumn: "dto"},
			{JsonName: "dto7", SQLColumn: "dto7"},
		},
		PrimaryKey:       []string{"d"},
		ConnectionString: ingestURL,
		Tables:           []string{"ct_temporal"},
		Options:          sql.Options{PollInterval: 300 * time.Millisecond},
	}
	payload := func(e *cluster.Entity) string {
		var m map[string]any
		require.NoError(t, json.Unmarshal(e.Data, &m))
		bs, err := json.Marshal(m)
		require.NoError(t, err)
		return string(bs)
	}

	// Run 1: minimum level 2 — the legacy renderings. (In one batch: the
	// legacy key text does not convert back to a DATE, so a legacy snapshot
	// over a temporal key cannot page — one of the defects the typed
	// rendering fixes.)
	const legacyKey = "2024-01-15 00:00:00 +0000 UTC"
	ctx1, cancel1 := context.WithCancel(context.Background())
	pr1 := make(chan *cluster.Proposal, 64)
	po1 := make(chan cluster.Position, 64)
	go func() {
		_ = (&sqlserver.SQLServerDialect{Features: gateLevel(2)}).Ingest(ctx1, config, nil, 0, pr1, po1)
	}()
	run1 := awaitStreaming(t, pr1, po1, 2*time.Minute, legacyKey)
	cancel1()
	var seed *cluster.Entity
	for _, p := range run1.Proposals {
		for _, e := range p.Entities {
			if string(e.Key) == legacyKey {
				seed = e
			}
		}
	}
	require.NotNil(t, seed)
	require.Contains(t, payload(seed), `"d":"2024-01-15T00:00:00Z"`, "below level 3: the legacy instant, byte-for-byte")
	require.Contains(t, payload(seed), `"t":"0001-01-01T12:30:45.5Z"`)
	legacyCheckpoint := run1.Position

	// Run 2: level 3, resuming the legacy checkpoint — re-key once, one row
	// per batch: the keyset cursor binds the typed key back.
	typedConfig := *config
	typedConfig.Options.BatchSize = 1
	core, observed := observer.New(zap.WarnLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Minute)
	pr2 := make(chan *cluster.Proposal, 64)
	po2 := make(chan cluster.Position, 64)
	go func() {
		_ = (&sqlserver.SQLServerDialect{Features: gateLevel(3)}).Ingest(ctx2, &typedConfig, legacyCheckpoint, 0, pr2, po2)
	}()
	res := ingesttest.AwaitRefresh(t, pr2, po2, 90*time.Second, isStreamingPosition, "2024-01-15", "2024-01-16")
	cancel2()
	restore()
	for _, p := range res.Proposals {
		for _, e := range p.Entities {
			require.NotContains(t, string(e.Key), "UTC", "level 3: no row may keep the legacy key")
		}
	}
	rekeyed := res.Entity("2024-01-15")
	require.Equal(t,
		`{"d":"2024-01-15","dt":"2024-01-15 12:30:45.127","dt2":"2024-01-15 12:30:45.780","dto":"2024-01-15 12:30:45.12+05:30","dto7":"2024-01-15 12:30:45.0000000+00:00","sdt":"2024-01-15 12:31:00","t":"12:30:45.5"}`,
		payload(rekeyed), "every temporal type by declared type (smalldatetime rounds to the minute at the source)")
	require.Equal(t, uint64(2), rekeyed.Generation, "the re-key is a bumped-epoch re-snapshot")
	require.Equal(t, uint64(2), res.MarkerEpoch, "and closes with the marker that sweeps the legacy rows on keyed destinations")
	require.NotEmpty(t, observed.FilterMessageSnippet("value rendering changed").All(), "the re-key is announced")
	typedCheckpoint := res.Position

	// Run 3: the typed checkpoint, gate reading level 2 (a member mid-join)
	// — no re-snapshot, and the live upsert and delete render typed.
	ctx3, cancel3 := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel3()
	pr3 := make(chan *cluster.Proposal, 64)
	po3 := make(chan cluster.Position, 64)
	go func() {
		_ = (&sqlserver.SQLServerDialect{Features: gateLevel(2)}).Ingest(ctx3, config, typedCheckpoint, 0, pr3, po3)
	}()
	_, err = db.Exec(`INSERT INTO dbo.ct_temporal (d, t) VALUES ('2024-02-01', '08:00:00.1')`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM dbo.ct_temporal WHERE d = '2024-01-16'`)
	require.NoError(t, err)
	deadline := time.After(90 * time.Second)
	var live, deleted *cluster.Entity
	for live == nil || deleted == nil {
		select {
		case p := <-pr3:
			for _, e := range p.Entities {
				require.False(t, e.IsRefreshBoundary(), "a typed checkpoint must not re-snapshot (no marker)")
				switch {
				case string(e.Key) == "2024-02-01":
					live = e
				case string(e.Key) == "2024-01-16" && e.IsDelete():
					deleted = e
				}
			}
		case <-po3:
		case <-deadline:
			t.Fatal("the live insert and delete never arrived through the poll")
		}
	}
	require.Contains(t, payload(live), `"t":"08:00:00.1"`, "a typed session renders typed, gate or no gate")
}

// TestSQLServerTemporalKeyCursor pins snapshot pagination over a
// DATETIMEOFFSET primary key: each batch binds the previous batch's rendered
// key back as the keyset cursor, so the typed rendering must convert back to
// the column's type in SQL Server — offset included.
func TestSQLServerTemporalKeyCursor(t *testing.T) {
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(`DROP TABLE IF EXISTS dbo.ct_tzkey`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE dbo.ct_tzkey (at DATETIMEOFFSET(3) NOT NULL PRIMARY KEY, v INT)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO dbo.ct_tzkey VALUES
		('2024-01-15 12:00:00.5 +05:30', 1), ('2024-01-15 12:00:00.5 +00:00', 2), ('2024-01-15 07:00:00 -08:00', 3)`)
	require.NoError(t, err)
	config := &sql.Config{
		Type:             &cluster.Type{ID: "ct-tzkey", Name: "ct-tzkey"},
		Mappings:         []sql.Mapping{{JsonName: "at", SQLColumn: "at"}, {JsonName: "v", SQLColumn: "v"}},
		PrimaryKey:       []string{"at"},
		ConnectionString: ingestURL,
		Tables:           []string{"ct_tzkey"},
		Options:          sql.Options{PollInterval: 300 * time.Millisecond, BatchSize: 1},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 64)
	po := make(chan cluster.Position, 64)
	go func() { _ = (&sqlserver.SQLServerDialect{}).Ingest(ctx, config, nil, 0, pr, po) }()
	got := drainEntities(t, pr, po, 3, 2*time.Minute)
	keys := make([]string, len(got))
	for i, e := range got {
		keys[i] = string(e.Key)
	}
	require.ElementsMatch(t, []string{
		"2024-01-15 12:00:00.500+05:30", "2024-01-15 12:00:00.500+00:00", "2024-01-15 07:00:00.000-08:00",
	}, keys, "every row exactly once across one-row batches")
}
