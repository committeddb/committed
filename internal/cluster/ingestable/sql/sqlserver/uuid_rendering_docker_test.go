//go:build docker

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

// gate is a FeatureReader whose answer is fixed: the cluster is (not) at the
// asked level.
type gate bool

func (g gate) FeatureEnabled(uint64) bool { return bool(g) }

const (
	guidUpper = "3E11FA47-71CA-11E1-9E33-C80AA9429562"
	guidLower = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
)

// TestSQLServerUniqueidentifierRenderingUpgrade pins the feature-level-5
// transition end to end. Run 1 (gate closed — a mixed-version cluster) keeps
// the pre-0.8.0 uppercase spelling in keys and payloads and records it in the
// checkpoint. Run 2 (gate open) resuming that checkpoint re-snapshots once at
// a bumped epoch with every key and payload spelled canonically and closes
// with a refresh marker, so a keyed sink sweeps the uppercase rows. Run 3
// resuming run 2's checkpoint does NOT re-snapshot: a live change arrives
// through the poll, lowercase, with no marker — and the promotion is one-way
// even with the gate reading closed.
func TestSQLServerUniqueidentifierRenderingUpgrade(t *testing.T) {
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(`DROP TABLE IF EXISTS dbo.ct_uuid`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE dbo.ct_uuid (id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY, ref UNIQUEIDENTIFIER, v NVARCHAR(20))`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO dbo.ct_uuid (id, ref, v) VALUES ('` + guidUpper + `', '` + guidUpper + `', 'seed')`)
	require.NoError(t, err)

	config := &sql.Config{
		Type: &cluster.Type{ID: "ct-uuid", Name: "ct-uuid"},
		Mappings: []sql.Mapping{
			{JsonName: "id", SQLColumn: "id"},
			{JsonName: "ref", SQLColumn: "ref"},
			{JsonName: "v", SQLColumn: "v"},
		},
		PrimaryKey:       []string{"id"},
		ConnectionString: ingestURL,
		Tables:           []string{"ct_uuid"},
		Options:          map[string]string{"poll_interval": "300ms"},
	}
	payload := func(e *cluster.Entity) map[string]any {
		var m map[string]any
		require.NoError(t, json.Unmarshal(e.Data, &m))
		return m
	}

	// Run 1: gate closed — the legacy spelling, recorded in the checkpoint.
	ctx1, cancel1 := context.WithCancel(context.Background())
	pr1 := make(chan *cluster.Proposal, 64)
	po1 := make(chan cluster.Position, 64)
	go func() {
		_ = (&sqlserver.SQLServerDialect{Features: gate(false)}).Ingest(ctx1, config, nil, 0, pr1, po1)
	}()
	run1 := awaitStreaming(t, pr1, po1, 2*time.Minute, guidUpper)
	cancel1()
	var seed *cluster.Entity
	for _, p := range run1.Proposals {
		for _, e := range p.Entities {
			if string(e.Key) == guidUpper {
				seed = e
			}
		}
	}
	require.NotNil(t, seed)
	require.Equal(t, guidUpper, payload(seed)["ref"], "gate closed: the pre-0.8.0 uppercase spelling, byte-for-byte")
	require.Equal(t, uint64(1), seed.Generation)
	legacyCheckpoint := run1.Position

	// Run 2: gate open, resuming the legacy checkpoint — re-key once.
	core, observed := observer.New(zap.WarnLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Minute)
	pr2 := make(chan *cluster.Proposal, 64)
	po2 := make(chan cluster.Position, 64)
	go func() {
		_ = (&sqlserver.SQLServerDialect{Features: gate(true)}).Ingest(ctx2, config, legacyCheckpoint, 0, pr2, po2)
	}()
	// The re-key is a re-snapshot: every row re-emits, the closing marker
	// follows, and the completion checkpoint (the only no-progress position
	// this idle run emits) arrives on the position channel in either order.
	res := ingesttest.AwaitRefresh(t, pr2, po2, 90*time.Second, isStreamingPosition, guidLower)
	cancel2()
	restore()
	for _, p := range res.Proposals {
		for _, e := range p.Entities {
			require.NotEqual(t, guidUpper, string(e.Key), "gate open: no row may keep the uppercase key")
		}
	}
	rekeyed := res.Entity(guidLower)
	require.Equal(t, guidLower, payload(rekeyed)["ref"], "payload fields re-render too")
	require.Equal(t, uint64(2), rekeyed.Generation, "the re-key is a bumped-epoch re-snapshot")
	require.Equal(t, uint64(2), res.MarkerEpoch, "and closes with the marker that sweeps the uppercase rows on keyed sinks")
	require.NotEmpty(t, observed.FilterMessageSnippet("uniqueidentifier rendering changed").All(), "the re-key is announced")
	canonicalCheckpoint := res.Position

	// Run 3: the canonical checkpoint, gate reading closed (a member mid-join
	// announces level 0 for a moment) — no re-snapshot, no marker, and the
	// live change still spells canonically: the promotion is one-way.
	const newID = "A1B2C3D4-0000-1111-2222-333344445555"
	ctx3, cancel3 := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel3()
	pr3 := make(chan *cluster.Proposal, 64)
	po3 := make(chan cluster.Position, 64)
	go func() {
		_ = (&sqlserver.SQLServerDialect{Features: gate(false)}).Ingest(ctx3, config, canonicalCheckpoint, 0, pr3, po3)
	}()
	_, err = db.Exec(`INSERT INTO dbo.ct_uuid (id, ref, v) VALUES ('` + newID + `', '` + guidUpper + `', 'live')`)
	require.NoError(t, err)
	deadline := time.After(90 * time.Second)
	for {
		var live *cluster.Entity
		select {
		case p := <-pr3:
			for _, e := range p.Entities {
				require.False(t, e.IsRefreshBoundary(), "a canonical checkpoint must not re-snapshot (no marker)")
				require.NotEqual(t, guidLower, string(e.Key), "the seed row must not re-emit (no re-snapshot)")
				if string(e.Key) == "a1b2c3d4-0000-1111-2222-333344445555" {
					live = e
				}
			}
		case <-po3:
		case <-deadline:
			t.Fatal("the live insert never arrived through the poll")
		}
		if live != nil {
			require.Equal(t, guidLower, payload(live)["ref"], "a canonical session spells every GUID canonically, gate or no gate")
			return
		}
	}
}
