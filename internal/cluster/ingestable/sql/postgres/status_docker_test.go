//go:build docker || integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
)

// TestPostgresStatusStreamingLag drives a real replication slot into the
// streaming phase, then exercises the live half of Status that the offline
// unit tests cannot: phase=streaming, a valid LSN, a non-nil source lag, and
// lag converging to 0 (CaughtUp) once the source is quiet.
func TestPostgresStatusStreamingLag(t *testing.T) {
	table := "status_lag_table"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, val TEXT)`, table))
	require.NoError(t, err)
	db.Close()

	cleanReplication(t, "slot_status", "pub_status")

	config := &sql.Config{
		Type: &cluster.Type{ID: "status", Name: "status"},
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "val", SQLColumn: "val"},
		},
		PrimaryKey:       []string{"pk"},
		ConnectionString: connString,
		Tables:           []string{table},
		Options: map[string]string{
			"slot_name":   "slot_status",
			"publication": "pub_status",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proposalChan := make(chan *cluster.Proposal, 10)
	positionChan := make(chan cluster.Position, 10)

	dialect := &postgres.PostgreSQLDialect{}
	go func() { _ = dialect.Ingest(ctx, config, nil, proposalChan, positionChan) }()

	waitForSlot(t, "slot_status")

	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('a', 'one')`, table))
	require.NoError(t, err)
	db.Close()

	// Wait for the insert to stream and a commit position (Lsn>0) to land, so
	// we know we are in the streaming phase with a real LSN to pass to Status.
	deadline := time.After(20 * time.Second)
	var lastPos cluster.Position
	seen := false
	for !seen || lastPos == nil {
		select {
		case p := <-proposalChan:
			for _, e := range p.Entities {
				if string(e.Key) == "a" {
					seen = true
				}
			}
		case pos := <-positionChan:
			if isCommitPosition(t, pos) {
				lastPos = pos
			}
		case <-deadline:
			t.Fatal("timed out waiting for the streamed insert + commit position")
		}
	}

	// Keep draining so the dialect's loop runs and sends standby status
	// updates (which advance the slot's confirmed_flush_lsn). The capture loop
	// above has finished, so there is no contention on the channels.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-positionChan:
			case <-proposalChan:
			}
		}
	}()

	// Streaming status: a valid LSN and a measurable lag against the live slot.
	st, err := dialect.Status(ctx, config, lastPos)
	require.NoError(t, err)
	require.Equal(t, "streaming", st.Phase)
	require.NotEmpty(t, st.Position)
	_, perr := pglogrepl.ParseLSN(st.Position)
	require.NoError(t, perr, "Position must be a valid LSN")
	require.NotNil(t, st.Lag, "streaming lag must be queryable against a live slot")

	// With no further writes, the slot's confirmed flush catches up to the
	// source write head: lag → 0 and CaughtUp flips true.
	require.Eventually(t, func() bool {
		s, e := dialect.Status(ctx, config, lastPos)
		return e == nil && s.CaughtUp
	}, 25*time.Second, 500*time.Millisecond, "CaughtUp should become true once the source is quiet")
}
