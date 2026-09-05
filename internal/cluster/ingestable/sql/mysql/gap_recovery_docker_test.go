//go:build docker

package mysql_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/ingesttest"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/mysql"
)

// lastBinlogFile returns the name of the newest binary log file — the one
// PURGE BINARY LOGS TO keeps.
func lastBinlogFile(t *testing.T, db *gosql.DB) string {
	t.Helper()
	rows, err := db.Query("SHOW BINARY LOGS")
	require.NoError(t, err)
	defer rows.Close()
	cols, err := rows.Columns()
	require.NoError(t, err)
	var last string
	for rows.Next() {
		var name string
		dest := make([]any, len(cols))
		dest[0] = &name
		for i := 1; i < len(cols); i++ {
			var rest any
			dest[i] = &rest
		}
		require.NoError(t, rows.Scan(dest...))
		last = name
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, last)
	return last
}

// TestMysqlPurgedBinlogResnapshots drives the error-1236 recovery path
// through the whole loop (only its classifier, isGtidPurged, was pinned
// before): a worker resuming by a GTID set the source has since purged past
// must re-snapshot at a bumped epoch and close with a refresh-boundary
// marker, so the rows changed in the purged window arrive and a keyed sink
// sweeps what that window deleted. The MySQL twin of
// TestPostgresSlotRecreatedResnapshots.
func TestMysqlPurgedBinlogResnapshots(t *testing.T) {
	table := "gap_purge"

	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE `%s` (id VARCHAR(32) NOT NULL, val TEXT, PRIMARY KEY (id));", table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (id, val) VALUES ('seed', 's');", table))
	require.NoError(t, err)

	config := &sql.Config{
		Type:             &cluster.Type{ID: "gappurge", Name: "gappurge"},
		Mappings:         []sql.Mapping{{JsonName: "id", SQLColumn: "id"}, {JsonName: "val", SQLColumn: "val"}},
		PrimaryKey:       []string{"id"},
		ConnectionString: ingestURL,
		Tables:           []string{table},
	}

	// Run 1: snapshot the seed, then a streamed commit whose checkpoint
	// carries the consumed GTID set (the container runs gtid_mode=ON).
	ctx1, cancel1 := context.WithCancel(context.Background())
	pr1 := make(chan *cluster.Proposal, 20)
	po1 := make(chan cluster.Position, 20)
	go func() { _ = (&mysql.MySQLDialect{}).Ingest(ctx1, config, nil, 0, pr1, po1) }()
	ingesttest.Await(t, pr1, po1, 15*time.Second, nil, "seed")
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (id, val) VALUES ('before', 'b');", table))
	require.NoError(t, err)
	checkpoint := awaitStreaming(t, pr1, po1, 15*time.Second, "before").Position
	cancel1()

	// While no worker runs: a transaction the checkpoint does not cover, then
	// rotate and purge every binlog file before the current one. gtid_purged
	// now holds that transaction's GTID, which the resume set lacks, so the
	// server refuses auto-positioning from the checkpoint with error 1236.
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (id, val) VALUES ('lost', 'l');", table))
	require.NoError(t, err)
	_, err = db.Exec("FLUSH BINARY LOGS")
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf("PURGE BINARY LOGS TO '%s'", lastBinlogFile(t, db)))
	require.NoError(t, err)
	var purged string
	require.NoError(t, db.QueryRow("SELECT @@global.gtid_purged").Scan(&purged))
	require.NotEmpty(t, purged, "the purge must retire GTIDs, or the resume would simply succeed")

	// Capture the loop's Error lines: the recovery must be the loud 1236 path,
	// not an incidental cold snapshot.
	core, observed := observer.New(zap.ErrorLevel)
	defer zap.ReplaceGlobals(zap.New(core))()

	// Run 2: resume from the stale checkpoint.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	pr2 := make(chan *cluster.Proposal, 20)
	po2 := make(chan cluster.Position, 20)
	errCh := make(chan error, 1)
	go func() { errCh <- (&mysql.MySQLDialect{}).Ingest(ctx2, config, checkpoint, 0, pr2, po2) }()
	go func() {
		if err := <-errCh; err != nil {
			t.Errorf("Ingest exited instead of recovering from the purged binlog: %v", err)
		}
	}()

	// The recovery re-snapshot re-emits every row and closes with its marker.
	res := ingesttest.AwaitRefresh(t, pr2, po2, 60*time.Second, nil, "seed", "before", "lost")
	cancel2()

	require.NotEmpty(t, observed.FilterMessageSnippet("binlog purged past consumed position").All(),
		"recovery must take the error-1236 branch")
	require.Greater(t, res.MarkerEpoch, uint64(1),
		"the recovery re-snapshot closes with a marker strictly above run 1's generation")
	for _, k := range []string{"seed", "before", "lost"} {
		require.Equal(t, res.MarkerEpoch, res.Entity(k).Generation, "row %q must re-snapshot at the marker's generation", k)
	}
}
