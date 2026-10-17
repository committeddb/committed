//go:build docker || integration

package mysql_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/ingesttest"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/mysql"
)

// gateLevel is a FeatureReader for a cluster whose minimum feature level is
// the value.
type gateLevel uint64

func (g gateLevel) FeatureEnabled(level uint64) bool { return level <= uint64(g) }

// streamingRendering accepts a checkpoint past the snapshot (no snapshot
// progress) that records the given BINARY(n) rendering.
func streamingRendering(t *testing.T, rendering uint32) func(cluster.Position) bool {
	return func(pos cluster.Position) bool {
		p := &dialectpb.MySQLBinLogPosition{}
		require.NoError(t, proto.Unmarshal(pos, p))
		return p.SnapshotProgress == nil && p.BinaryRendering == rendering
	}
}

// binaryRenderingConfig makes a one-table ingestable keyed by k.
func binaryRenderingConfig(table string) *sql.Config {
	return &sql.Config{
		Type:             &cluster.Type{ID: table, Name: table},
		Mappings:         []sql.Mapping{{JsonName: "k", SQLColumn: "k"}, {JsonName: "v", SQLColumn: "v"}},
		PrimaryKey:       []string{"k"},
		ConnectionString: ingestURL,
		Tables:           []string{table},
	}
}

// TestMysqlBinaryRenderingUpgrade pins the feature-level-4 transition end to
// end. Run 1 (minimum level 3 — a 0.8.2/0.8.3 mixed cluster) streams a
// BINARY(6) key as the binlog carries it, trailing 0x00 bytes stripped, and
// records that in the checkpoint. Run 2 (level 4) resuming that checkpoint
// re-snapshots once at a bumped epoch with every key padded, and closes with
// the marker that sweeps the stripped-key rows on keyed destinations.
func TestMysqlBinaryRenderingUpgrade(t *testing.T) {
	const table = "binrender_upgrade"
	db := createDB(t)
	defer db.Close()
	for _, stmt := range []string{
		fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table),
		fmt.Sprintf("CREATE TABLE `%s` (k BINARY(6) NOT NULL PRIMARY KEY, v INT)", table),
		fmt.Sprintf("INSERT INTO `%s` VALUES (x'4142', 1)", table),
	} {
		_, err := db.Exec(stmt)
		require.NoError(t, err, stmt)
	}
	config := binaryRenderingConfig(table)
	const padded1, padded2, stripped2 = "AB\x00\x00\x00\x00", "CD\x00\x00\x00\x00", "CD"

	// Run 1: minimum level 3. The snapshot reads the padded value; the
	// streamed row arrives stripped.
	ctx1, cancel1 := context.WithCancel(context.Background())
	pr1 := make(chan *cluster.Proposal, 64)
	po1 := make(chan cluster.Position, 64)
	go func() {
		_ = (&mysql.MySQLDialect{Features: gateLevel(3)}).Ingest(ctx1, config, nil, 0, pr1, po1)
	}()
	ingesttest.Await(t, pr1, po1, 60*time.Second, nil, padded1)
	_, err := db.Exec(fmt.Sprintf("INSERT INTO `%s` VALUES (x'4344', 2)", table))
	require.NoError(t, err)
	run1 := ingesttest.Await(t, pr1, po1, 30*time.Second, streamingRendering(t, 0), stripped2)
	cancel1()
	legacyCheckpoint := run1.Position

	// Run 2: level 4, resuming the stripped checkpoint — re-key once.
	core, observed := observer.New(zap.WarnLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel2()
	pr2 := make(chan *cluster.Proposal, 64)
	po2 := make(chan cluster.Position, 64)
	go func() {
		_ = (&mysql.MySQLDialect{Features: gateLevel(4)}).Ingest(ctx2, config, legacyCheckpoint, 0, pr2, po2)
	}()
	res := ingesttest.AwaitRefresh(t, pr2, po2, 90*time.Second, streamingRendering(t, 1), padded1, padded2)
	restore()
	for _, p := range res.Proposals {
		for _, e := range p.Entities {
			require.NotEqual(t, stripped2, string(e.Key), "level 4: no row may keep the stripped key")
		}
	}
	require.Equal(t, uint64(2), res.Entity(padded2).Generation, "the re-key is a bumped-epoch re-snapshot")
	require.Equal(t, uint64(2), res.MarkerEpoch, "and closes with the marker that sweeps the stripped-key rows on keyed destinations")
	require.NotEmpty(t, observed.FilterMessageSnippet("BINARY(n) rendering changed").All(), "the re-key is announced")

	// The re-snapshot's fresh position streams padded.
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` VALUES (x'4546', 3)", table))
	require.NoError(t, err)
	ingesttest.Await(t, pr2, po2, 30*time.Second, nil, "EF\x00\x00\x00\x00")
}

// A stripped checkpoint whose tables have no BINARY column is promoted
// without a re-snapshot: the next checkpoint records it.
func TestMysqlBinaryRenderingUpgradeSkipsUnaffectedTables(t *testing.T) {
	const table = "binrender_noop"
	db := createDB(t)
	defer db.Close()
	for _, stmt := range []string{
		fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table),
		fmt.Sprintf("CREATE TABLE `%s` (k VARBINARY(6) NOT NULL PRIMARY KEY, v INT)", table),
		fmt.Sprintf("INSERT INTO `%s` VALUES (x'4142', 1)", table),
	} {
		_, err := db.Exec(stmt)
		require.NoError(t, err, stmt)
	}
	config := binaryRenderingConfig(table)

	ctx1, cancel1 := context.WithCancel(context.Background())
	pr1 := make(chan *cluster.Proposal, 64)
	po1 := make(chan cluster.Position, 64)
	go func() {
		_ = (&mysql.MySQLDialect{Features: gateLevel(3)}).Ingest(ctx1, config, nil, 0, pr1, po1)
	}()
	ingesttest.Await(t, pr1, po1, 60*time.Second, nil, "AB")
	_, err := db.Exec(fmt.Sprintf("INSERT INTO `%s` VALUES (x'4344', 2)", table))
	require.NoError(t, err)
	run1 := ingesttest.Await(t, pr1, po1, 30*time.Second, streamingRendering(t, 0), "CD")
	cancel1()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel2()
	pr2 := make(chan *cluster.Proposal, 64)
	po2 := make(chan cluster.Position, 64)
	go func() {
		_ = (&mysql.MySQLDialect{Features: gateLevel(4)}).Ingest(ctx2, config, run1.Position, 0, pr2, po2)
	}()
	// Run 2 resumes from run 1's binlog position, so this row streams whenever
	// the syncer connects.
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` VALUES (x'4546', 3)", table))
	require.NoError(t, err)
	res := ingesttest.Await(t, pr2, po2, 30*time.Second, streamingRendering(t, 1), "EF")
	for _, p := range res.Proposals {
		for _, e := range p.Entities {
			require.False(t, e.IsRefreshBoundary(), "no BINARY column, so no re-snapshot")
			require.NotEqual(t, "AB", string(e.Key), "no re-snapshot re-emits row 1")
		}
	}
}
