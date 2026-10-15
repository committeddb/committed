//go:build docker || integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/ingesttest"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
)

func partitionedConfig(slot string, primaryKey []string, tables ...string) *sql.Config {
	return &sql.Config{
		Type: &cluster.Type{ID: "part", Name: "part"},
		Mappings: []sql.Mapping{
			{JsonName: "id", SQLColumn: "id"}, {JsonName: "region", SQLColumn: "region"}, {JsonName: "val", SQLColumn: "val"},
		},
		PrimaryKey:       primaryKey,
		ConnectionString: connString,
		Tables:           tables,
		Options:          sql.Options{SlotName: slot, Publication: slot},
	}
}

func execAll(t *testing.T, stmts ...string) {
	t.Helper()
	db := createDB(t)
	defer db.Close()
	for _, stmt := range stmts {
		_, err := db.Exec(stmt)
		require.NoError(t, err, stmt)
	}
}

// entityByKey returns the last entity seen for key, so a test can tell a
// tombstone from an upsert.
func entityByKey(res ingesttest.Result, key string) *cluster.Entity {
	var found *cluster.Entity
	for _, p := range res.Proposals {
		for _, e := range p.Entities {
			if string(e.Key) == key {
				found = e
			}
		}
	}
	return found
}

// TestPostgresPartitionedTable is the partitioned-table silent-loss
// regression. The publication reported a partition's changes under the
// partition's name, which matched no watched table, so the snapshot read every
// row but every streamed change was dropped — and the empty commit still
// acked past it. Published via the root, the stream delivers every partition's
// changes under the root, including a partition created after the slot.
func TestPostgresPartitionedTable(t *testing.T) {
	execAll(t,
		`DROP TABLE IF EXISTS part_root`,
		`CREATE TABLE part_root (id INT NOT NULL, region TEXT NOT NULL, val TEXT, PRIMARY KEY (id, region)) PARTITION BY LIST (region)`,
		`CREATE TABLE part_east PARTITION OF part_root FOR VALUES IN ('east')`,
		`CREATE TABLE part_west PARTITION OF part_root FOR VALUES IN ('west')`,
		`INSERT INTO part_root VALUES (1, 'east', 'snap'), (2, 'west', 'snap')`,
	)
	cleanReplication(t, "slot_part", "slot_part")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 100)
	po := make(chan cluster.Position, 100)
	cfg := partitionedConfig("slot_part", []string{"id"}, "part_root")
	require.NoError(t, (&postgres.PostgreSQLDialect{}).Preflight(cfg))
	go func() { _ = (&postgres.PostgreSQLDialect{}).Ingest(ctx, cfg, nil, 0, pr, po) }()

	ingesttest.Await(t, pr, po, 20*time.Second, nil, "1", "2")

	execAll(t, `INSERT INTO part_root VALUES (3, 'east', 'cdc'), (4, 'west', 'cdc')`)
	awaitCommit(t, pr, po, 20*time.Second, "3", "4")

	execAll(t,
		`CREATE TABLE part_north PARTITION OF part_root FOR VALUES IN ('north')`,
		`INSERT INTO part_root VALUES (5, 'north', 'cdc')`,
	)
	awaitCommit(t, pr, po, 20*time.Second, "5")

	execAll(t, `UPDATE part_root SET val = 'updated' WHERE id = 3`, `DELETE FROM part_root WHERE id = 4`)
	res := awaitCommit(t, pr, po, 20*time.Second, "3", "4")
	require.False(t, entityByKey(res, "3").IsDelete())
	require.JSONEq(t, `{"id":3,"region":"east","val":"updated"}`, string(entityByKey(res, "3").Data))
	require.True(t, entityByKey(res, "4").IsDelete(), "a DELETE on a partition arrives as a tombstone")

	// Changing the partition key moves the row: Postgres logs a delete from one
	// partition and an insert into the other, in that order.
	execAll(t, `UPDATE part_root SET region = 'west' WHERE id = 1`)
	res = awaitCommit(t, pr, po, 20*time.Second, "1")
	var ops []bool
	for _, p := range res.Proposals {
		for _, e := range p.Entities {
			if string(e.Key) == "1" {
				ops = append(ops, e.IsDelete())
			}
		}
	}
	require.Equal(t, []bool{true, false}, ops, "a cross-partition move is a tombstone then an upsert")
	require.JSONEq(t, `{"id":1,"region":"west","val":"snap"}`, string(entityByKey(res, "1").Data))
}

// A partition watched on its own still reports under its own name: the
// publication names the topmost PUBLISHED ancestor.
func TestPostgresPartitionWatchedAlone(t *testing.T) {
	execAll(t,
		`DROP TABLE IF EXISTS part_solo_root`,
		`CREATE TABLE part_solo_root (id INT NOT NULL, region TEXT NOT NULL, val TEXT, PRIMARY KEY (id, region)) PARTITION BY LIST (region)`,
		`CREATE TABLE part_solo_east PARTITION OF part_solo_root FOR VALUES IN ('east')`,
		`CREATE TABLE part_solo_west PARTITION OF part_solo_root FOR VALUES IN ('west')`,
	)
	cleanReplication(t, "slot_part_solo", "slot_part_solo")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 100)
	po := make(chan cluster.Position, 100)
	cfg := partitionedConfig("slot_part_solo", []string{"id"}, "part_solo_east")
	require.NoError(t, (&postgres.PostgreSQLDialect{}).Preflight(cfg))
	go func() { _ = (&postgres.PostgreSQLDialect{}).Ingest(ctx, cfg, nil, 0, pr, po) }()
	waitForSlot(t, "slot_part_solo")

	execAll(t, `INSERT INTO part_solo_root VALUES (1, 'east', 'cdc'), (2, 'west', 'cdc')`)
	res := awaitCommit(t, pr, po, 20*time.Second, "1")
	require.False(t, res.Seen["2"], "the unwatched sibling partition is not published")
}

func TestPostgresPartitionedPreflight(t *testing.T) {
	execAll(t,
		`DROP TABLE IF EXISTS part_pf_root`,
		`CREATE TABLE part_pf_root (id INT NOT NULL, region TEXT NOT NULL, val TEXT, PRIMARY KEY (id, region)) PARTITION BY LIST (region)`,
		`CREATE TABLE part_pf_east PARTITION OF part_pf_root FOR VALUES IN ('east')`,
		`ALTER TABLE part_pf_root REPLICA IDENTITY FULL`,
	)
	d := &postgres.PostgreSQLDialect{}

	require.NoError(t, d.Preflight(partitionedConfig("pf", []string{"id", "region"}, "part_pf_root")))

	// FULL on the root never reaches the partitions, so a non-key column is
	// not carried by a partition's DELETE.
	err := d.Preflight(partitionedConfig("pf", []string{"val"}, "part_pf_root"))
	require.ErrorContains(t, err, "is partitioned, so its REPLICA IDENTITY does not reach its partitions")

	err = d.Preflight(partitionedConfig("pf", []string{"id"}, "part_pf_root", "part_pf_east"))
	require.ErrorContains(t, err, `"part_pf_east" is a partition of "part_pf_root", which this ingestable also watches`)
}

// TestPostgresRemovedPartitionReported: logical replication carries no DDL, so
// detaching or dropping a partition removes its rows with nothing in the
// stream. The worker notices the partition leave and reports it in status.
func TestPostgresRemovedPartitionReported(t *testing.T) {
	restore := postgres.SetPartitionPollIntervalForTest(200 * time.Millisecond)
	defer restore()
	execAll(t,
		`DROP TABLE IF EXISTS part_rm_root`,
		`DROP TABLE IF EXISTS part_rm_east`,
		`CREATE TABLE part_rm_root (id INT NOT NULL, region TEXT NOT NULL, val TEXT, PRIMARY KEY (id, region)) PARTITION BY LIST (region)`,
		`CREATE TABLE part_rm_east PARTITION OF part_rm_root FOR VALUES IN ('east')`,
		`CREATE TABLE part_rm_west PARTITION OF part_rm_root FOR VALUES IN ('west')`,
		`CREATE TABLE part_rm_north PARTITION OF part_rm_root FOR VALUES IN ('north')`,
		`INSERT INTO part_rm_root VALUES (1, 'east', 'snap'), (2, 'west', 'snap'), (3, 'north', 'snap')`,
	)
	t.Cleanup(func() { execAll(t, `DROP TABLE IF EXISTS part_rm_root`, `DROP TABLE IF EXISTS part_rm_east`) })
	cleanReplication(t, "slot_part_rm", "slot_part_rm")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 100)
	po := make(chan cluster.Position, 100)
	d := &postgres.PostgreSQLDialect{}
	cfg := partitionedConfig("slot_part_rm", []string{"id"}, "part_rm_root")
	require.NoError(t, d.Preflight(cfg))
	go func() { _ = d.Ingest(ctx, cfg, nil, 0, pr, po) }()
	res := ingesttest.Await(t, pr, po, 20*time.Second, nil, "1", "2", "3")

	st, err := d.Status(ctx, cfg, res.Position)
	require.NoError(t, err)
	require.Empty(t, st.RemovedPartitions, "nothing removed yet")

	execAll(t, `ALTER TABLE part_rm_root DETACH PARTITION part_rm_east`, `DROP TABLE part_rm_west`)

	var removed []cluster.RemovedPartition
	require.Eventually(t, func() bool {
		st, err := d.Status(ctx, cfg, res.Position)
		if err != nil {
			return false
		}
		removed = st.RemovedPartitions
		return len(removed) == 2
	}, 10*time.Second, 50*time.Millisecond)
	names := make([]string, 0, len(removed))
	for _, p := range removed {
		require.Equal(t, "part_rm_root", p.Table)
		require.False(t, p.DetectedAt.IsZero())
		names = append(names, p.Partition)
	}
	require.ElementsMatch(t, []string{"part_rm_east", "part_rm_west"}, names)
}
