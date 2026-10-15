package postgres

import (
	"context"
	gosql "database/sql"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// partitionPollInterval is how often a streaming worker reads the partitions of
// its watched tables. A var so tests can shorten it.
var partitionPollInterval = 10 * time.Second

// maxRemovedPartitions bounds the removals a worker remembers for status: a
// retention job that drops a partition a month must not grow it forever.
const maxRemovedPartitions = 20

// partitionWatch notices partitions leaving a watched table. Logical
// replication carries no DDL, so detaching or dropping a partition removes its
// rows from the source with nothing in the stream: the rows stay in the log and
// every destination (a reconciling refresh later sweeps keyed SQL syncables
// only). The watch makes
// that divergence loud (a Warn per partition and a status entry) instead of
// silent. It lives as long as the ingestable's worker, across reconnects, so a
// partition dropped while the stream reconnects is still noticed.
type partitionWatch struct {
	mu      sync.Mutex
	known   map[string]map[uint32]string // watched table → partition oid → name
	removed []cluster.RemovedPartition
}

// partitionWatch returns the watch for the ingestable config belongs to. The
// dialect is shared by every Postgres ingestable; Ingest and Status receive the
// same *sql.Config for one ingestable, so it keys the watch.
func (d *PostgreSQLDialect) partitionWatch(config *sql.Config) *partitionWatch {
	w, _ := d.partitions.LoadOrStore(config, &partitionWatch{known: make(map[string]map[uint32]string)})
	return w.(*partitionWatch)
}

// run reads the watched tables' partitions now and every partitionPollInterval
// until ctx ends. The first read of a table is its baseline.
func (w *partitionWatch) run(ctx context.Context, db *gosql.DB, tables []string) {
	t := time.NewTicker(partitionPollInterval)
	defer t.Stop()
	for {
		for _, table := range tables {
			parts, err := readPartitions(ctx, db, table)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				zap.L().Debug("read partitions of a watched table", zap.String("table", table), zap.Error(err))
				continue
			}
			w.observe(table, parts, time.Now())
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// observe compares one read of table's partitions with the last and records
// every partition that left.
func (w *partitionWatch) observe(table string, parts map[uint32]string, at time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	prev, seen := w.known[table]
	w.known[table] = parts
	if !seen {
		return
	}
	var gone []string
	for oid, name := range prev {
		if _, ok := parts[oid]; !ok {
			gone = append(gone, name)
		}
	}
	slices.Sort(gone)
	for _, name := range gone {
		zap.L().Warn("a partition of a watched table was detached or dropped; its rows are gone from the source "+
			"with no deletes in the change stream, so they stay in committed and its destinations "+
			"(a reconciling refresh removes them from keyed SQL syncables only). "+
			"To delete them everywhere, delete the rows before detaching or dropping the partition",
			zap.String("table", table), zap.String("partition", name))
		w.removed = append(w.removed, cluster.RemovedPartition{Table: table, Partition: name, DetectedAt: at})
	}
	if n := len(w.removed); n > maxRemovedPartitions {
		w.removed = slices.Clone(w.removed[n-maxRemovedPartitions:])
	}
}

// removedPartitions returns the remembered removals, oldest first.
func (w *partitionWatch) removedPartitions() []cluster.RemovedPartition {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.removed)
}

// readPartitions returns every partition under table, at any depth, by oid.
// A table that is not partitioned has none.
func readPartitions(ctx context.Context, db *gosql.DB, table string) (map[uint32]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT relid::oid, relid::regclass::text FROM pg_partition_tree($1::regclass) WHERE relid <> $1::regclass`, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	parts := make(map[uint32]string)
	for rows.Next() {
		var oid uint32
		var name string
		if err := rows.Scan(&oid, &name); err != nil {
			return nil, err
		}
		parts[oid] = name
	}
	return parts, rows.Err()
}
