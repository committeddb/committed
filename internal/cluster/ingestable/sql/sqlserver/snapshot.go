package sqlserver

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

// encodePosition marshals the durable checkpoint. Plain proto — this dialect
// has no legacy format to magic-byte around.
func encodePosition(version uint64, progress *dialectpb.SnapshotProgress, epoch uint64, snapshotted []string) (cluster.Position, error) {
	bs, err := proto.Marshal(&dialectpb.SQLServerPosition{
		Version:           version,
		SnapshotProgress:  progress,
		RefreshEpoch:      epoch,
		SnapshottedTables: snapshotted,
	})
	if err != nil {
		return nil, fmt.Errorf("encode position: %w", err)
	}
	return bs, nil
}

// snapshot enumerates tables into the log at the captured change-tracking
// version: the shared snapshot pass (sql.RunSnapshot) drives the
// resume/complete bookkeeping and the inline checkpoints; this dialect
// supplies the keyset batch SQL and the checkpoint encoding at version.
func (d *SQLServerDialect) snapshot(
	ctx context.Context,
	db *gosql.DB,
	config *sql.Config,
	tables []string,
	progress *dialectpb.SnapshotProgress,
	version uint64,
	epoch uint64,
	snapshotted []string,
	pr chan<- *cluster.Proposal,
	po chan<- cluster.Position,
) error {
	return sql.RunSnapshot(ctx, sql.SnapshotRun{
		Config:    config,
		Reader:    sqlserverSnapshotReader{db: db},
		Tables:    tables,
		Progress:  progress,
		Epoch:     epoch,
		BatchSize: sql.ParseBatchSize(config.Options, defaultSnapshotBatchSize),
		Readers:   1,
		Encode: func(p *dialectpb.SnapshotProgress) ([]byte, error) {
			return encodePosition(version, p, epoch, snapshotted)
		},
		BatchHook: d.snapshotBatchHook,
		Proposals: pr,
		Positions: po,
	})
}

// defaultSnapshotBatchSize is the keyset batch when Config.Options has no
// "batch_size" override. Smaller than the MySQL/Postgres default: the
// change-tracking dialect's snapshot reads are plainer OFFSET-free keyset
// scans against sources that are typically smaller.
const defaultSnapshotBatchSize = 1000

// sqlserverSnapshotReader is the dialect's snapshot adapter: one keyset
// batch per call (readBatch).
type sqlserverSnapshotReader struct{ db *gosql.DB }

func (r sqlserverSnapshotReader) ReadBatch(ctx context.Context, table string, spec *sql.TopicSpec, lastPK string, haveLastPK bool, batchSize int) ([]*cluster.Entity, string, int, error) {
	return readBatch(ctx, r.db, table, spec, lastPK, haveLastPK, batchSize)
}

// readBatch reads up to batchSize rows past the keyset cursor in one short
// read transaction, returning entities rendered through the shared decode
// path (the SAME path the CT poll uses for upserts — byte parity by
// construction).
//
// T-SQL has no row-value comparison, so a composite keyset expands to the
// nested OR form: (c1 > @1) OR (c1 = @1 AND c2 > @2) …. WHERE and ORDER BY
// reference the real table columns (never output aliases), and the driver
// converts each string-bound cursor parameter to the COLUMN's type — so a
// numeric PK compares numerically, the exact property whose absence caused
// the mixed-semantics partial-snapshot field incident on another dialect.
func readBatch(
	ctx context.Context,
	db *gosql.DB,
	table string,
	spec *sql.TopicSpec,
	lastPK string,
	haveLastPK bool,
	batchSize int,
) ([]*cluster.Entity, string, int, error) {
	ref := resolveTableRef(table)
	pkCols := spec.PrimaryKey

	// No explicit transaction: a batch is ONE SELECT, and a single statement
	// is self-consistent at READ COMMITTED — the driver rejects ReadOnly tx
	// options anyway, and the convergent contract never needed point-in-time
	// reads across batches (changes racing the snapshot re-deliver via CT).
	orderBy := make([]string, len(pkCols))
	for i, c := range pkCols {
		orderBy[i] = quoter.Ident(c)
	}

	var where string
	var args []any
	if haveLastPK {
		cursor, err := sql.DecodeCompositeCursor(lastPK, len(pkCols))
		if err != nil {
			return nil, "", 0, fmt.Errorf("decode keyset cursor: %w", err)
		}
		clauses := make([]string, len(pkCols))
		argN := 0
		for i := range pkCols {
			var parts []string
			for j := 0; j < i; j++ {
				argN++
				parts = append(parts, fmt.Sprintf("%s = @p%d", quoter.Ident(pkCols[j]), argN))
				args = append(args, cursor[j])
			}
			argN++
			parts = append(parts, fmt.Sprintf("%s > @p%d", quoter.Ident(pkCols[i]), argN))
			args = append(args, cursor[i])
			clauses[i] = "(" + strings.Join(parts, " AND ") + ")"
		}
		where = " WHERE " + strings.Join(clauses, " OR ")
	}

	//nolint:gosec // G201: identifiers pass through quoter; values are bound parameters.
	query := fmt.Sprintf("SELECT TOP (%d) * FROM %s%s ORDER BY %s",
		batchSize, ref.qualified(), where, strings.Join(orderBy, ", "))
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", 0, fmt.Errorf("read batch: %w", err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, "", 0, err
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, "", 0, err
	}
	cats, uuidCols := columnCategories(colTypes)

	var entities []*cluster.Entity
	var lastKey string
	count := 0
	for rows.Next() {
		m, err := scanRowValues(rows, cols, uuidCols)
		if err != nil {
			return nil, "", 0, err
		}
		count++
		e, key := rowEntity(spec, m, cats)
		lastKey = key
		if e != nil {
			entities = append(entities, e)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, "", 0, err
	}
	return entities, lastKey, count, nil
}

// snapshotBaseVersion captures the CT version the poll loop will resume from,
// BEFORE the snapshot's first read — every change racing the snapshot is then
// also re-delivered and converges (the shared convergent boundary, same as
// capturing the binlog position first).
func snapshotBaseVersion(ctx context.Context, db *gosql.DB) (uint64, error) {
	cctx, cancel := context.WithTimeout(ctx, statusQueryTimeout)
	defer cancel()
	var v gosql.NullInt64
	if err := db.QueryRowContext(cctx, "SELECT CHANGE_TRACKING_CURRENT_VERSION()").Scan(&v); err != nil {
		return 0, fmt.Errorf("read change tracking version: %w", err)
	}
	return ctVersion(v, "database")
}

// waitCtx sleeps d or returns early with ctx's error.
func waitCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
