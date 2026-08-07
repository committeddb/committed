package sqlserver

import (
	"context"
	gosql "database/sql"
	"fmt"
	"maps"
	"strings"
	"time"

	"go.uber.org/zap"
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

// newSnapshotProgress seeds a mutable progress cursor from a durable one
// (nil-safe), carrying the partial-backfill mark — dropping it would let a
// crash mid-backfill resume as a FULL refresh whose closing marker sweeps the
// sibling rows the backfill never re-emits (the shared propagation contract).
func newSnapshotProgress(seed *dialectpb.SnapshotProgress) *dialectpb.SnapshotProgress {
	p := &dialectpb.SnapshotProgress{LastPkByTable: map[string]string{}}
	if seed != nil {
		maps.Copy(p.LastPkByTable, seed.LastPkByTable)
		p.CompletedTables = append(p.CompletedTables, seed.CompletedTables...)
		p.PartialBackfill = seed.PartialBackfill
	}
	return p
}

// stampGeneration sets the reconciling-refresh epoch on every entity in a
// batch (same helper as the other dialects; per-dialect because it touches
// the dialect's batch shapes).
func stampGeneration(entities []*cluster.Entity, epoch uint64) {
	for _, e := range entities {
		e.Generation = epoch
	}
}

// snapshot dumps existing rows from the given tables with keyset pagination —
// one short read transaction per batch, per-table resume progress, inline
// checkpoints riding the proposals (Proposal.Position, the shared
// effectively-once contract for SourceSeq-0 snapshot rows). The CT version was
// captured BEFORE the first read, so every change racing the snapshot is also
// re-delivered by the poll loop — the shared convergent contract.
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
	batchSize := batchSizeOption(config.Options)

	completed := make(map[string]bool, len(progress.CompletedTables))
	for _, t := range progress.CompletedTables {
		completed[t] = true
	}

	// Table-level resume-vs-fresh announcement; the keyset cursor itself is
	// never logged (a natural PK is often source PII — the shared logging
	// contract).
	if len(completed) > 0 || len(progress.LastPkByTable) > 0 {
		zap.L().Info("snapshot: resuming from checkpoint",
			zap.Int("tables_complete", len(completed)),
			zap.Int("tables_resuming", len(progress.LastPkByTable)),
			zap.Int("tables_total", len(tables)),
			zap.Uint64("refresh_epoch", epoch))
	} else {
		zap.L().Info("snapshot: starting fresh",
			zap.Int("tables_total", len(tables)),
			zap.Uint64("refresh_epoch", epoch))
	}

	for _, table := range tables {
		if completed[table] {
			zap.L().Info("snapshot: skipping already-completed table", zap.String("table", table))
			continue
		}
		if err := d.snapshotTable(ctx, db, config, table, batchSize, progress, version, epoch, snapshotted, pr); err != nil {
			return fmt.Errorf("snapshot: table %s: %w", table, err)
		}
		progress.CompletedTables = append(progress.CompletedTables, table)
		delete(progress.LastPkByTable, table)
		completed[table] = true

		posBytes, err := encodePosition(version, progress, epoch, snapshotted)
		if err != nil {
			return err
		}
		select {
		case po <- posBytes:
		case <-ctx.Done():
			return ctx.Err()
		}
		zap.L().Info("snapshot: table complete", zap.String("table", table))
	}
	return nil
}

// snapshotTable reads one table in keyset-paginated batches, handing rows off
// as single-row proposals with inline stride checkpoints.
func (d *SQLServerDialect) snapshotTable(
	ctx context.Context,
	db *gosql.DB,
	config *sql.Config,
	table string,
	batchSize int,
	progress *dialectpb.SnapshotProgress,
	version uint64,
	epoch uint64,
	snapshotted []string,
	pr chan<- *cluster.Proposal,
) error {
	spec := config.SpecForTable(table)
	if spec == nil {
		return fmt.Errorf("no topic-spec routes table %q", table)
	}
	lastPK, haveLastPK := progress.LastPkByTable[table]

	batchNum := 0
	totalRows := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batchNum++
		if d.snapshotBatchHook != nil {
			if err := d.snapshotBatchHook(table, batchNum); err != nil {
				return err
			}
		}

		entities, batchLastPK, count, err := readBatch(ctx, db, table, spec, lastPK, haveLastPK, batchSize)
		if err != nil {
			return err
		}
		if count == 0 {
			break
		}
		stampGeneration(entities, epoch)

		lastPK = batchLastPK
		haveLastPK = true

		stride := sql.SnapshotCheckpointStride
		for ri, row := range entities {
			p := &cluster.Proposal{Entities: []*cluster.Entity{row}}
			if ri == len(entities)-1 || (ri+1)%stride == 0 {
				progress.LastPkByTable[table] = string(row.Key)
				posBytes, err := encodePosition(version, progress, epoch, snapshotted)
				if err != nil {
					return err
				}
				p.Position = posBytes
			}
			select {
			case pr <- p:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		totalRows += count

		zap.L().Info("snapshot: batch flushed",
			zap.String("table", table),
			zap.Int("batch", batchNum),
			zap.Int("rows_in_batch", count),
			zap.Int("rows_total", totalRows))

		if count < batchSize {
			break
		}
	}
	return nil
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

// batchSizeOption reads the snapshot batch size, defaulting to the shared
// convention (mirrors the other dialects' parseBatchSize).
func batchSizeOption(options map[string]string) int {
	if v := options["batch_size"]; v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			return n
		}
	}
	return 1000
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
