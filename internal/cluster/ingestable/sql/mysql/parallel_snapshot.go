package mysql

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
	"github.com/committeddb/committed/internal/cluster/sqlident"
)

// The chunked parallel snapshot: one large table is split into PK ranges
// (chunks) read by N concurrent readers, so wall-clock scales with reader
// count instead of a single stream's round-trip latency. The design keeps
// every invariant of the single-stream path by construction:
//
//   - Readers only READ (the expensive part: the batch query, the phase-2
//     JSON type resolution, entity building). A single EMITTER serializes
//     their windows onto the proposal channel, and it alone mutates the
//     chunk cursors — so every inline checkpoint (Proposal.Position)
//     describes exactly the rows handed off before it in channel order.
//     Cursor-ahead-of-rows (the data-loss direction) is impossible; a crash
//     re-reads at most each chunk's tail window (at-least-once, same
//     contract as the single stream).
//   - The chunk plan is FROZEN: persisted inside every inline checkpoint and
//     reused verbatim on resume (see dialectpb.SnapshotProgress). The reader
//     count is just the pool size and may change across restarts.
//   - Rows carry the same epoch stamping, canonicalization, and byte
//     rendering as the single stream — chunk readers call the same readBatch.
//
// Default reader count is 1 (the single-stream path, byte-for-byte the
// pre-0.8.0 behavior): the snapshot target is usually someone's production
// replica, so parallelism is an explicit operator opt-in via the ingestable's
// options: snapshot_readers = "4".

// maxSnapshotReaders caps the per-table reader pool. The ceiling protects the
// SOURCE (each reader holds a connection and streams a range scan), not
// committed.
const maxSnapshotReaders = 16

// parseSnapshotReaders reads "snapshot_readers" from Config.Options.
// Missing/invalid values fall back to 1 (single stream); values above the
// cap clamp to it.
func parseSnapshotReaders(options map[string]string) int {
	v, ok := options["snapshot_readers"]
	if !ok {
		return 1
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 1
	}
	return min(n, maxSnapshotReaders)
}

// batchPredicate builds the keyset-pagination clause shared by the batch row
// read and the phase-2 JSON type query: FROM … [WHERE …] ORDER BY pk LIMIT n.
// A single PK column compares `c > ?`; a composite uses the row-value form
// `(c1, c2) > (?, ?)` — MySQL coerces the bound string cursor values to each
// column's type. lastPK (when haveLastPK) is the exclusive lower cursor;
// upper (when non-empty) is the inclusive chunk upper bound — both in the
// composite-key encoding (a bare value for a single column).
func batchPredicate(table string, pkCols []string, lastPK string, haveLastPK bool, upper string, batchSize int) (string, []any, error) {
	orderCols := make([]string, len(pkCols))
	cols := make([]string, len(pkCols))
	placeholders := make([]string, len(pkCols))
	for i, c := range pkCols {
		orderCols[i] = sqlident.MySQL.Ident(c) + " ASC"
		cols[i] = sqlident.MySQL.Ident(c)
		placeholders[i] = "?"
	}
	rowExpr := fmt.Sprintf("(%s)", strings.Join(cols, ", "))
	valExpr := fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))

	var conds []string
	var args []any
	if haveLastPK {
		cursor, err := sql.DecodeCompositeCursor(lastPK, len(pkCols))
		if err != nil {
			return "", nil, err
		}
		conds = append(conds, fmt.Sprintf("%s > %s", rowExpr, valExpr))
		for _, v := range cursor {
			args = append(args, v)
		}
	}
	if upper != "" {
		bound, err := sql.DecodeCompositeCursor(upper, len(pkCols))
		if err != nil {
			return "", nil, err
		}
		conds = append(conds, fmt.Sprintf("%s <= %s", rowExpr, valExpr))
		for _, v := range bound {
			args = append(args, v)
		}
	}
	where := ""
	if len(conds) > 0 {
		where = " WHERE " + strings.Join(conds, " AND ")
	}
	return fmt.Sprintf(
		"FROM %s%s ORDER BY %s LIMIT %d",
		sqlident.MySQL.Table(table), where, strings.Join(orderCols, ", "), batchSize,
	), args, nil
}

// planTableChunks computes a fresh chunk plan for table, or (nil, nil) when
// the table should stay on the single-stream path: a composite PK, a PK type
// without a split strategy, a size estimate too small to be worth splitting,
// or an empty table. Split strategies (v1):
//
//   - integer PKs (tinyint…bigint, signed or unsigned): equal-width ranges
//     between MIN and MAX via big-integer arithmetic (no overflow traps at
//     the BIGINT UNSIGNED extremes). Sparse ranges just finish early.
//   - binary PKs (binary/varbinary — UUID-as-BINARY(16) being the common
//     case): byte-wise interpolation between MIN and MAX. Bounds compare
//     bytewise, which is exactly MySQL's binary-type ordering.
//
// A CHAR/VARCHAR PK deliberately has NO strategy: its ORDER BY follows the
// column collation, and arithmetic bounds computed from bytes can disagree
// with collation order — a chunk boundary that disagrees with ORDER BY loses
// rows silently. Single stream is slower and correct.
func planTableChunks(
	ctx context.Context,
	db *gosql.DB,
	table string,
	spec *sql.TopicSpec,
	readers int,
	batchSize int,
) (*dialectpb.TableChunkProgress, error) {
	if len(spec.PrimaryKey) != 1 || readers < 2 {
		return nil, nil
	}
	pkCol := spec.PrimaryKey[0]
	schemaArg, bareTable := splitQualifiedTable(table)

	var dataType string
	var estRows int64
	err := db.QueryRowContext(ctx, `
		SELECT LOWER(c.DATA_TYPE), COALESCE(t.TABLE_ROWS, 0)
		FROM information_schema.COLUMNS c
		JOIN information_schema.TABLES t
		  ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
		WHERE c.TABLE_SCHEMA = COALESCE(NULLIF(?, ''), DATABASE())
		  AND c.TABLE_NAME = ? AND c.COLUMN_NAME = ?`,
		schemaArg, bareTable, pkCol,
	).Scan(&dataType, &estRows)
	if err != nil {
		// Planning is best-effort: an info-schema hiccup falls back to the
		// single stream rather than failing the snapshot.
		zap.L().Warn("snapshot: chunk planning skipped (information_schema)",
			zap.String("table", table), zap.Error(err))
		return nil, nil
	}

	// TABLE_ROWS is an estimate; it only gates "is splitting worth it". A
	// table that fits in a couple of batches gains nothing from concurrency.
	chunks := min(readers, int(estRows/int64(batchSize)))
	if chunks < 2 {
		return nil, nil
	}

	var bounds []string
	switch dataType {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
		bounds, err = integerSplitPoints(ctx, db, table, pkCol, chunks)
	case "binary", "varbinary":
		bounds, err = binarySplitPoints(ctx, db, table, pkCol, chunks)
	default:
		return nil, nil // no strategy for this PK type — single stream
	}
	if err != nil {
		return nil, err
	}
	if bounds == nil {
		return nil, nil // empty table, or min==max — single stream
	}

	plan := &dialectpb.TableChunkProgress{}
	for i := 0; i <= len(bounds); i++ {
		c := &dialectpb.ChunkCursor{}
		if i > 0 {
			c.Lower = bounds[i-1]
		}
		if i < len(bounds) {
			c.Upper = bounds[i]
		}
		plan.Chunks = append(plan.Chunks, c)
	}
	zap.L().Info("snapshot: table chunk plan frozen",
		zap.String("table", table),
		zap.Int("chunks", len(plan.Chunks)),
		zap.Int64("estimated_rows", estRows))
	return plan, nil
}

// integerSplitPoints returns chunks-1 interior boundaries between MIN(pk) and
// MAX(pk), as decimal strings (the single-column composite-key encoding).
// big.Int arithmetic handles the full signed and unsigned BIGINT domains.
func integerSplitPoints(ctx context.Context, db *gosql.DB, table, pkCol string, chunks int) ([]string, error) {
	var minS, maxS gosql.NullString
	//nolint:gosec // G201: only sqlident-quoted identifiers (backticks doubled) are interpolated; no values.
	q := fmt.Sprintf("SELECT CAST(MIN(%s) AS CHAR), CAST(MAX(%s) AS CHAR) FROM %s",
		sqlident.MySQL.Ident(pkCol), sqlident.MySQL.Ident(pkCol), sqlident.MySQL.Table(table))
	if err := db.QueryRowContext(ctx, q).Scan(&minS, &maxS); err != nil {
		return nil, fmt.Errorf("chunk plan: min/max: %w", err)
	}
	if !minS.Valid || !maxS.Valid {
		return nil, nil // empty table
	}
	lo, ok1 := new(big.Int).SetString(strings.TrimSpace(minS.String), 10)
	hi, ok2 := new(big.Int).SetString(strings.TrimSpace(maxS.String), 10)
	if !ok1 || !ok2 || lo.Cmp(hi) >= 0 {
		return nil, nil
	}
	span := new(big.Int).Sub(hi, lo)
	if span.Cmp(big.NewInt(int64(chunks))) < 0 {
		return nil, nil // fewer distinct values than chunks
	}
	bounds := make([]string, 0, chunks-1)
	for i := 1; i < chunks; i++ {
		b := new(big.Int).Mul(span, big.NewInt(int64(i)))
		b.Div(b, big.NewInt(int64(chunks))).Add(b, lo)
		bounds = append(bounds, b.String())
	}
	return bounds, nil
}

// binarySplitPoints returns chunks-1 interior boundaries between MIN(pk) and
// MAX(pk) for a binary PK, interpolating over the first 8 bytes (padded).
// The boundary strings carry the raw bytes — the single-column composite-key
// encoding — and compare bytewise on the source, matching binary-type ORDER
// BY. Uniformly-distributed keys (UUIDs) split near-evenly; skewed ones just
// make uneven chunks, which finish early.
func binarySplitPoints(ctx context.Context, db *gosql.DB, table, pkCol string, chunks int) ([]string, error) {
	var minB, maxB []byte
	//nolint:gosec // G201: only sqlident-quoted identifiers (backticks doubled) are interpolated; no values.
	q := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s",
		sqlident.MySQL.Ident(pkCol), sqlident.MySQL.Ident(pkCol), sqlident.MySQL.Table(table))
	if err := db.QueryRowContext(ctx, q).Scan(&minB, &maxB); err != nil {
		return nil, fmt.Errorf("chunk plan: min/max: %w", err)
	}
	if minB == nil || maxB == nil || string(minB) >= string(maxB) {
		return nil, nil
	}
	const prefixLen = 8
	loU, hiU := binaryPrefixUint(minB, prefixLen), binaryPrefixUint(maxB, prefixLen)
	nChunks := uint64(chunks) //nolint:gosec // G115: chunks is in [2, maxSnapshotReaders]
	if hiU-loU < nChunks {
		return nil, nil
	}
	width := len(minB)
	if len(maxB) > width {
		width = len(maxB)
	}
	bounds := make([]string, 0, chunks-1)
	for i := 1; i < chunks; i++ {
		v := loU + (hiU-loU)/nChunks*uint64(i) //nolint:gosec // G115: i is in [1, maxSnapshotReaders)
		b := make([]byte, width)
		for j := 0; j < prefixLen && j < width; j++ {
			b[j] = byte(v >> (8 * (prefixLen - 1 - j))) //nolint:gosec // G115: deliberate byte extraction
		}
		bounds = append(bounds, string(b))
	}
	return bounds, nil
}

// binaryPrefixUint reads the first n bytes of b as a big-endian integer,
// zero-padding short values (MySQL pads BINARY comparisons the same way).
func binaryPrefixUint(b []byte, n int) uint64 {
	var v uint64
	for i := 0; i < n; i++ {
		v <<= 8
		if i < len(b) {
			v |= uint64(b[i])
		}
	}
	return v
}

// chunkWindow is one read window a chunk reader hands to the emitter: the
// rows (already entity-built and canonicalized by readBatch), or a final
// marker (rows nil) declaring the chunk exhausted.
type chunkWindow struct {
	chunk int
	rows  []*cluster.Entity
	final bool
}

// snapshotTableParallel drives one table's chunked snapshot: a reader pool
// over the plan's unfinished chunks, one emitter serializing their windows
// onto pr with inline checkpoints. plan is already installed in
// progress.ChunksByTable[table]; the caller removes it and marks the table
// complete when this returns nil.
func snapshotTableParallel(
	ctx context.Context,
	db *gosql.DB,
	config *sql.Config,
	table string,
	batchSize int,
	readers int,
	plan *dialectpb.TableChunkProgress,
	progress *dialectpb.SnapshotProgress,
	pos *mysql.Position,
	gtid string,
	epoch uint64,
	pr chan<- *cluster.Proposal,
) error {
	spec := config.SpecForTable(table)
	if spec == nil {
		return fmt.Errorf("snapshot: no topic-spec routes table %q", table)
	}

	// Work list: unfinished chunks only (a resume skips done ones).
	var work []int
	for i, c := range plan.Chunks {
		if !c.Done {
			work = append(work, i)
		}
	}
	if len(work) == 0 {
		return nil
	}
	pool := min(readers, len(work))
	zap.L().Info("snapshot: table snapshot starting (chunked)",
		zap.String("table", table),
		zap.Int("chunks_total", len(plan.Chunks)),
		zap.Int("chunks_remaining", len(work)),
		zap.Int("readers", pool),
		zap.Uint64("refresh_epoch", epoch))

	rctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workCh := make(chan int)
	emitCh := make(chan chunkWindow, pool)

	var wg sync.WaitGroup
	var readerErrMu sync.Mutex
	var readerErr error
	fail := func(err error) {
		readerErrMu.Lock()
		if readerErr == nil {
			readerErr = err
		}
		readerErrMu.Unlock()
		cancel()
	}

	for w := 0; w < pool; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				var idx int
				select {
				case i, ok := <-workCh:
					if !ok {
						return
					}
					idx = i
				case <-rctx.Done():
					return
				}
				if err := readChunk(rctx, db, table, spec, plan.Chunks[idx], idx, batchSize, emitCh); err != nil {
					if rctx.Err() == nil {
						fail(fmt.Errorf("chunk %d: %w", idx, err))
					}
					return
				}
			}
		}()
	}
	go func() {
		defer close(emitCh)
		defer wg.Wait()
		for _, idx := range work {
			select {
			case workCh <- idx:
			case <-rctx.Done():
				close(workCh)
				return
			}
		}
		close(workCh)
	}()

	// The emitter: the ONLY goroutine that mutates the chunk cursors, in
	// hand-off order — so every encoded inline checkpoint describes exactly
	// the rows already sent ahead of it.
	remaining := len(work)
	for win := range emitCh {
		if win.final {
			plan.Chunks[win.chunk].Done = true
			remaining--
			zap.L().Info("snapshot: chunk complete",
				zap.String("table", table),
				zap.Int("chunk", win.chunk),
				zap.Int("chunks_remaining", remaining))
			continue
		}
		stampGeneration(win.rows, epoch)
		if err := handOffChunkWindow(rctx, win.rows, win.chunk, plan, progress, *pos, gtid, epoch, pr); err != nil {
			fail(err)
			// Drain so the reader pool can exit; fail() already canceled rctx.
			for range emitCh { //nolint:revive // draining to unblock senders
			}
			break
		}
	}

	readerErrMu.Lock()
	defer readerErrMu.Unlock()
	if readerErr != nil {
		return readerErr
	}
	return ctx.Err()
}

// readChunk reads one chunk to exhaustion, sending each window (and a final
// marker) to the emitter. It keeps a LOCAL cursor — the durable cursor is the
// emitter's, advanced per hand-off; on a crash the chunk resumes from the
// last inline checkpoint and re-reads at most its tail window.
func readChunk(
	ctx context.Context,
	db *gosql.DB,
	table string,
	spec *sql.TopicSpec,
	c *dialectpb.ChunkCursor,
	idx int,
	batchSize int,
	emitCh chan<- chunkWindow,
) error {
	lastPK, haveLastPK := chunkStartCursor(c)
	windows := 0
	totalRows := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, lastKey, count, err := readBatch(ctx, db, table, spec, lastPK, haveLastPK, c.Upper, batchSize)
		if err != nil {
			return err
		}
		if count > 0 {
			windows++
			totalRows += count
			lastPK, haveLastPK = lastKey, true
			select {
			case emitCh <- chunkWindow{chunk: idx, rows: rows}:
			case <-ctx.Done():
				return ctx.Err()
			}
			// No last_pk in the log: a natural PK is often source PII.
			zap.L().Info("snapshot: chunk window read",
				zap.String("table", table),
				zap.Int("chunk", idx),
				zap.Int("window", windows),
				zap.Int("rows_in_window", count),
				zap.Int("rows_total", totalRows))
		}
		if count < batchSize {
			select {
			case emitCh <- chunkWindow{chunk: idx, final: true}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// chunkStartCursor picks where a chunk's read begins: its durable cursor
// when one was checkpointed (resume), else its exclusive lower bound, else
// unbounded (the table-start chunk).
func chunkStartCursor(c *dialectpb.ChunkCursor) (lastPK string, have bool) {
	if c.LastPk != "" {
		return c.LastPk, true
	}
	if c.Lower != "" {
		return c.Lower, true
	}
	return "", false
}

// handOffChunkWindow is handOffSnapshotWindow's chunked twin: one single-row
// proposal per row, with an inline resume checkpoint riding every stride-th
// row and the window's final row. The cursor written is THIS chunk's, inside
// the full frozen plan, so the persisted blob always carries every chunk's
// durable position.
func handOffChunkWindow(
	ctx context.Context,
	rows []*cluster.Entity,
	chunk int,
	plan *dialectpb.TableChunkProgress,
	progress *dialectpb.SnapshotProgress,
	pos mysql.Position,
	gtid string,
	epoch uint64,
	pr chan<- *cluster.Proposal,
) error {
	stride := sql.SnapshotCheckpointStride
	for ri, row := range rows {
		p := &cluster.Proposal{Entities: []*cluster.Entity{row}}
		if ri == len(rows)-1 || (ri+1)%stride == 0 {
			plan.Chunks[chunk].LastPk = string(row.Key)
			posBytes, err := encodeProgress(pos, gtid, progress, epoch)
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
	return nil
}
