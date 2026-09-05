package sql

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"sync"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

// The snapshot pass — the dialect-neutral half of an initial snapshot,
// mid-snapshot resume, or added-table backfill: per-table keyset
// enumeration with inline resume checkpoints, table-completion bookkeeping,
// and the optional chunked-parallel dispatch. Every dialect used to carry a
// near-verbatim copy of this; what differs per dialect is only the keyset
// batch SQL (SnapshotReader), the chunk planner (ChunkPlanner, optional),
// and how a checkpoint is encoded at this snapshot's coordinate
// (SnapshotRun.Encode).
//
// Logging contract, shared by every dialect: progress is reported by table
// and row COUNTS only. The keyset cursor itself is never logged — a natural
// primary key is often source PII (email, national id, account number) and
// these lines are Info-level and shipped to log aggregation. The resume
// cursor lives in SnapshotProgress, not the logs.

// SnapshotReader is the adapter half of the snapshot pass: one keyset batch
// of table after the cursor, in primary-key order — the dialect's SQL
// (identifier quoting, the text casts that keep snapshot and CDC payloads
// byte-identical, type categories). lastPK is the prior batch's last entity
// key (a CompositeKey); haveLastPK distinguishes "no rows yet" from "last key
// was the empty string". Returns the entities, the batch's last key, and the
// row count (a count below batchSize ends the table).
type SnapshotReader interface {
	ReadBatch(ctx context.Context, table string, spec *TopicSpec, lastPK string, haveLastPK bool, batchSize int) (entities []*cluster.Entity, lastKey string, n int, err error)
}

// ChunkPlanner is the optional adapter capability behind snapshot_readers > 1:
// splitting a table into primary-key ranges read by a pool of parallel
// readers. PlanChunks returns nil (no error) when the table cannot be split
// (composite key, no split strategy, too few rows) — the table then takes
// the single-stream path. ReadChunkBatch is ReadBatch bounded above by the
// chunk's exclusive upper key ("" = unbounded, the last chunk).
type ChunkPlanner interface {
	PlanChunks(ctx context.Context, table string, spec *TopicSpec, readers, batchSize int) (*dialectpb.TableChunkProgress, error)
	ReadChunkBatch(ctx context.Context, table string, spec *TopicSpec, lastPK string, haveLastPK bool, upper string, batchSize int) (entities []*cluster.Entity, lastKey string, n int, err error)
}

// SnapshotRun is one snapshot pass's inputs.
type SnapshotRun struct {
	Config *Config
	Reader SnapshotReader
	// Tables to enumerate, in order; those already in Progress.CompletedTables
	// are skipped.
	Tables []string
	// Progress is the caller-owned live cursor, mutated IN PLACE as batches
	// hand off, so a failed attempt resumes from exactly the last handed-off
	// batch instead of restarting the enumeration. Seed it from the durable
	// resume cursor with NewSnapshotProgress.
	Progress *dialectpb.SnapshotProgress
	// Epoch is the refresh generation stamped on every emitted row, so a
	// closing refresh-boundary marker can sweep rows this positive
	// enumeration could not re-emit.
	Epoch uint64
	// BatchSize is rows per keyset batch; Readers the parallel reader pool
	// (1 = single stream; honored only when Reader is a ChunkPlanner).
	BatchSize int
	Readers   int
	// Encode renders a checkpoint carrying progress at THIS snapshot's
	// coordinate (the dialect's position proto): the inline row checkpoints
	// and the per-table completion checkpoint.
	Encode func(progress *dialectpb.SnapshotProgress) ([]byte, error)
	// BatchHook is a test-only failure-injection seam, called before each
	// batch read so the resume-after-transient-error tests can abort a
	// snapshot mid-enumeration exactly as a dropped source connection would.
	// Nil in production.
	BatchHook func(table string, batch int) error

	Proposals chan<- *cluster.Proposal
	Positions chan<- cluster.Position
}

// RunSnapshot enumerates every not-yet-completed table in run.Tables,
// handing rows off with inline checkpoints and checkpointing each table's
// completion.
func RunSnapshot(ctx context.Context, run SnapshotRun) error {
	progress := run.Progress
	completed := make(map[string]bool, len(progress.CompletedTables))
	for _, t := range progress.CompletedTables {
		completed[t] = true
	}
	// Announce whether this is a resume or a cold start, at absolute
	// (table-level) granularity, so an operator can tell the two apart from
	// the log alone — a per-run batch counter that resets each supervisor
	// restart cannot.
	if len(completed) > 0 || len(progress.LastPkByTable) > 0 || len(progress.ChunksByTable) > 0 {
		zap.L().Info("snapshot: resuming from checkpoint",
			zap.Int("tables_complete", len(completed)),
			zap.Int("tables_resuming", len(progress.LastPkByTable)),
			zap.Int("tables_total", len(run.Tables)),
			zap.Uint64("refresh_epoch", run.Epoch))
	} else {
		zap.L().Info("snapshot: starting fresh",
			zap.Int("tables_total", len(run.Tables)),
			zap.Uint64("refresh_epoch", run.Epoch))
	}
	planner, _ := run.Reader.(ChunkPlanner)
	for _, table := range run.Tables {
		if completed[table] {
			zap.L().Info("snapshot: skipping already-completed table",
				zap.String("table", table),
			)
			continue
		}
		// Chunked-parallel dispatch: a table with a persisted chunk plan
		// ALWAYS resumes chunked (the frozen-plan contract — even if
		// snapshot_readers has since changed, including to 1). A fresh table
		// goes parallel only when the operator opted in (snapshot_readers > 1),
		// the adapter can plan, AND the planner found a split strategy;
		// everything else takes the single-stream path unchanged. A table
		// mid-flight on the SINGLE stream (last_pk_by_table) similarly keeps
		// its cursor — never converted to chunks mid-table.
		plan := progress.ChunksByTable[table]
		if plan == nil && run.Readers > 1 && planner != nil {
			if _, resuming := progress.LastPkByTable[table]; !resuming {
				spec := run.Config.SpecForTable(table)
				if spec == nil {
					return fmt.Errorf("snapshot: no topic-spec routes table %q", table)
				}
				p, err := planner.PlanChunks(ctx, table, spec, run.Readers, run.BatchSize)
				if err != nil {
					return fmt.Errorf("snapshot: table %s: %w", table, err)
				}
				if p != nil {
					plan = p
					if progress.ChunksByTable == nil {
						progress.ChunksByTable = map[string]*dialectpb.TableChunkProgress{}
					}
					progress.ChunksByTable[table] = p
				}
			}
		}
		if plan != nil {
			if err := snapshotTableParallel(ctx, run, planner, table, plan); err != nil {
				return fmt.Errorf("snapshot: table %s: %w", table, err)
			}
		} else if err := snapshotTable(ctx, run, table); err != nil {
			return fmt.Errorf("snapshot: table %s: %w", table, err)
		}
		progress.CompletedTables = append(progress.CompletedTables, table)
		delete(progress.LastPkByTable, table)
		delete(progress.ChunksByTable, table)
		completed[table] = true
		if err := emitProgress(ctx, run, progress); err != nil {
			return err
		}
		zap.L().Info("snapshot: table complete", zap.String("table", table))
	}
	return nil
}

// snapshotTable is the single-stream enumeration of one table: keyset
// batches from the table's resume cursor to the end.
func snapshotTable(ctx context.Context, run SnapshotRun, table string) error {
	// Resolve this table's topic-spec (the snapshot iterates config entries,
	// so table is one SpecForTable keys on directly). One spec for the flat
	// form.
	spec := run.Config.SpecForTable(table)
	if spec == nil {
		return fmt.Errorf("snapshot: no topic-spec routes table %q", table)
	}
	progress := run.Progress
	// lastPK, haveLastPK distinguish "no rows yet flushed" from "last flushed
	// pk was the empty string".
	lastPK, haveLastPK := progress.LastPkByTable[table]
	batchNum := 0
	totalRows := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batchNum++
		if run.BatchHook != nil {
			if err := run.BatchHook(table, batchNum); err != nil {
				return err
			}
		}
		entities, batchLastPK, count, err := run.Reader.ReadBatch(ctx, table, spec, lastPK, haveLastPK, run.BatchSize)
		if err != nil {
			return err
		}
		if count == 0 {
			// Empty table, or we've advanced past the last row.
			break
		}
		// Stamp the snapshot rows with the refresh epoch (a re-snapshot bumps
		// it; the initial snapshot is epoch 1) so a closing refresh-boundary
		// marker can sweep the rows this positive enumeration could not
		// re-emit.
		StampGeneration(entities, run.Epoch)
		// Advance the read cursor to this window, then hand the rows off with
		// inline checkpoints every SnapshotCheckpointStride rows (and on the
		// window's final row) — see handOffSnapshotWindow. Each checkpoint
		// commits atomically with the row it rides (Proposal.Position),
		// closing the effectively-once gap a separate position proposal left
		// open for snapshot rows (SourceSeq 0, so the streaming dedup can't
		// cover them).
		lastPK = batchLastPK
		haveLastPK = true
		if err := handOffSnapshotWindow(ctx, entities, table, progress, run.Encode, run.Proposals); err != nil {
			return err
		}
		totalRows += count
		zap.L().Info("snapshot: batch flushed",
			zap.String("table", table),
			zap.Int("batch", batchNum),
			zap.Int("rows_in_batch", count),
			zap.Int("rows_total", totalRows),
		)
		if count < run.BatchSize {
			// A short batch means we've reached the end.
			break
		}
	}
	return nil
}

// handOffSnapshotWindow proposes one read window row by row. An inline
// resume checkpoint (Proposal.Position) rides every stride-th row AND the
// window's final row, each carrying THAT row's key as the cursor — not the
// window's last — so a freeze mid-window resumes from the last committed
// checkpoint instead of re-proposing the whole window. Before the stride,
// only the final row carried a checkpoint, making the window an
// all-or-nothing durability cliff.
func handOffSnapshotWindow(
	ctx context.Context,
	entities []*cluster.Entity,
	table string,
	progress *dialectpb.SnapshotProgress,
	encode func(*dialectpb.SnapshotProgress) ([]byte, error),
	pr chan<- *cluster.Proposal,
) error {
	stride := SnapshotCheckpointStride
	for ri, row := range entities {
		p := &cluster.Proposal{Entities: []*cluster.Entity{row}}
		if ri == len(entities)-1 || (ri+1)%stride == 0 {
			progress.LastPkByTable[table] = string(row.Key)
			posBytes, err := encode(progress)
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

// emitProgress checkpoints the whole snapshot progress (a table just
// completed) on the position channel.
func emitProgress(ctx context.Context, run SnapshotRun, progress *dialectpb.SnapshotProgress) error {
	bs, err := run.Encode(progress)
	if err != nil {
		return err
	}
	select {
	case run.Positions <- bs:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// chunkWindow is one read window a chunk reader hands to the dispatcher, or
// (final) the signal that the chunk is exhausted.
type chunkWindow struct {
	chunk int
	rows  []*cluster.Entity
	final bool
}

// snapshotTableParallel drives one table's chunked snapshot: a reader pool
// enumerates the plan's not-yet-done chunks concurrently, each window is
// handed off IN THE DISPATCHER (one goroutine proposes, so the inline
// checkpoints stay ordered per chunk), and a chunk's completion is recorded
// in the plan. The plan is frozen: it resumes verbatim across restarts.
func snapshotTableParallel(ctx context.Context, run SnapshotRun, planner ChunkPlanner, table string, plan *dialectpb.TableChunkProgress) error {
	spec := run.Config.SpecForTable(table)
	if spec == nil {
		return fmt.Errorf("snapshot: no topic-spec routes table %q", table)
	}
	var work []int
	for i, c := range plan.Chunks {
		if !c.Done {
			work = append(work, i)
		}
	}
	if len(work) == 0 {
		return nil
	}
	pool := min(run.Readers, len(work))
	zap.L().Info("snapshot: table snapshot starting (chunked)",
		zap.String("table", table),
		zap.Int("chunks_total", len(plan.Chunks)),
		zap.Int("chunks_remaining", len(work)),
		zap.Int("readers", pool),
		zap.Uint64("refresh_epoch", run.Epoch))
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
				if err := readChunk(rctx, planner, table, spec, plan.Chunks[idx], idx, run.BatchSize, emitCh); err != nil {
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
		StampGeneration(win.rows, run.Epoch)
		if err := handOffChunkWindow(rctx, win.rows, win.chunk, plan, run.Progress, run.Encode, run.Proposals); err != nil {
			fail(err)
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

// readChunk enumerates one chunk's key range in keyset batches, handing each
// window to the dispatcher and a final marker when the range is exhausted.
func readChunk(
	ctx context.Context,
	planner ChunkPlanner,
	table string,
	spec *TopicSpec,
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
		rows, lastKey, count, err := planner.ReadChunkBatch(ctx, table, spec, lastPK, haveLastPK, c.Upper, batchSize)
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

// chunkStartCursor is where a chunk's enumeration resumes: its own last
// handed-off key if it has one, else its lower bound, else the table start.
func chunkStartCursor(c *dialectpb.ChunkCursor) (lastPK string, have bool) {
	if c.LastPk != "" {
		return c.LastPk, true
	}
	if c.Lower != "" {
		return c.Lower, true
	}
	return "", false
}

// handOffChunkWindow is handOffSnapshotWindow for a chunk: the inline
// checkpoint cursor is the chunk's, recorded in the frozen plan.
func handOffChunkWindow(
	ctx context.Context,
	rows []*cluster.Entity,
	chunk int,
	plan *dialectpb.TableChunkProgress,
	progress *dialectpb.SnapshotProgress,
	encode func(*dialectpb.SnapshotProgress) ([]byte, error),
	pr chan<- *cluster.Proposal,
) error {
	stride := SnapshotCheckpointStride
	for ri, row := range rows {
		p := &cluster.Proposal{Entities: []*cluster.Entity{row}}
		if ri == len(rows)-1 || (ri+1)%stride == 0 {
			plan.Chunks[chunk].LastPk = string(row.Key)
			posBytes, err := encode(progress)
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

// StampGeneration marks every entity with the refresh epoch it was emitted
// under, so a later refresh-boundary marker at a higher epoch can sweep
// rows a re-snapshot left behind.
func StampGeneration(entities []*cluster.Entity, epoch uint64) {
	for _, e := range entities {
		e.Generation = epoch
	}
}

// NewSnapshotProgress returns a fresh, caller-owned SnapshotProgress seeded
// from a durable resume cursor (nil for a from-scratch snapshot). The clone
// keeps the live cursor independent of the decoded checkpoint value, and
// carries the partial-backfill mark: dropping it would make a crash-resume
// treat pre-seeded sibling completions as a nearly-done FULL snapshot and
// emit the refresh marker — whose topic sweep would delete every sibling row
// the backfill never re-emits. A chunked table's FROZEN plan (with per-chunk
// cursors) resumes verbatim — never re-planned.
func NewSnapshotProgress(seed *dialectpb.SnapshotProgress) *dialectpb.SnapshotProgress {
	p := &dialectpb.SnapshotProgress{
		LastPkByTable: map[string]string{},
		ChunksByTable: map[string]*dialectpb.TableChunkProgress{},
	}
	if seed != nil {
		maps.Copy(p.LastPkByTable, seed.LastPkByTable)
		maps.Copy(p.ChunksByTable, seed.ChunksByTable)
		p.CompletedTables = append(p.CompletedTables, seed.CompletedTables...)
		p.PartialBackfill = seed.PartialBackfill
	}
	return p
}

// AddedTables returns the configured tables absent from the snapshotted
// registry, in config order (deterministic scan order) — the added-table
// backfill trigger. A pure diff: a dialect that grandfathers an empty
// registry as all-snapshotted applies that rule before calling.
func AddedTables(configured, snapshotted []string) []string {
	have := make(map[string]bool, len(snapshotted))
	for _, t := range snapshotted {
		have[t] = true
	}
	var added []string
	for _, t := range configured {
		if !have[t] {
			added = append(added, t)
		}
	}
	return added
}

// ParseBatchSize reads "batch_size" from Config.Options, falling back to the
// dialect's default for a missing, non-numeric, zero, or negative value.
func ParseBatchSize(options map[string]string, def int) int {
	if v, ok := options["batch_size"]; ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// MaxSnapshotReaders caps the per-table reader pool. The ceiling protects
// the SOURCE (each reader holds a connection and streams a range scan), not
// committed.
const MaxSnapshotReaders = 16

// ParseSnapshotReaders reads "snapshot_readers" from Config.Options: 1 (the
// single stream) when missing or invalid, clamped to MaxSnapshotReaders.
func ParseSnapshotReaders(options map[string]string) int {
	v, ok := options["snapshot_readers"]
	if !ok {
		return 1
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 1
	}
	return min(n, MaxSnapshotReaders)
}
