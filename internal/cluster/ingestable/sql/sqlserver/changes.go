package sqlserver

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

const (
	backoffMin = 1 * time.Second
	backoffMax = 30 * time.Second

	// ctSeqSubBits is the SourceSeq sub-counter width: SourceSeq =
	// version<<16 | sub. The CT version increments per source transaction, so
	// 2^47 versions (the headroom the shift leaves in uint64) is beyond any
	// real database's lifetime; sub orders the flushes emitted at one poll
	// window's shared version coordinate. Overflowing either bound marks
	// proposals DedupUnsafe — the worker freezes rather than risk a dedup
	// collision silently dropping data (the shared freeze contract).
	ctSeqSubBits = 16
	ctSeqSubMax  = 1<<ctSeqSubBits - 1
	ctSeqVerMax  = 1<<(64-ctSeqSubBits) - 1
)

// encodeCTSeq packs (version, sub) into the strictly-monotonic SourceSeq the
// ingest dedup requires. ok is false when either bound overflows.
func encodeCTSeq(version uint64, sub int) (seq uint64, ok bool) {
	if version > ctSeqVerMax || sub < 0 || sub > ctSeqSubMax {
		return 0, false
	}
	return version<<ctSeqSubBits | uint64(sub), true
}

// Ingest runs the worker: ensure Change Tracking (owner-side enablement with
// ownership markers), snapshot when needed, then the CT poll loop. Transient
// source errors back off and retry; a retention purge past the consumed
// version triggers a loud automatic re-snapshot at a bumped refresh epoch.
func (d *SQLServerDialect) Ingest(ctx context.Context, config *sql.Config, pos cluster.Position, epochFloor uint64, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	config.EnsureTopics()

	var version uint64
	var resumeProgress *dialectpb.SnapshotProgress
	var epoch uint64
	var snapshotted []string
	if len(pos) > 0 {
		posProto := &dialectpb.SQLServerPosition{}
		if err := proto.Unmarshal(pos, posProto); err != nil {
			return fmt.Errorf("[sqlserver] decode resume position: %w", err)
		}
		version = posProto.Version
		resumeProgress = posProto.SnapshotProgress
		epoch = posProto.RefreshEpoch
		snapshotted = posProto.SnapshottedTables
	}
	// Never stamp below a generation already on the sink; a genuine first
	// snapshot starts at epoch 1 (the shared floor contract).
	epoch = max(epoch, epochFloor, 1)

	db, err := openSQLServer(config.ConnectionString)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	backoff := backoffMin
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		err := d.ingestOnce(ctx, db, config, &version, &resumeProgress, &epoch, &snapshotted, pr, po)
		if err == nil || ctx.Err() != nil {
			return nil
		}
		zap.L().Warn("[sqlserver] ingest error, retrying",
			zap.Duration("backoff", backoff), zap.Error(err))
		if werr := waitCtx(ctx, backoff); werr != nil {
			return nil
		}
		backoff = min(backoff*2, backoffMax)
	}
}

// ingestOnce is one supervised attempt: enablement, snapshot-if-needed, then
// the poll loop until an error (returned for backoff) or ctx cancel. The
// cursor state lives in the caller's pointers so a retry resumes exactly
// where the durable checkpoints left off.
func (d *SQLServerDialect) ingestOnce(
	ctx context.Context,
	db *gosql.DB,
	config *sql.Config,
	version *uint64,
	resumeProgress **dialectpb.SnapshotProgress,
	epoch *uint64,
	snapshotted *[]string,
	pr chan<- *cluster.Proposal,
	po chan<- cluster.Position,
) error {
	if err := ensureChangeTracking(ctx, db, config); err != nil {
		return err
	}

	// --- snapshot on first run, mid-snapshot resume, or added-table backfill ---
	// Never-ran is version 0 AND an empty snapshotted registry: CT versions
	// start at 0 on a fresh database, so the version alone can't mean "no
	// checkpoint" — a completed snapshot on an idle fresh source checkpoints
	// at version 0 WITH a populated registry, and must stream, not re-run.
	needFull := *version == 0 && len(*snapshotted) == 0 && *resumeProgress == nil
	added := addedTables(config.Tables, *snapshotted)
	if needFull || *resumeProgress != nil || len(added) > 0 {
		if err := d.runSnapshot(ctx, db, config, version, resumeProgress, epoch, snapshotted, needFull, added, pr, po); err != nil {
			return err
		}
	}

	// The resume-time positioning line (the shared per-dialect contract): what
	// the poll resumes FROM is the first datum a re-delivery question needs.
	zap.L().Info("change tracking poll started",
		zap.Uint64("resumeVersion", *version),
		zap.Duration("pollInterval", pollInterval(config.Options)))

	// --- the poll loop ---
	interval := pollInterval(config.Options)
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		current, minValid, err := readCTState(ctx, db, config)
		if err != nil {
			return err
		}
		if minValid > *version {
			// Retention purged changes we never consumed — streaming can never
			// close the gap. Loud automatic recovery: full re-snapshot at a
			// bumped epoch so the closing markers sweep rows deleted in the
			// purged window (the shared purge-hole contract).
			zap.L().Error("change tracking retention purged past the consumed version — re-snapshotting",
				zap.Uint64("consumedVersion", *version),
				zap.Uint64("minValidVersion", minValid))
			*epoch = max(*epoch, 1) + 1
			*resumeProgress = nil
			*snapshotted = nil
			if err := d.runSnapshot(ctx, db, config, version, resumeProgress, epoch, snapshotted, true, nil, pr, po); err != nil {
				return err
			}
			continue
		}
		if current <= *version {
			if err := waitCtx(ctx, interval); err != nil {
				return nil
			}
			continue
		}

		if err := d.pollWindow(ctx, db, config, *version, current, *epoch, pr); err != nil {
			return err
		}

		// Checkpoint the window: all tables consumed through `current`.
		posBytes, err := encodePosition(current, nil, *epoch, *snapshotted)
		if err != nil {
			return err
		}
		select {
		case po <- posBytes:
		case <-ctx.Done():
			return nil
		}
		*version = current
	}
}

// addedTables returns configured tables missing from the snapshotted
// registry — tables a config re-POST added after the last snapshot, whose
// history must be backfilled (their ongoing changes already flow: the poll
// reads every configured table).
func addedTables(configured, snapshotted []string) []string {
	if len(snapshotted) == 0 {
		return nil // pre-registry state: grandfathered as all-snapshotted
	}
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

// runSnapshot performs a full snapshot, a mid-snapshot resume, or an
// added-table backfill, then (full/resumed-full only) closes with one
// refresh-boundary marker per topic and checkpoints the completed state.
func (d *SQLServerDialect) runSnapshot(
	ctx context.Context,
	db *gosql.DB,
	config *sql.Config,
	version *uint64,
	resumeProgress **dialectpb.SnapshotProgress,
	epoch *uint64,
	snapshotted *[]string,
	full bool,
	added []string,
	pr chan<- *cluster.Proposal,
	po chan<- cluster.Position,
) error {
	progress := newSnapshotProgress(*resumeProgress)
	tables := config.Tables

	switch {
	case *resumeProgress != nil:
		// Mid-snapshot resume: keep the recorded shape (full or backfill).
	case full:
		progress.PartialBackfill = false
	case len(added) > 0:
		// Backfill: pre-seed every sibling as complete so only the added
		// tables scan, and mark partial so NO markers are emitted — a
		// topic-scoped sweep would delete the sibling rows this snapshot never
		// re-emits (the shared partial-backfill contract).
		zap.L().Info("config change added tables; backfilling their existing rows (a partial backfill — no refresh sweep)",
			zap.Strings("added", added))
		progress.PartialBackfill = true
		for _, t := range tables {
			isAdded := false
			for _, a := range added {
				if a == t {
					isAdded = true
				}
			}
			if !isAdded {
				progress.CompletedTables = append(progress.CompletedTables, t)
			}
		}
	}

	// The convergent boundary: capture the poll's resume version BEFORE the
	// first snapshot read. A mid-snapshot resume keeps the version its
	// checkpoint recorded.
	if *resumeProgress == nil {
		v, err := snapshotBaseVersion(ctx, db)
		if err != nil {
			return err
		}
		*version = v
	}

	if err := d.snapshot(ctx, db, config, tables, progress, *version, *epoch, config.Tables, pr, po); err != nil {
		return err
	}

	if !progress.PartialBackfill {
		for ti := range config.Topics {
			spec := &config.Topics[ti]
			if spec.Type == nil {
				continue
			}
			marker := cluster.NewRefreshBoundaryEntity(spec.Type, *epoch)
			select {
			case pr <- &cluster.Proposal{Entities: []*cluster.Entity{marker}}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	// Completed: clear progress, record every configured table snapshotted,
	// checkpoint so a restart streams instead of re-snapshotting.
	*resumeProgress = nil
	*snapshotted = append([]string(nil), config.Tables...)
	posBytes, err := encodePosition(*version, nil, *epoch, *snapshotted)
	if err != nil {
		return err
	}
	select {
	case po <- posBytes:
	case <-ctx.Done():
		return ctx.Err()
	}
	zap.L().Info("snapshot complete",
		zap.Uint64("version", *version), zap.Uint64("refresh_epoch", *epoch))
	return nil
}

// pollWindow reads every table's changes in (consumed, current] and emits
// them as per-topic proposals at the window's version coordinate. Change rows
// above `current` (committed while iterating) are excluded — they belong to
// the next window — while the base-table join lawfully reads values NEWER
// than the window: a newer value re-emits in its own window and keyed
// last-write-wins converges (the net-change contract). A changed row missing
// from the base table was deleted after changing — skipped here, because its
// own delete entry keys a tombstone in this or a later window.
func (d *SQLServerDialect) pollWindow(
	ctx context.Context,
	db *gosql.DB,
	config *sql.Config,
	consumed, current uint64,
	epoch uint64,
	pr chan<- *cluster.Proposal,
) error {
	sub := 0
	subOverflowed := false
	flush := func(entities []*cluster.Entity) error {
		if len(entities) == 0 {
			return nil
		}
		stampGeneration(entities, epoch)
		for _, group := range sql.PartitionByTopic(entities) {
			p := &cluster.Proposal{Entities: group}
			seq, ok := encodeCTSeq(current, sub)
			if !ok || subOverflowed {
				// The freeze contract: a coordinate we cannot encode uniquely
				// must not be silently dedup-dropped.
				subOverflowed = true
				p.DedupUnsafe = true
			} else {
				p.SourceSeq = seq
			}
			sub++
			select {
			case pr <- p:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}

	for _, table := range config.Tables {
		spec := config.SpecForTable(table)
		if spec == nil {
			return fmt.Errorf("no topic-spec routes table %q", table)
		}
		if err := d.pollTable(ctx, db, table, spec, consumed, current, flush); err != nil {
			return fmt.Errorf("poll %s: %w", table, err)
		}
	}
	return nil
}

// pollTable streams one table's CHANGETABLE window through the flush
// callback, soft-flushing on the shared byte budget so a large catch-up
// window never materializes wholesale in memory.
func (d *SQLServerDialect) pollTable(
	ctx context.Context,
	db *gosql.DB,
	table string,
	spec *sql.TopicSpec,
	consumed, current uint64,
	flush func([]*cluster.Entity) error,
) error {
	ref := resolveTableRef(table)

	pkJoin := make([]string, len(spec.PrimaryKey))
	ctPKs := make([]string, len(spec.PrimaryKey))
	for i, c := range spec.PrimaryKey {
		pkJoin[i] = fmt.Sprintf("ct.%s = t.%s", quoter.Ident(c), quoter.Ident(c))
		ctPKs[i] = fmt.Sprintf("ct.%s AS %s", quoter.Ident(c), quoter.Ident("__ct_pk_"+c))
	}

	//nolint:gosec // G201: identifiers pass through quoter; values are bound parameters.
	query := fmt.Sprintf(`
		SELECT ct.SYS_CHANGE_OPERATION, %s, t.*
		FROM CHANGETABLE(CHANGES %s, @p1) AS ct
		LEFT JOIN %s AS t ON %s
		WHERE ct.SYS_CHANGE_VERSION <= @p2
		ORDER BY ct.SYS_CHANGE_VERSION`,
		strings.Join(ctPKs, ", "), ref.qualified(), ref.qualified(), strings.Join(pkJoin, " AND "))

	rows, err := db.QueryContext(ctx, query, int64(consumed), int64(current)) //nolint:gosec // G115: CT versions are non-negative bigints well under int64 max.
	if err != nil {
		return fmt.Errorf("changetable query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	cats, uuidCols := columnCategories(colTypes)

	var pending []*cluster.Entity
	pendingBytes := 0
	rowCount := 0
	for rows.Next() {
		m, err := scanRowValues(rows, cols, uuidCols)
		if err != nil {
			return err
		}
		rowCount++

		op, _ := m["sys_change_operation"].(string)
		var e *cluster.Entity
		if strings.EqualFold(op, "D") {
			// Delete: key from the CT-carried PK columns (the base row is gone).
			keyVals := make(map[string]any, len(spec.PrimaryKey))
			for _, c := range spec.PrimaryKey {
				keyVals[strings.ToLower(c)] = stringifyKeyValue(m["__ct_pk_"+strings.ToLower(c)])
			}
			key := sql.CompositeKey(keyVals, spec.PrimaryKey)
			e = cluster.NewDeleteEntity(spec.Type, []byte(key))
		} else {
			// Insert/update: the base row's current values. A missing base row
			// (all-nil PK from the LEFT JOIN) was deleted after this change —
			// its delete entry tombstones it; skip the stale upsert.
			if m[strings.ToLower(spec.PrimaryKey[0])] == nil {
				continue
			}
			e, _ = rowEntity(spec, m, cats)
			if e == nil {
				continue
			}
		}
		pending = append(pending, e)
		pendingBytes += sql.EntityFlushBytes(e)
		if pendingBytes >= sql.TxnSoftFlushBytes {
			if err := flush(pending); err != nil {
				return err
			}
			pending, pendingBytes = nil, 0
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := flush(pending); err != nil {
		return err
	}
	if rowCount > 0 {
		zap.L().Info("change tracking window applied",
			zap.String("table", table),
			zap.Int("changes", rowCount),
			zap.Uint64("throughVersion", current))
	}
	return nil
}

// rowEntity renders one base-table row as an upsert entity through the shared
// decode path — the single rendering both snapshot and poll use, which is
// what makes their payloads byte-identical. Returns (nil, key) for a row
// whose payload cannot marshal (skipped, logged), keeping the keyset cursor
// advancing per SCANNED row.
func rowEntity(spec *sql.TopicSpec, m map[string]any, cats map[string]sql.JSONCategory) (*cluster.Entity, string) {
	keyVals := make(map[string]any, len(spec.PrimaryKey))
	for _, c := range spec.PrimaryKey {
		keyVals[strings.ToLower(c)] = stringifyKeyValue(m[strings.ToLower(c)])
	}
	key := sql.CompositeKey(keyVals, spec.PrimaryKey)

	toJSON := sql.BuildEntityJSON(spec.Mappings, m, cats)
	jsonBytes, err := json.Marshal(toJSON)
	if err != nil {
		zap.L().Warn("skipping row with unmarshalable data", zap.Error(err))
		return nil, key
	}
	return &cluster.Entity{Type: spec.Type, Key: []byte(key), Data: jsonBytes}, key
}

// stringifyKeyValue renders a driver-typed value into the stable text form
// entity keys use — ONE function for the snapshot, upsert, and delete paths,
// so the same row always keys identically (the keyed-convergence
// requirement). []byte becomes string(b) (not fmt's byte-slice rendering);
// uniqueidentifier was already canonicalized by scanRowValues.
func stringifyKeyValue(v any) string {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return fmt.Sprintf("%v", v)
}
