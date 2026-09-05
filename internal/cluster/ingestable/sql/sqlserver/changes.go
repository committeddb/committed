package sqlserver

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

const (
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
	// epoch stays the RAW checkpoint epoch here (0 when none): the snapshot
	// decisions (sql.PlanSnapshot) choose the generation from it and the
	// sink's highwater, and the poll loop floors it before streaming.

	db, err := openSQLServer(config.ConnectionString)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	backoff := sql.NewReconnectBackoff()
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		err := d.ingestOnce(ctx, db, config, &version, &resumeProgress, &epoch, epochFloor, &snapshotted, backoff, pr, po)
		if err == nil || ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, sql.ErrPrimaryKeyColumnMissing) {
			// Permanent schema drift: a configured primaryKey column was renamed or
			// dropped at the source. The CHANGETABLE join names it, so the same
			// window fails on every retry — forever, at the backoff cap, visible
			// only in these logs. Return instead so the worker parks (freeze →
			// supervisor → committed.worker.parked), loud and observable — the
			// same exit the log-based dialects take. err (the ParkError) names the
			// affected topic and the missing columns.
			zap.L().Error("ingest stopping: a primaryKey column is missing from the source schema (renamed or dropped) — worker will park until the ingestable is re-POSTed or the column restored",
				zap.Error(err),
			)
			return err
		}
		zap.L().Warn("[sqlserver] ingest error, retrying",
			zap.Duration("backoff", backoff.Delay()), zap.Error(err))
		if backoff.Wait(ctx) != nil {
			return nil
		}
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
	epochFloor uint64,
	snapshotted *[]string,
	backoff *sql.Backoff,
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
	switch {
	case *resumeProgress != nil:
		if err := d.runSnapshot(ctx, db, config, sql.SnapshotResume, version, resumeProgress, epoch, epochFloor, snapshotted, nil, pr, po); err != nil {
			return err
		}
	case needFull:
		if err := d.runSnapshot(ctx, db, config, sql.SnapshotCold, version, resumeProgress, epoch, epochFloor, snapshotted, nil, pr, po); err != nil {
			return err
		}
	case len(added) > 0:
		if err := d.runSnapshot(ctx, db, config, sql.SnapshotBackfill, version, resumeProgress, epoch, epochFloor, snapshotted, added, pr, po); err != nil {
			return err
		}
	}
	// The stream floor: never stamp below the sink's highwater, never below 1.
	*epoch = sql.FloorEpoch(*epoch, epochFloor)

	// The attempt reached streaming (any snapshot it needed is complete): the
	// next failure is a fresh one, so the reconnect delay starts over.
	backoff.Reset()

	// The resume-time positioning line (the shared per-dialect contract): what
	// the poll resumes FROM is the first datum a re-delivery question needs.
	zap.L().Info("change tracking poll started",
		zap.Uint64("resumeVersion", *version),
		zap.Duration("pollInterval", pollInterval(config.Options)))

	// --- the poll loop ---
	interval := pollInterval(config.Options)
	drift := &schemaDriftGuard{}
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
			*resumeProgress = nil
			*snapshotted = nil
			if err := d.runSnapshot(ctx, db, config, sql.SnapshotGap, version, resumeProgress, epoch, epochFloor, snapshotted, nil, pr, po); err != nil {
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

		if err := d.pollWindow(ctx, db, config, *version, current, *epoch, drift, pr); err != nil {
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

// addedTables diffs configured tables against the snapshotted registry — the
// added-table backfill trigger — with the empty registry grandfathered as
// all-snapshotted (the pre-registry compat contract: a checkpoint written
// before the registry existed must not read as "nothing snapshotted").
func addedTables(configured, snapshotted []string) []string {
	if len(snapshotted) == 0 {
		return nil
	}
	return sql.AddedTables(configured, snapshotted)
}

// runSnapshot performs one snapshot pass of the given kind — a full
// snapshot, a mid-snapshot resume, an added-table backfill, or a gap
// re-snapshot — then closes it: the per-topic refresh-boundary markers when
// the plan carries them, and the completion checkpoint.
func (d *SQLServerDialect) runSnapshot(
	ctx context.Context,
	db *gosql.DB,
	config *sql.Config,
	kind sql.SnapshotKind,
	version *uint64,
	resumeProgress **dialectpb.SnapshotProgress,
	epoch *uint64,
	epochFloor uint64,
	snapshotted *[]string,
	added []string,
	pr chan<- *cluster.Proposal,
	po chan<- cluster.Position,
) error {
	if kind == sql.SnapshotBackfill {
		zap.L().Info("config change added tables; backfilling their existing rows (a partial backfill — no refresh sweep)",
			zap.Strings("added", added))
	}
	plan := sql.PlanSnapshot(kind, *resumeProgress, *epoch, epochFloor, config.Tables, added)
	*epoch = plan.Epoch

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

	if err := d.snapshot(ctx, db, config, config.Tables, plan.Progress, *version, *epoch, config.Tables, pr, po); err != nil {
		return err
	}

	// Completed: clear progress, record every configured table snapshotted,
	// checkpoint so a restart streams instead of re-snapshotting.
	*resumeProgress = nil
	*snapshotted = append([]string(nil), config.Tables...)
	posBytes, err := encodePosition(*version, nil, *epoch, *snapshotted)
	if err != nil {
		return err
	}
	if err := sql.CompleteSnapshot(ctx, config, plan.Marker, *epoch, posBytes, pr, po); err != nil {
		return err
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
	drift *schemaDriftGuard,
	pr chan<- *cluster.Proposal,
) error {
	sub := 0
	subOverflowed := false

	// Capture provenance is BEST-EFFORT on this dialect: change tracking is a
	// poll, so one flushed batch can span many source transactions — only a
	// batch whose rows all share one SYS_CHANGE_VERSION (txnVer > 0; the
	// steady-state small window) carries a transaction identity. Its commit
	// time comes from sys.dm_tran_commit_table, cached per window; absent when
	// the DMV row is already cleaned up or the query fails (permissions), in
	// which case the version still stamps as SourceTxnID.
	commitTimes := map[int64]int64{}
	commitLookupWarned := false
	commitTimeFor := func(ver int64) int64 {
		if t, ok := commitTimes[ver]; ok {
			return t
		}
		nanos := int64(0)
		var t time.Time
		err := db.QueryRowContext(ctx,
			`SELECT commit_time FROM sys.dm_tran_commit_table WHERE commit_ts = @p1`, ver).Scan(&t)
		switch {
		case err == nil:
			nanos = t.UnixNano()
		case !errors.Is(err, gosql.ErrNoRows) && !commitLookupWarned:
			commitLookupWarned = true
			zap.L().Debug("source commit-time lookup unavailable; provenance carries the change version only",
				zap.Error(err))
		}
		commitTimes[ver] = nanos
		return nanos
	}

	// The window's sub-index runs across every flush in it: each proposal
	// needs a distinct, strictly-increasing SourceSeq under one change-tracking
	// version. Once the sub-index overflows the packed encoding, every later
	// proposal in the window is DedupUnsafe (sticky) — the worker freezes
	// rather than risk a dedup drop.
	seq := func(p *cluster.Proposal) {
		s, ok := encodeCTSeq(current, sub)
		if !ok || subOverflowed {
			subOverflowed = true
			p.DedupUnsafe = true
		} else {
			p.SourceSeq = s
		}
		sub++
	}
	flush := func(entities []*cluster.Entity, txnVer int64) error {
		if len(entities) == 0 {
			return nil
		}
		var prov sql.Provenance
		if txnVer > 0 {
			prov.CommitUnixNano = commitTimeFor(txnVer)
			prov.TxnID = strconv.FormatInt(txnVer, 10)
		}
		// No bundled checkpoint: this dialect checkpoints per polling window
		// (after pollWindow), not per transaction, so no txn-scoped dedup either.
		_, err := sql.Flush(ctx, entities, epoch, prov, seq, nil, pr)
		return err
	}

	for _, table := range config.Tables {
		spec := config.SpecForTable(table)
		if spec == nil {
			return fmt.Errorf("no topic-spec routes table %q", table)
		}
		if err := d.pollTable(ctx, db, table, spec, consumed, current, drift, flush); err != nil {
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
	drift *schemaDriftGuard,
	flush func([]*cluster.Entity, int64) error,
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
		SELECT ct.SYS_CHANGE_OPERATION, ct.SYS_CHANGE_VERSION, %s, t.*
		FROM CHANGETABLE(CHANGES %s, @p1) AS ct
		LEFT JOIN %s AS t ON %s
		WHERE ct.SYS_CHANGE_VERSION <= @p2
		ORDER BY ct.SYS_CHANGE_VERSION`,
		strings.Join(ctPKs, ", "), ref.qualified(), ref.qualified(), strings.Join(pkJoin, " AND "))

	rows, err := db.QueryContext(ctx, query, int64(consumed), int64(current)) //nolint:gosec // G115: CT versions are non-negative bigints well under int64 max.
	if err != nil {
		// The join names every primaryKey column, so a key column renamed or
		// dropped at the source fails this query before a single row is read —
		// the reconcile below never runs. Classify by the schema fact, not the
		// error text: re-read the table's current columns and park when a key
		// column is gone; anything else (source down, the probe itself failing)
		// stays the retryable error it was.
		if perr := keyDriftPark(ctx, db, ref, spec); perr != nil {
			return perr
		}
		return fmt.Errorf("changetable query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	// The window's projection (t.*) is this dialect's schema-change boundary —
	// change tracking carries no in-band DDL signal (no TableMap event, no
	// RelationMessage) — so reconcile the config's column contract against it
	// here: a vanished mapped column diverges (renders null) and warns once; a
	// vanished key column cannot reach this point (the query fails above).
	if err := drift.reconcile(table, spec, cols); err != nil {
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
	// pendingVer / pendingMixed track whether every buffered row shares one
	// SYS_CHANGE_VERSION — i.e. whether this batch is exactly one source
	// transaction's changes to this table. Homogeneous batches carry the
	// version as their provenance identity; a mixed batch (a catch-up window
	// spanning transactions) flushes with 0 = provenance omitted.
	pendingVer := int64(0)
	pendingMixed := false
	batchVer := func() int64 {
		if pendingMixed {
			return 0
		}
		return pendingVer
	}
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
		ver, verOK := m["sys_change_version"].(int64)
		if len(pending) == 0 {
			pendingVer, pendingMixed = ver, !verOK
		} else if !verOK || ver != pendingVer {
			pendingMixed = true
		}
		pending = append(pending, e)
		pendingBytes += sql.EntityFlushBytes(e)
		if pendingBytes >= sql.TxnSoftFlushBytes {
			if err := flush(pending, batchVer()); err != nil {
				return err
			}
			pending, pendingBytes = nil, 0
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := flush(pending, batchVer()); err != nil {
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

// schemaDriftGuard reconciles the config's column contract against the
// columns each poll window observes. A window recurs every poll interval, so
// the divergence Warn is deduped to once per (table, column) per session —
// exactly as the MySQL handler dedups its per-transaction TableMap warn. One
// guard per ingestOnce session: the dialect value is the registry singleton
// shared by every ingestable and holds no per-worker state.
type schemaDriftGuard struct {
	warned map[string]bool
}

// reconcile returns the ParkError for a missing primaryKey column and Warns
// once per missing mapped column. cols are the window projection's column
// names as the driver reports them (source case; lowercased here).
func (g *schemaDriftGuard) reconcile(table string, spec *sql.TopicSpec, cols []string) error {
	observed := make(map[string]bool, len(cols))
	for _, c := range cols {
		observed[strings.ToLower(c)] = true
	}
	drift := sql.ReconcileSchema(spec, observed)
	if err := drift.ParkError(); err != nil {
		return err
	}
	for _, col := range drift.MissingMapped {
		k := table + "\x00" + col
		if g.warned[k] {
			continue
		}
		if g.warned == nil {
			g.warned = make(map[string]bool)
		}
		g.warned[k] = true
		zap.L().Warn("mapped column dropped or renamed at the source; it now renders null on the sink, which diverges from the source and must be re-snapshotted to reconcile",
			zap.String("table", table),
			zap.String("column", col),
		)
	}
	return nil
}

// keyDriftPark classifies a failed CHANGETABLE read: it re-reads the table's
// current columns and returns the ParkError when a configured primaryKey
// column is no longer among them. nil means "not key drift" — the table is
// intact, or gone entirely (a different fault, not this class), or the probe
// itself failed — and the caller keeps its original, retryable error.
func keyDriftPark(ctx context.Context, db *gosql.DB, ref tableRef, spec *sql.TopicSpec) error {
	ctx, cancel := context.WithTimeout(ctx, ctLivenessTimeout)
	defer cancel()
	cols, _, err := sourceTableColumns(ctx, db, ref)
	if err != nil || len(cols) == 0 {
		return nil
	}
	observed := make(map[string]bool, len(cols))
	for _, c := range cols {
		observed[strings.ToLower(c)] = true
	}
	return sql.ReconcileSchema(spec, observed).ParkError()
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
