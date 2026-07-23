package sql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/PaesslerAG/jsonpath"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

type Syncable struct {
	db      *sql.DB
	config  *Config
	dialect Dialect
	insert  *Insert
	// delete is the prepared DELETE-by-key statement honoring delete
	// Actuals (right-to-be-forgotten). It is nil only when the config
	// names neither keyColumn nor primaryKey, in which case a delete
	// Actual is a permanent misconfiguration rather than a silent
	// retention. See Config.DeleteKeyColumn.
	delete *Delete
	// appliedMark is the dedup-sidecar mark, non-nil ONLY for a keyless
	// (append/history) syncable — the one shape whose bare INSERT would
	// duplicate rows on replay. See AppliedMark and applyEntity.
	appliedMark *AppliedMark
	// sweep is the prepared reconciling delete run on a refresh-boundary marker
	// (see cluster.Entity.IsRefreshBoundary): DELETE the rows carrying an epoch
	// older than the refresh. Non-nil ONLY for a keyed syncable (PrimaryKey set)
	// — the shape whose keyed upsert stamps GenerationColumn on every row. It is
	// nil for keyless/append syncables, where a refresh marker is a no-op (there
	// is no current-row identity to reconcile). See Init and applyEntity.
	sweep *sql.Stmt
}

func New(d *DB, config *Config) *Syncable {
	return &Syncable{db: d.DB, config: config, dialect: d.dialect}
}

// CheckpointPolicy implements cluster.CheckpointConfigurable so the sync
// worker honors the cadence parsed from the [syncable] TOML. For this batch
// syncable, Every is the batch size and MaxAge the batch-age flush; zero
// fields fall back to the worker's batch defaults.
func (c *Syncable) CheckpointPolicy() cluster.CheckpointPolicy {
	return c.config.Checkpoint
}

// Teardown implements cluster.Teardownable: it drops the syncable's
// destination table (DROP TABLE IF EXISTS), the destructive mirror of Init's
// CREATE. It is idempotent and reconstructable from the persisted config alone
// (only the table name + DB handle), which the delete/rebuild paths rely on.
// It never touches prepared statements or the connection pool; call Close for
// those.
// teardownTimeout bounds a Teardown's DROP statements against the destination.
// Teardown is best-effort by contract (a failure logs and leaves orphaned
// state), so a generous-but-finite bound is strictly better than hanging on an
// unreachable destination.
const teardownTimeout = 10 * time.Second

func (c *Syncable) Teardown() error {
	// Self-bounded (teardownTimeout): teardown targets a destination that may be
	// the very reason the worker was torn down — a hung DROP must not run
	// unbounded. Mirrors the ingest twin (TeardownSource). The db-layer caller
	// additionally bounds the whole call (runBounded), but the ctx is what
	// actually cancels the query/pool-wait instead of leaking it.
	ctx, cancel := context.WithTimeout(context.Background(), teardownTimeout)
	defer cancel()

	dropString := c.dialect.DropDDL(c.config)
	if _, err := c.db.ExecContext(ctx, dropString); err != nil {
		return fmt.Errorf("teardown [%s]: %w", dropString, err)
	}
	// Drop the keyless syncable's dedup sidecar too — DropDDL on its name. A
	// keyed syncable has none, so this is skipped.
	if c.config.PrimaryKey == "" {
		sidecarDrop := c.dialect.DropDDL(&Config{Table: AppliedSidecarName(c.config.Table)})
		if _, err := c.db.ExecContext(ctx, sidecarDrop); err != nil {
			return fmt.Errorf("teardown applied-sidecar [%s]: %w", sidecarDrop, err)
		}
	}
	return nil
}

func (c *Syncable) Init() error {
	// If Init fails after preparing one or more statements, close them before
	// returning: the caller (the parser) discards this half-built Syncable, but
	// the statements live server-side on the shared, re-POST-preserved *sql.DB
	// pool, so without this they accumulate on the destination across every
	// restart/reconcile re-parse until it hits its prepared-statement ceiling.
	success := false
	defer func() {
		if !success {
			_ = c.closeStatements()
		}
	}()

	// Re-validate even though ParseConfig already did: directly constructed
	// configs (tests, future callers) must hit the same wall before any DDL
	// reaches the destination database.
	if err := validateMappings(c.config.Mappings); err != nil {
		return err
	}

	ddlString := c.dialect.CreateDDL(c.config)
	_, err := c.db.Exec(ddlString)
	if err != nil {
		return fmt.Errorf("ddl [%s]: %w", ddlString, err)
	}

	keyed := c.config.PrimaryKey != ""

	// A keyed sink stamps the committed-managed generation column on every upsert
	// so a refresh-boundary marker can sweep rows left at an older epoch. That
	// column is not in CreateDDL, so add it idempotently first (covering a
	// pre-feature table upgraded in place), then prepare the generation-aware
	// upsert. A keyless/append table has no keyed current row to reconcile, so it
	// keeps the plain insert and prepares no sweep.
	sqlString := c.dialect.CreateSQL(c.config)
	if keyed {
		if err := c.dialect.EnsureGenerationColumn(c.db, c.config); err != nil {
			return err
		}
		sqlString = c.dialect.CreateGenerationUpsertSQL(c.config)
	}

	stmt, err := c.db.Prepare(sqlString)
	if err != nil {
		return fmt.Errorf("prepare sql [%s]: %w", sqlString, err)
	}

	var jsonPaths []string
	for _, mapping := range c.config.Mappings {
		jsonPaths = append(jsonPaths, mapping.JsonPath)
	}

	c.insert = &Insert{sqlString, stmt, jsonPaths}

	// Prepare the DELETE-by-key statement so delete Actuals can be honored
	// without a JSON unmarshal (the delete sentinel is not a payload). Only
	// possible when a key column is known; otherwise leave it nil and let
	// applyEntity surface the misconfiguration if a delete ever arrives.
	if c.config.DeleteKeyColumn() != "" {
		deleteString := c.dialect.CreateDeleteSQL(c.config)
		deleteStmt, err := c.db.Prepare(deleteString)
		if err != nil {
			return fmt.Errorf("prepare delete sql [%s]: %w", deleteString, err)
		}
		c.delete = &Delete{deleteString, deleteStmt}
	}

	// Prepare the reconciling sweep run on a refresh-boundary marker (keyed
	// sinks only — see the sweep field and applyRefreshBoundary).
	if keyed {
		sweepSQL := c.dialect.CreateGenerationSweepSQL(c.config)
		sweepStmt, err := c.db.Prepare(sweepSQL)
		if err != nil {
			return fmt.Errorf("prepare sweep sql [%s]: %w", sweepSQL, err)
		}
		c.sweep = sweepStmt
	}

	// A keyless (append/history) syncable has no key to be idempotent on, so its
	// bare INSERT would duplicate rows on replay (crash mid-batch, leader-change
	// re-sync, corrupt-checkpoint restart). Give it a dedup sidecar keyed on
	// (raft index, entity ordinal) — a deterministic, log-derived identity — and
	// guard each history insert with a mark (see applyEntity): a replay marks a
	// no-op and skips the insert. Keyed syncables need none of this; their upsert
	// is already idempotent, so they carry no sidecar and pay nothing.
	if c.config.PrimaryKey == "" {
		sidecarDDL := c.dialect.CreateAppliedSidecarDDL(c.config)
		if _, err := c.db.Exec(sidecarDDL); err != nil {
			return fmt.Errorf("applied-sidecar ddl [%s]: %w", sidecarDDL, err)
		}
		markSQL := c.dialect.CreateAppliedMarkSQL(c.config)
		markStmt, err := c.db.Prepare(markSQL)
		if err != nil {
			return fmt.Errorf("prepare applied-mark sql [%s]: %w", markSQL, err)
		}
		c.appliedMark = &AppliedMark{markSQL, markStmt}
	}

	success = true
	return nil
}

func (c *Syncable) Sync(ctx context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	// A proposal can carry entities from several topics; apply only ours, and
	// skip the transaction entirely when none match (so a non-matching Actual
	// costs no transaction). Filtering per-entity below — rather than dropping the
	// whole Actual on the first foreign entity — is what keeps a mixed-topic
	// Actual from silently losing this topic's data. This mirrors SyncBatch.
	matched := false
	for _, e := range a.Entities {
		if c.config.Topic == e.Type.ID {
			matched = true
			break
		}
	}
	if !matched {
		return false, nil
	}

	// BeginTx / ExecContext let cancellation actually interrupt the
	// transaction. Without this, a canceled worker would have to wait
	// for the current Sync to drain naturally before it could exit,
	// which on a slow destination database can be many seconds.
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		// A failed BeginTx returns the driver's raw connect error, which embeds
		// user=/database=/host:port; redact it (as every other Sync driver call
		// is) so the replicated stuck status and any permanent dead-letter carry
		// only the classifier. Transient: a begin failure is a connection issue to
		// retry and surface as stuck, never a permanent dead-letter.
		return false, execFailure("[sql.apply] begin", err, false)
	}

	for i, e := range a.Entities {
		if c.config.Topic != e.Type.ID {
			continue // an entity from another topic in a mixed proposal — not ours
		}
		// a.Index + the entity's ordinal i is this row's dedup identity (used
		// only by a keyless syncable's applied sidecar); i is the absolute
		// position in a.Entities, so it's stable across replay.
		if err := c.applyEntity(ctx, tx, e, a.Index, i); err != nil {
			_ = tx.Rollback()
			return false, err
		}
	}

	zap.L().Debug("sql syncable committing")
	// CAVEAT: tx.Commit() does NOT take a context — database/sql does
	// not expose CommitContext. Everything before this point in Sync
	// (BeginTx, StmtContext, ExecContext) is interruptible by ctx, but
	// the commit itself can hang on the network and there is no
	// portable way to abort it. If the destination database is
	// unreachable, a worker that is being canceled by Close or by a
	// registry replace will block here until the underlying conn's
	// driver-level read deadline fires (if any) or until the conn is
	// closed externally. This is a known database/sql limitation, not
	// something we can fix without per-driver hacks.
	err = tx.Commit()
	if err != nil {
		// A deferred-constraint violation surfaces here (past the per-exec
		// RedactedError coverage) and can echo Key (col)=(value); redact it so the
		// replicated dead-letter + stuck status never carry the bound value. No
		// rollback: a failed Commit already finalized the tx and freed the
		// connection, so a Rollback now only returns ErrTxDone and would mask this
		// error.
		return false, execFailure("[sql.apply] commit", err, c.dialect.IsPermanent(err))
	}
	zap.L().Debug("sql syncable committed")

	return true, nil
}

func (c *Syncable) SyncBatch(ctx context.Context, as []*cluster.Actual) (bool, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		// Redact the raw connect error (user=/database=/host:port); transient — see
		// the matching note in Sync.
		return false, execFailure("[sql.apply] begin", err, false)
	}

	for _, a := range as {
		for i, e := range a.Entities {
			if c.config.Topic != e.Type.ID {
				continue
			}

			if err := c.applyEntity(ctx, tx, e, a.Index, i); err != nil {
				_ = tx.Rollback()
				return false, err
			}
		}
	}

	zap.L().Debug("sql syncable batch committing", zap.Int("batch_size", len(as)))
	err = tx.Commit()
	if err != nil {
		// A deferred-constraint violation surfaces here (past the per-exec
		// RedactedError coverage) and can echo Key (col)=(value); redact it so the
		// replicated dead-letter + stuck status never carry the bound value. No
		// rollback: a failed Commit already finalized the tx and freed the
		// connection, so a Rollback now only returns ErrTxDone and would mask this
		// error.
		return false, execFailure("[sql.apply] commit", err, c.dialect.IsPermanent(err))
	}
	zap.L().Debug("sql syncable batch committed", zap.Int("batch_size", len(as)))

	return true, nil
}

// applyEntity applies one entity to an open transaction: a delete removes the
// downstream row keyed by the entity's Key (right-to-be-forgotten); any other
// entity upserts the jsonpath-mapped columns. The delete binds the entity Key
// directly — the sentinel payload is never unmarshaled — and a DELETE of a row
// that was never inserted is a natural no-op, which is what makes a fresh
// syncable replaying an already-scrubbed log correct. Returns a
// cluster.Permanent error for non-retryable failures so the worker skips
// rather than retries. The caller owns the transaction (commit/rollback).
func (c *Syncable) applyEntity(ctx context.Context, tx *sql.Tx, e *cluster.Entity, index uint64, seq int) error {
	// One exhaustive switch on the entity's variant: a refresh-boundary marker
	// (no row; triggers the reconciling sweep on a keyed sink), a delete
	// tombstone, or a row upsert (the body below the switch). The default case
	// is deliberate future-proofing: a variant this binary does not implement
	// dead-letters loudly instead of being misapplied as an upsert.
	switch v := e.Variant(); v {
	case cluster.EntityVariantRefresh:
		return c.applyRefreshBoundary(ctx, tx, e)
	case cluster.EntityVariantDelete:
		if c.delete == nil {
			// Do NOT put e.Key in this message. It becomes a permanent,
			// Raft-replicated dead-letter record (recordSyncDeadLetter), and for a
			// right-to-be-forgotten delete the key IS the subject identifier being
			// erased — logging it would defeat the erasure and survive the scrub.
			// The dead-letter's syncable id + raft index already identify the row.
			return cluster.Permanent(fmt.Errorf(
				"[sql.apply] cannot honor delete: no keyColumn or primaryKey configured (topic %q)",
				c.config.Topic))
		}
		_, err := tx.StmtContext(ctx, c.delete.Stmt).ExecContext(ctx, string(e.Key))
		if err != nil {
			return execFailure(fmt.Sprintf("[sql.apply] exec [%s]", c.delete.SQL), err, c.dialect.IsPermanent(err))
		}
		return nil
	case cluster.EntityVariantRow:
		// Fall through to the row apply below.
	default:
		return cluster.Permanent(fmt.Errorf(
			"[sql.apply] entity variant %q is not supported by this binary (topic %q); upgrade the node before syncing this topic",
			v, c.config.Topic))
	}

	// Decode with UseNumber so a numeric leaf stays json.Number — its exact
	// source digits — rather than collapsing to float64 (which corrupts
	// integers above 2^53). coerceForColumn then renders it for the
	// destination column, and the whole-payload path binds the raw bytes
	// untouched regardless.
	dec := json.NewDecoder(bytes.NewReader(e.Data))
	dec.UseNumber()
	var jsonData any
	if err := dec.Decode(&jsonData); err != nil {
		return cluster.Permanent(fmt.Errorf("unmarshal entity data: %w", err))
	}

	var values []any
	for i, path := range c.insert.JsonPath {
		// A whole-payload mapping binds the raw submitted bytes (already
		// validated as JSON by the unmarshal above). Re-marshaling jsonData
		// instead would round-trip numbers through float64 — corrupting
		// integers above 2^53 — and lose key order and duplicate keys.
		if path == wholePayloadPath {
			values = append(values, string(e.Data))
			continue
		}
		res, err := jsonpath.Get(path, jsonData)
		if err != nil {
			// KNOWN LIMITATION (ambiguous classification): a jsonpath.Get failure is
			// EITHER entry-specific (the field is genuinely absent in THIS row →
			// permanent is right) OR config-shaped (a wrong-for-the-whole-topic path,
			// an operator typo, that fails EVERY row → should be transient). The path
			// is evaluated per-row, not validated at config time, so the two are
			// indistinguishable here and it stays Permanent (the same accepted
			// asymmetry as Postgres 23502; the projection sink's jsonpath.Get sites
			// share it). The clean fix is config-time jsonpath validation — see the
			// 0.8 ticket classify-config-shaped-syncable-errors.
			return cluster.Permanent(fmt.Errorf("jsonpath [%v]: %w", path, err))
		}
		// A typed payload carries JSON-native scalars; coerce each to the form
		// its declared sink column expects (e.g. a numeric source value mapped
		// into a TEXT column must bind as text). JsonPath and Mappings are built
		// from the same config slice in order, so index i lines up.
		values = append(values, coerceForColumn(res, c.config.Mappings[i].SQLType))
	}

	// A keyed sink stamps the entity's refresh epoch into the committed-managed
	// generation column — the trailing placeholder CreateGenerationUpsertSQL
	// added. Every re-emitted row of a re-snapshot carries the new epoch; a row
	// deleted at the source is never re-emitted, so it keeps its older epoch and
	// the next refresh-boundary sweep removes it. Appended after the mapped
	// values so it fills that last placeholder. Keyless syncables have no such
	// column and append nothing.
	if c.config.PrimaryKey != "" {
		values = append(values, int64(e.Generation)) //nolint:gosec // G115: a refresh epoch is a small monotonic counter, far below 2^63
	}

	// The dialect decides how values map to placeholders: MySQL repeats them
	// (INSERT ? + ON DUPLICATE KEY UPDATE ?), PostgreSQL binds them once
	// (ON CONFLICT ... EXCLUDED).
	allValues := c.dialect.BindArgs(values)

	// Keyless (append) syncable: mark this row's (index, seq) in the dedup
	// sidecar first. RowsAffected == 0 means the pair was already there — a
	// replay — so skip the non-idempotent history insert. The mark and the
	// insert share the caller's transaction, so they commit or roll back
	// together. int64(index) is safe: a raft log index never approaches 2^63.
	if c.appliedMark != nil {
		res, err := tx.StmtContext(ctx, c.appliedMark.Stmt).ExecContext(ctx, int64(index), seq) //nolint:gosec // G115: raft index is far below 2^63
		if err != nil {
			return execFailure(fmt.Sprintf("[sql.apply] exec [%s]", c.appliedMark.SQL), err, c.dialect.IsPermanent(err))
		}
		if n, aerr := res.RowsAffected(); aerr == nil && n == 0 {
			return nil // already applied — replay no-op, don't re-append
		}
	}

	if _, err := tx.StmtContext(ctx, c.insert.Stmt).ExecContext(ctx, allValues...); err != nil {
		return execFailure(fmt.Sprintf("[sql.apply] exec [%s]", c.insert.SQL), err, c.dialect.IsPermanent(err))
	}
	return nil
}

// applyRefreshBoundary reconciles the sink to a completed full refresh at the
// marker's epoch: it deletes every row still carrying an older generation — the
// rows a positive re-enumeration could not signal because they were deleted at
// the source in a lost change-data window (an RTBF-erased subject among them).
// It runs only on a keyed sink (c.sweep != nil); on a keyless/append table there
// is no current-row identity to reconcile, so the marker is a no-op. The sweep
// is idempotent, so a replayed marker (rebuild-from-0, re-sync) reproduces the
// same reconciled state.
func (c *Syncable) applyRefreshBoundary(ctx context.Context, tx *sql.Tx, e *cluster.Entity) error {
	if c.sweep == nil {
		// Keyless/append: no current-row identity, so the marker is a no-op BY
		// DESIGN. But a re-snapshot boundary (generation > 1) means an upstream full
		// refresh just recovered a source gap this sink could NOT reconcile —
		// source-side deletes in that window are not reflected here, and the recovery
		// is a manual rebuild. The ingest-side warn can't know the sink type, so
		// signal it here. The initial snapshot (generation 1) has no pre-existing
		// state to reconcile, so stay quiet.
		if e.Generation > 1 {
			zap.L().Warn("refresh boundary on a keyless/append (history) sink is a no-op: a re-snapshot recovered a source gap, but a delete in that window (an RTBF/GDPR erasure among them) was never captured, so the subject's earlier rows remain in this history with no delete. A rebuild reconstructs the captured events; it cannot recover the uncaptured delete. Erase any source-side-forgotten subject manually.",
				zap.String("topic", e.Type.ID), zap.Uint64("generation", e.Generation))
		}
		return nil // keyless/append: nothing to sweep
	}
	//nolint:gosec // G115: a refresh epoch is a small monotonic counter, far below 2^63
	if _, err := tx.StmtContext(ctx, c.sweep).ExecContext(ctx, int64(e.Generation)); err != nil {
		return execFailure("[sql.apply] generation sweep", err, c.dialect.IsPermanent(err))
	}
	return nil
}

func (c *Syncable) Close() error {
	return c.closeStatements()
}

// closeStatements closes every prepared statement this syncable built, reporting
// the first error but always attempting the rest so none leaks when one close
// fails. It tolerates a partially-initialized syncable (any statement may be nil
// if Init failed partway), so Init's error path reuses it to avoid leaking the
// statements it already prepared onto the shared, long-lived *sql.DB pool.
func (c *Syncable) closeStatements() error {
	var err error
	closeStmt := func(s *sql.Stmt) {
		if s != nil {
			if cerr := s.Close(); err == nil {
				err = cerr
			}
		}
	}
	if c.insert != nil {
		closeStmt(c.insert.Stmt)
	}
	if c.delete != nil {
		closeStmt(c.delete.Stmt)
	}
	if c.appliedMark != nil {
		closeStmt(c.appliedMark.Stmt)
	}
	closeStmt(c.sweep)
	return err
}
