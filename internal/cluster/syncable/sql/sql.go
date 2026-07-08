package sql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

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

// ValidateReplace implements cluster.ConfigChangeValidator: it rejects a
// re-POST whose materialized table schema differs from prior's, which
// CREATE TABLE IF NOT EXISTS would silently ignore. Returns a
// *SchemaChangeError (a cluster.RebuildRequiredError) or nil.
func (c *Syncable) ValidateReplace(prior cluster.Syncable) error {
	return validateSchemaReplace(prior, c.materializedSchema())
}

// materializedSchema is the table syncable's shape: its mappings (columns +
// types), primary key, and indexes — exactly what CreateDDL runs.
func (c *Syncable) materializedSchema() SyncableSchema {
	return schemaOf(c.config)
}

// Teardown implements cluster.Teardownable: it drops the syncable's
// destination table (DROP TABLE IF EXISTS), the destructive mirror of Init's
// CREATE. It is idempotent and reconstructable from the persisted config alone
// (only the table name + DB handle), which the delete/rebuild paths rely on.
// It never touches prepared statements or the connection pool; call Close for
// those.
func (c *Syncable) Teardown() error {
	dropString := c.dialect.DropDDL(c.config)
	if _, err := c.db.Exec(dropString); err != nil {
		return fmt.Errorf("teardown [%s]: %w", dropString, err)
	}
	// Drop the keyless syncable's dedup sidecar too — DropDDL on its name. A
	// keyed syncable has none, so this is skipped.
	if c.config.PrimaryKey == "" {
		sidecarDrop := c.dialect.DropDDL(&Config{Table: AppliedSidecarName(c.config.Table)})
		if _, err := c.db.Exec(sidecarDrop); err != nil {
			return fmt.Errorf("teardown applied-sidecar [%s]: %w", sidecarDrop, err)
		}
	}
	return nil
}

func (c *Syncable) Init() error {
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

	sqlString := c.dialect.CreateSQL(c.config)

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
		return false, err
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
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return false, rollbackErr
		}
		return false, err
	}
	zap.L().Debug("sql syncable committed")

	return true, nil
}

func (c *Syncable) SyncBatch(ctx context.Context, as []*cluster.Actual) (bool, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
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
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return false, rollbackErr
		}
		return false, err
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
	if e.IsDelete() {
		if c.delete == nil {
			return cluster.Permanent(fmt.Errorf(
				"[sql.apply] cannot honor delete for key %q: no keyColumn or primaryKey configured",
				string(e.Key)))
		}
		_, err := tx.StmtContext(ctx, c.delete.Stmt).ExecContext(ctx, string(e.Key))
		if err != nil {
			wrapped := fmt.Errorf("[sql.apply] exec [%s]: %w", c.delete.SQL, err)
			if c.dialect.IsPermanent(err) {
				return cluster.Permanent(wrapped)
			}
			return wrapped
		}
		return nil
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
			return cluster.Permanent(fmt.Errorf("jsonpath [%v]: %w", path, err))
		}
		// A typed payload carries JSON-native scalars; coerce each to the form
		// its declared sink column expects (e.g. a numeric source value mapped
		// into a TEXT column must bind as text). JsonPath and Mappings are built
		// from the same config slice in order, so index i lines up.
		values = append(values, coerceForColumn(res, c.config.Mappings[i].SQLType))
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
			wrapped := fmt.Errorf("[sql.apply] exec [%s]: %w", c.appliedMark.SQL, err)
			if c.dialect.IsPermanent(err) {
				return cluster.Permanent(wrapped)
			}
			return wrapped
		}
		if n, aerr := res.RowsAffected(); aerr == nil && n == 0 {
			return nil // already applied — replay no-op, don't re-append
		}
	}

	if _, err := tx.StmtContext(ctx, c.insert.Stmt).ExecContext(ctx, allValues...); err != nil {
		wrapped := fmt.Errorf("[sql.apply] exec [%s]: %w", c.insert.SQL, err)
		if c.dialect.IsPermanent(err) {
			return cluster.Permanent(wrapped)
		}
		return wrapped
	}
	return nil
}

func (c *Syncable) Close() error {
	// Close both prepared statements; report the first error but always
	// attempt the delete close so it does not leak when insert close fails.
	err := c.insert.Stmt.Close()
	if c.delete != nil {
		if derr := c.delete.Stmt.Close(); err == nil {
			err = derr
		}
	}
	if c.appliedMark != nil {
		if merr := c.appliedMark.Stmt.Close(); err == nil {
			err = merr
		}
	}
	return err
}
