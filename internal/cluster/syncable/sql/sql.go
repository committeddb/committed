package sql

import (
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
}

func New(d *DB, config *Config) *Syncable {
	return &Syncable{db: d.DB, config: config, dialect: d.dialect}
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

	return nil
}

func (c *Syncable) Sync(ctx context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	// BeginTx / ExecContext let cancellation actually interrupt the
	// transaction. Without this, a canceled worker would have to wait
	// for the current Sync to drain naturally before it could exit,
	// which on a slow destination database can be many seconds.
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}

	for _, e := range a.Entities {
		if c.config.Topic != e.Type.ID {
			return false, nil
		}
	}

	for _, e := range a.Entities {
		if err := c.applyEntity(ctx, tx, e); err != nil {
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
		for _, e := range a.Entities {
			if c.config.Topic != e.Type.ID {
				continue
			}

			if err := c.applyEntity(ctx, tx, e); err != nil {
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
func (c *Syncable) applyEntity(ctx context.Context, tx *sql.Tx, e *cluster.Entity) error {
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

	var jsonData any
	if err := json.Unmarshal(e.Data, &jsonData); err != nil {
		return cluster.Permanent(fmt.Errorf("unmarshal entity data: %w", err))
	}

	var values []any
	for _, path := range c.insert.JsonPath {
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
		values = append(values, res)
	}

	// The dialect decides how values map to placeholders: MySQL repeats them
	// (INSERT ? + ON DUPLICATE KEY UPDATE ?), PostgreSQL binds them once
	// (ON CONFLICT ... EXCLUDED).
	allValues := c.dialect.BindArgs(values)

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
	return err
}
