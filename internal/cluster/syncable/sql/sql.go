package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"github.com/oliveagle/jsonpath"
	"github.com/philborlin/committed/internal/cluster"
)

type Syncable struct {
	db      *sql.DB
	config  *Config
	dialect Dialect
	insert  *Insert
}

func New(d *DB, config *Config) *Syncable {
	return &Syncable{db: d.DB, config: config, dialect: d.dialect}
}

func (c *Syncable) Init() error {
	ddlString := c.dialect.CreateDDL(c.config)
	_, err := c.db.Exec(ddlString)
	if err != nil {
		return fmt.Errorf("ddl [%s]: %w", ddlString, err)
	}

	sqlString := c.dialect.CreateSQL(c.config)

	stmt, err := c.db.Prepare(sqlString)
	if err != nil {
		log.Fatalf("Error Preparing sql [%s]: %v", sqlString, err)
	}

	var jsonPaths []string
	for _, mapping := range c.config.Mappings {
		jsonPaths = append(jsonPaths, mapping.JsonPath)
	}

	c.insert = &Insert{sqlString, stmt, jsonPaths}

	return nil
}

func (c *Syncable) Sync(ctx context.Context, p *cluster.Proposal) (cluster.ShouldSnapshot, error) {
	// BeginTx / ExecContext let cancellation actually interrupt the
	// transaction. Without this, a canceled worker would have to wait
	// for the current Sync to drain naturally before it could exit,
	// which on a slow destination database can be many seconds.
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}

	for _, e := range p.Entities {
		if c.config.Topic != e.Type.ID {
			return false, nil
		}
	}

	for _, e := range p.Entities {
		var jsonData any
		err := json.Unmarshal(e.Data, &jsonData)
		if err != nil {
			return false, fmt.Errorf("%v: %w", string(e.Data), err)
		}

		var values []any
		for _, path := range c.insert.JsonPath {
			res, err := jsonpath.JsonPathLookup(jsonData, path)
			if err != nil {
				return false, fmt.Errorf("parsing [%v] in [%v]: %w", path, jsonData, err)
			}
			values = append(values, res)
		}

		// We need one set for the insert and one for the on duplicate key update.
		// TODO Do all dbs need this? Should this be a dialect setting?
		allValues := append(values, values...)

		_, err = tx.StmtContext(ctx, c.insert.Stmt).ExecContext(ctx, allValues...)
		if err != nil {
			return false, fmt.Errorf("[sql.Sync] tx.Stmt [%s]: %w", c.insert.SQL, err)
		}
	}

	fmt.Printf("[sql syncable] Committing...\n")
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
	fmt.Printf("[sql syncable] ...Committed\n")

	return true, nil
}

func (c *Syncable) Close() error {
	return c.insert.Stmt.Close()
}
