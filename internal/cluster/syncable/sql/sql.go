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

	sqlString := c.dialect.CreateSQL(c.config.Table, c.config.Mappings)

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
	tx, err := c.db.Begin()
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

		_, err = tx.Stmt(c.insert.Stmt).Exec(allValues...)
		if err != nil {
			return false, fmt.Errorf("[sql.Sync] tx.Stmt [%s]: %w", c.insert.SQL, err)
		}
	}

	fmt.Printf("[sql syncable] Committing...\n")
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
