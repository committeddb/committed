package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/oliveagle/jsonpath"
	"github.com/philborlin/committed/internal/cluster"
)

type Syncable struct {
	d       *DB
	config  *SQLConfig
	dialect Dialect
	insert  *SQLInsert
}

func New(d *DB, dialect Dialect, config *SQLConfig) *Syncable {
	return &Syncable{d: d, config: config, dialect: dialect}
}

func (c *Syncable) Init() error {
	sqlString := c.dialect.CreateSQL(c.config.Table, c.config.Mappings)

	stmt, err := c.d.DB.Prepare(sqlString)
	if err != nil {
		log.Fatalf("Error Preparing sql [%s]: %v", sqlString, err)
	}

	var jsonPaths []string
	for _, mapping := range c.config.Mappings {
		jsonPaths = append(jsonPaths, mapping.JsonPath)
	}

	c.insert = &SQLInsert{stmt, jsonPaths}

	return nil
}

func (c *Syncable) Sync(ctx context.Context, p *cluster.Proposal) error {
	tx, err := c.d.DB.Begin()
	if err != nil {
		return err
	}

	for _, e := range p.Entities {
		var jsonData any
		err := json.Unmarshal(e.Data, &jsonData)
		if err != nil {
			return fmt.Errorf("%v: %w", string(e.Data), err)
		}

		var values []any
		for _, path := range c.insert.JsonPath {
			res, err := jsonpath.JsonPathLookup(jsonData, path)
			if err != nil {
				return fmt.Errorf("parsing [%v] in [%v]: %w", path, jsonData, err)
			}
			values = append(values, res)
		}

		_, err = tx.Stmt(c.insert.Stmt).Exec(values...)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return rollbackErr
		}
		return err
	}

	return nil
}

func (c *Syncable) Close() error {
	return nil
}
