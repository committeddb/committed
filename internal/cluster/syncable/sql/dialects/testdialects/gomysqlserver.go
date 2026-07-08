package testdialects

import (
	gosql "database/sql"
	"fmt"

	"github.com/dolthub/go-mysql-server/driver"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

type GoMySQLServerDialect struct {
	Driver *driver.Driver
}

func (d *GoMySQLServerDialect) CreateDDL(c *sql.Config) string {
	return (&dialects.MySQLDialect{}).CreateDDL(c)
}

func (d *GoMySQLServerDialect) DropDDL(c *sql.Config) string {
	return (&dialects.MySQLDialect{}).DropDDL(c)
}

func (d *GoMySQLServerDialect) CreateSQL(config *sql.Config) string {
	return (&dialects.MySQLDialect{}).CreateSQL(config)
}

func (d *GoMySQLServerDialect) CreateDeleteSQL(config *sql.Config) string {
	return (&dialects.MySQLDialect{}).CreateDeleteSQL(config)
}

func (d *GoMySQLServerDialect) CreateClearSQL(config *sql.Config, columns []string) string {
	return (&dialects.MySQLDialect{}).CreateClearSQL(config, columns)
}

func (d *GoMySQLServerDialect) CreateAggregateSidecarDDL(spec sql.AggregateSpec) string {
	return (&dialects.MySQLDialect{}).CreateAggregateSidecarDDL(spec)
}

func (d *GoMySQLServerDialect) CreateAggregateMaterializeSQL(spec sql.AggregateSpec) string {
	return (&dialects.MySQLDialect{}).CreateAggregateMaterializeSQL(spec)
}

func (d *GoMySQLServerDialect) CreateAggregateRebuildSQL(spec sql.AggregateSpec) string {
	return (&dialects.MySQLDialect{}).CreateAggregateRebuildSQL(spec)
}

func (d *GoMySQLServerDialect) CreateAggregateParentLookupSQL(spec sql.AggregateSpec) string {
	return (&dialects.MySQLDialect{}).CreateAggregateParentLookupSQL(spec)
}

func (d *GoMySQLServerDialect) CreateLookupDimensionDDL(spec sql.LookupSpec) string {
	return (&dialects.MySQLDialect{}).CreateLookupDimensionDDL(spec)
}

func (d *GoMySQLServerDialect) CreateAggregateAffectedParentsSQL(spec sql.AggregateSpec, onField string) string {
	return (&dialects.MySQLDialect{}).CreateAggregateAffectedParentsSQL(spec, onField)
}

func (d *GoMySQLServerDialect) Open(connectionString string) (*gosql.DB, error) {
	conn, err := d.Driver.OpenConnector(connectionString)
	if err != nil {
		return nil, err
	}

	db := gosql.OpenDB(conn)

	_, err = db.Exec("USE " + connectionString)
	if err != nil {
		return nil, fmt.Errorf("[GoMySQLServerDialect] use: %w", err)
	}

	return db, nil
}

// IsPermanent always returns false. GoMySQLServerDialect is in-process
// test infrastructure (dolthub/go-mysql-server), so errors surface as
// vitess SQLErrors rather than go-sql-driver MySQLErrors — classifying
// them would require a parallel error table that test code does not
// exercise. Treating everything as retryable matches SQLMockDialect.
func (d *GoMySQLServerDialect) IsPermanent(err error) bool {
	return false
}

// BindArgs mirrors MySQLDialect: CreateSQL delegates to it, so the same
// value doubling applies.
func (d *GoMySQLServerDialect) BindArgs(values []any) []any {
	return (&dialects.MySQLDialect{}).BindArgs(values)
}
