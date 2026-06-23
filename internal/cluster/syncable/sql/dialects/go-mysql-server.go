package dialects

import (
	gosql "database/sql"
	"fmt"

	"github.com/dolthub/go-mysql-server/driver"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

type GoMySQLServerDialect struct {
	Driver *driver.Driver
}

func (d *GoMySQLServerDialect) CreateDDL(c *sql.Config) string {
	return createDDL(c)
}

func (d *GoMySQLServerDialect) DropDDL(c *sql.Config) string {
	return dropDDL(c)
}

func (d *GoMySQLServerDialect) CreateSQL(config *sql.Config) string {
	mySQL := &MySQLDialect{}
	return mySQL.CreateSQL(config)
}

func (d *GoMySQLServerDialect) CreateDeleteSQL(config *sql.Config) string {
	return (&MySQLDialect{}).CreateDeleteSQL(config)
}

func (d *GoMySQLServerDialect) CreateClearSQL(config *sql.Config, columns []string) string {
	return (&MySQLDialect{}).CreateClearSQL(config, columns)
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
	return (&MySQLDialect{}).BindArgs(values)
}
