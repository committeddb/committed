package dialects

import (
	gosql "database/sql"
	"fmt"

	"github.com/dolthub/go-mysql-server/driver"
	"github.com/philborlin/committed/internal/cluster/syncable/sql"
)

type GoMySQLServerDialect struct {
	Driver *driver.Driver
}

func (d *GoMySQLServerDialect) CreateDDL(c *sql.Config) string {
	return createDDL(c)
}

func (d *GoMySQLServerDialect) CreateSQL(config *sql.Config) string {
	mySQL := &MySQLDialect{}
	return mySQL.CreateSQL(config)
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
