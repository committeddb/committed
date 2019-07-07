package types

import (
	"database/sql"

	"github.com/cznic/ql"
)

// Database represents a query database
type Database interface {
	Init() error
	Type() string
}

// SQLDB represents an sql query database
type SQLDB struct {
	Name             string
	driver           string
	connectionString string
}

// NewSQLDB creates a new SQLDB
func NewSQLDB(name string, driver string, connectionString string) *SQLDB {
	return &SQLDB{Name: name, driver: driver, connectionString: connectionString}
}

// Init implements Database
func (d *SQLDB) Init() error {
	if d.driver == "ql" {
		ql.RegisterDriver()
	}

	return nil
}

// Type implements Database
func (d *SQLDB) Type() string {
	return "sql"
}

// Open returns a db connection
func (d *SQLDB) Open() (*sql.DB, error) {
	// In the future we probably want to reuse connections
	return sql.Open(d.driver, d.connectionString)
}
