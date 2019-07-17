package types

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"    // mysql driver
	_ "github.com/proullon/ramsql/driver" // ramsql driver
)

// Database represents a query database
type Database interface {
	Init() error
	Type() string
}

// SQLDB represents an sql query database
type SQLDB struct {
	driver           string
	connectionString string
	DB               *sql.DB
}

// NewSQLDB creates a new SQLDB
func NewSQLDB(driver string, connectionString string) *SQLDB {
	return &SQLDB{driver: driver, connectionString: connectionString}
}

// Init implements Database
func (d *SQLDB) Init() error {
	// TODO If the connection string is the same lets cache and return. Also let's check if it is active and allow
	// successive calls to Init() to refresh closed db connections.
	// Maybe, but what happens if the syncable closes and we try to clean up a shared database connection? Sounds bad...
	db, err := sql.Open(d.driver, d.connectionString)
	if err != nil {
		return err
	}
	d.DB = db

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
