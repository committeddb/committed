package sql

import (
	"database/sql"

	"github.com/philborlin/committed/internal/node/syncable"
	"github.com/spf13/viper"
)

type dbParser struct{}

func (p *dbParser) Parse(v *viper.Viper) (syncable.Database, error) {
	driver := v.GetString("database.sql.dialect")
	connectionString := v.GetString("database.sql.connectionString")
	db := NewDB(driver, connectionString)
	return db, nil
}

func init() {
	syncable.RegisterDBParser("sql", &dbParser{})
}

// DB represents an sql query database
type DB struct {
	dialect          Dialect
	driver           string
	connectionString string
	DB               *sql.DB
}

// NewDB creates a new SQLDB
func NewDB(driver string, connectionString string) *DB {
	return &DB{dialect: dialects[driver], driver: driver, connectionString: connectionString}
}

// Init implements Database
func (d *DB) Init() error {
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
func (d *DB) Type() string {
	return "sql"
}

// Close implements Database
func (d *DB) Close() error {
	return d.DB.Close()
}

// Open returns a db connection
func (d *DB) Open() (*sql.DB, error) {
	// In the future we probably want to reuse connections
	return sql.Open(d.driver, d.connectionString)
}
