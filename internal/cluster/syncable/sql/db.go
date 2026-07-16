package sql

import (
	"database/sql"
	"fmt"

	"github.com/committeddb/committed/internal/cluster"
)

type DB struct {
	dialect Dialect
	DB      *sql.DB
	// connectionString is the (already ${VAR}-resolved) value handed to
	// dialect.Open — the sole input that defines this pool. Retained so
	// ValidateReplace can tell a pool-preserving re-POST from a pool-changing one
	// without touching the network.
	connectionString string
}

func NewDB(dialect Dialect, connectionString string) (*DB, error) {
	db, err := dialect.Open(connectionString)
	if err != nil {
		return nil, err
	}

	return &DB{dialect: dialect, DB: db, connectionString: connectionString}, nil
}

// ValidateReplace implements cluster.DatabaseConfigChangeValidator: the pool is
// defined entirely by the connection string handed to dialect.Open, so an
// identical connection string means an identical pool (safe to keep the existing
// handle on a re-POST), and any change — or a different database kind — means a
// new pool. Compared against the configured value both sides parsed, never the
// network.
func (d *DB) ValidateReplace(prior cluster.Database) error {
	p, ok := prior.(*DB)
	if !ok || d.connectionString != p.connectionString {
		return cluster.ErrDatabaseConnectionChanged
	}
	return nil
}

func (d *DB) CreateInsert(config *Config) (*Insert, error) {
	ddlString := d.dialect.CreateDDL(config)
	_, err := d.DB.Exec(ddlString)
	if err != nil {
		return nil, fmt.Errorf("ddl [%s]: %w", ddlString, err)
	}

	sqlString := d.dialect.CreateSQL(config)

	stmt, err := d.DB.Prepare(sqlString)
	if err != nil {
		return nil, fmt.Errorf("sql [%s]: %w", sqlString, err)
	}

	var jsonPaths []string
	for _, mapping := range config.Mappings {
		jsonPaths = append(jsonPaths, mapping.JsonPath)
	}

	return &Insert{sqlString, stmt, jsonPaths}, nil
}

func (d *DB) Close() error {
	return d.DB.Close()
}

func (d *DB) GetType() string {
	return "sql"
}
