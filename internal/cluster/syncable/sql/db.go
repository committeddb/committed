package sql

import (
	"database/sql"
	"fmt"
)

// DB represents an sql query database
type DB struct {
	dialect Dialect
	// driver           string
	// connectionString string
	DB *sql.DB
	// Insert *SQLInsert
}

func NewDB(dialect Dialect, connectionString string) (*DB, error) {
	db, err := dialect.Open(connectionString)
	if err != nil {
		return nil, err
	}

	return &DB{dialect: dialect, DB: db}, nil
}

// NewDB creates a new SQLDB
// func NewDB(dialect Dialect, driver string, connectionString string) *DB {
// 	return &DB{dialect: dialect, driver: driver, connectionString: connectionString}
// }

// func (d *DB) Init() error {
// 	// TODO If the connection string is the same lets cache and return. Also let's check if it is active and allow
// 	// successive calls to Init() to refresh closed db connections.
// 	// Maybe, but what happens if the syncable closes and we try to clean up a shared database connection? Sounds bad...
// 	db, err := sql.Open(d.driver, d.connectionString)
// 	if err != nil {
// 		return err
// 	}
// 	d.DB = db

// 	return nil
// }

func (d *DB) CreateInsert(config *Config) (*Insert, error) {
	sqlString := d.dialect.CreateSQL(config.Table, config.Mappings)

	stmt, err := d.DB.Prepare(sqlString)
	if err != nil {
		return nil, fmt.Errorf("error preparing sql [%s]: %w", sqlString, err)
	}

	var jsonPaths []string
	for _, mapping := range config.Mappings {
		jsonPaths = append(jsonPaths, mapping.JsonPath)
	}

	return &Insert{stmt, jsonPaths}, nil
}

func (d *DB) Close() error {
	return d.DB.Close()
}
