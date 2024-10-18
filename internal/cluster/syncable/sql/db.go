package sql

import (
	"database/sql"
	"fmt"
)

type DB struct {
	dialect Dialect
	DB      *sql.DB
}

func NewDB(dialect Dialect, connectionString string) (*DB, error) {
	db, err := dialect.Open(connectionString)
	if err != nil {
		return nil, err
	}

	return &DB{dialect: dialect, DB: db}, nil
}

func (d *DB) CreateInsert(config *Config) (*Insert, error) {
	ddlString := d.dialect.CreateDDL(config)
	_, err := d.DB.Exec(ddlString)
	if err != nil {
		return nil, fmt.Errorf("ddl [%s]: %w", ddlString, err)
	}

	sqlString := d.dialect.CreateSQL(config.Table, config.Mappings)

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
