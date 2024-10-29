package sql

import "database/sql"

type DB struct {
	dialect Dialect
	DB      *sql.DB
}

func NewDB(dialect Dialect, connectionString string) (*DB, error) {
	// db, err := dialect.Open(connectionString)
	// if err != nil {
	// 	return nil, err
	// }

	// return &DB{dialect: dialect, DB: db}, nil
	return nil, nil
}

func (d *DB) Close() error {
	return nil
}

func (d *DB) GetType() string {
	return "sql"
}
