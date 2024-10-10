package sql

import (
	gosql "database/sql"
)

type Dialect interface {
	CreateDDL(config *SQLConfig) string
	CreateSQL(table string, sqlMappings []SQLMapping) string
}

type Index struct {
	IndexName   string
	ColumnNames string // comma separated list of columns
}

type SQLMapping struct {
	JsonPath string
	Column   string
	SQLType  string
	// TODO Add a concept of an optional mapping that doesn't error if it is missing
}

type SQLConfig struct {
	SQLDB      string
	Topic      string
	Table      string
	Mappings   []SQLMapping
	Indexes    []Index
	PrimaryKey string
}

type SQLInsert struct {
	Stmt     *gosql.Stmt
	JsonPath []string
}
