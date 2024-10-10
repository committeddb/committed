package sql

import (
	gosql "database/sql"
)

type Dialect interface {
	CreateDDL(config *Config) string
	CreateSQL(table string, sqlMappings []Mapping) string
}

type Index struct {
	IndexName   string
	ColumnNames string // comma separated list of columns - why isn't this a slice?
}

type Mapping struct {
	JsonPath string
	Column   string
	SQLType  string
	// TODO Add a concept of an optional mapping that doesn't error if it is missing
}

type Config struct {
	SQLDB      string
	Topic      string
	Table      string
	Mappings   []Mapping
	Indexes    []Index
	PrimaryKey string
}

type Insert struct {
	Stmt     *gosql.Stmt
	JsonPath []string
}
