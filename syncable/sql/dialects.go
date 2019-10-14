package sql

import (
	"fmt"
	"strings"
)

var dialects = make(map[string]Dialect)

func init() {
	dialects["mysql"] = &mySQLDialect{}
	dialects["ramsql"] = &ramsqlDialect{}
}

// Dialect contains instructions for creating DDL and SQL for a specific database vendor
type Dialect interface {
	CreateDDL(config *sqlConfig) string
	CreateSQL(table string, sqlMappings []sqlMapping) string
}

// mySQLDialect is the dialect for MySQL
type mySQLDialect struct{}

// CreateDDL implements Dialect
func (d *mySQLDialect) CreateDDL(config *sqlConfig) string {
	var ddl strings.Builder
	ddl.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", config.table))
	for i, column := range config.mappings {
		ddl.WriteString(fmt.Sprintf("%s %s", column.column, column.sqlType))
		if i < len(config.mappings)-1 {
			ddl.WriteString(",")
		}
	}
	if config.primaryKey != "" {
		ddl.WriteString(fmt.Sprintf(",PRIMARY KEY (%s)", config.primaryKey))
	}
	for _, index := range config.indexes {
		ddl.WriteString(fmt.Sprintf(",INDEX %s (%s)", index.indexName, index.columnNames))
	}
	ddl.WriteString(");")

	return ddl.String()
}

// CreateSQL implements Dialect
func (d *mySQLDialect) CreateSQL(table string, sqlMappings []sqlMapping) string {
	var sql strings.Builder

	fmt.Fprintf(&sql, "INSERT INTO %s(", table)
	for i, item := range sqlMappings {
		if i == 0 {
			fmt.Fprintf(&sql, "%s", item.column)
		} else {
			fmt.Fprintf(&sql, ",%s", item.column)
		}
	}
	fmt.Fprint(&sql, ") VALUES (")
	for i := range sqlMappings {
		if i == 0 {
			fmt.Fprintf(&sql, "$%d", i+1)
		} else {
			fmt.Fprintf(&sql, ",$%d", i+1)
		}
	}
	fmt.Fprint(&sql, ") ON DUPLICATE KEY UPDATE ")
	for i, item := range sqlMappings {
		if i == 0 {
			fmt.Fprintf(&sql, "%s=$%d", item.column, i+1)
		} else {
			fmt.Fprintf(&sql, ",%s=$%d", item.column, i+1)
		}
	}

	return sql.String()
}

// ramsqlDialect is the dialect for ramsql, used for testing
type ramsqlDialect struct{}

// CreateDDL implements Dialect
func (d *ramsqlDialect) CreateDDL(config *sqlConfig) string {
	return dialects["mysql"].CreateDDL(config)
}

// CreateSQL implements Dialect
func (d *ramsqlDialect) CreateSQL(table string, sqlMappings []sqlMapping) string {
	var sql strings.Builder

	fmt.Fprintf(&sql, "INSERT INTO %s(", table)
	for i, item := range sqlMappings {
		if i == 0 {
			fmt.Fprintf(&sql, "%s", item.column)
		} else {
			fmt.Fprintf(&sql, ",%s", item.column)
		}
	}
	fmt.Fprint(&sql, ") VALUES (")
	for i := range sqlMappings {
		if i == 0 {
			fmt.Fprintf(&sql, "$%d", i+1)
		} else {
			fmt.Fprintf(&sql, ",$%d", i+1)
		}
	}
	fmt.Fprint(&sql, ")")

	return sql.String()
}
