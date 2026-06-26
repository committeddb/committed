package mysql

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// columnInfo is committed's own per-column metadata for the binlog decode path —
// the parts the raw binlog row does not carry. The binlog stream delivers
// positionally-decoded values with no column names or member lists, so committed
// sources this from information_schema (see schemaCache) rather than relying on
// canal's tracking. enumValues is non-nil only for an ENUM column, setValues only
// for a SET column; which one is populated is the column's kind.
type columnInfo struct {
	name       string           // lower-cased column name
	cat        sql.JSONCategory // how the column's value renders as JSON
	enumValues []string         // ENUM member labels in definition order; nil otherwise
	setValues  []string         // SET member labels in definition order; nil otherwise
}

// tableSchema is a table's columns in ordinal (binlog row image) order.
type tableSchema struct {
	cols []columnInfo
}

// schemaCache lazily loads and caches per-table column metadata for the binlog
// decode path. All access is from the single event-processing goroutine (OnRow
// loads, OnDDL clears), so it needs no locking.
type schemaCache struct {
	db     *gosql.DB
	byName map[string]*tableSchema // key: lower-cased table name
}

func newSchemaCache(db *gosql.DB) *schemaCache {
	return &schemaCache{db: db, byName: map[string]*tableSchema{}}
}

// get returns the cached schema for a table, loading it on first use. A load
// error is returned and not cached, so the next call retries.
func (c *schemaCache) get(ctx context.Context, table string) (*tableSchema, error) {
	key := strings.ToLower(table)
	if ts, ok := c.byName[key]; ok {
		return ts, nil
	}
	ts, err := loadTableSchema(ctx, c.db, table)
	if err != nil {
		return nil, err
	}
	c.byName[key] = ts
	return ts, nil
}

// clear drops every cached schema. Called on DDL: a schema change may have
// altered any watched table, and the next row reloads the current columns.
func (c *schemaCache) clear() {
	c.byName = map[string]*tableSchema{}
}

// loadTableSchema reads a table's columns, in ordinal order, from
// information_schema — the same metadata canal sourced from SHOW FULL COLUMNS, so
// the decoded payload is identical. The JSON category comes from data_type; ENUM
// and SET member lists are parsed from column_type.
func loadTableSchema(ctx context.Context, db *gosql.DB, table string) (*tableSchema, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT column_name, data_type, column_type
		FROM information_schema.columns
		WHERE table_schema = DATABASE()
		  AND table_name = ?
		ORDER BY ordinal_position`, table)
	if err != nil {
		return nil, fmt.Errorf("load schema of %q: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	var ts tableSchema
	for rows.Next() {
		var name, dataType, columnType string
		if err := rows.Scan(&name, &dataType, &columnType); err != nil {
			return nil, err
		}
		ci := columnInfo{
			name: strings.ToLower(name),
			cat:  mysqlCategoryForTypeName(dataType),
		}
		switch strings.ToLower(dataType) {
		case "enum":
			ci.enumValues = parseEnumSetMembers(columnType, "enum(")
		case "set":
			ci.setValues = parseEnumSetMembers(columnType, "set(")
		}
		ts.cols = append(ts.cols, ci)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &ts, nil
}

// parseEnumSetMembers extracts the quoted member list from a MySQL ENUM/SET
// column_type — parseEnumSetMembers("enum('red','green','blue')", "enum(") →
// ["red","green","blue"]. It mirrors go-mysql/canal's own naive parse (strip the
// prefix and trailing ")", drop single quotes, split on comma) so the decoded
// labels are byte-identical to what canal produced. Naive on members that
// contain a comma or quote — a deliberate fidelity match, locked by tests.
func parseEnumSetMembers(columnType, prefix string) []string {
	inner := strings.TrimSuffix(strings.TrimPrefix(columnType, prefix), ")")
	return strings.Split(strings.ReplaceAll(inner, "'", ""), ",")
}
