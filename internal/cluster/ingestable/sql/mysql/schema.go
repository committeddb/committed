package mysql

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// columnInfo is committed's per-column metadata for the binlog decode path — the
// parts the positional row image does not carry (name, JSON category, and the
// ENUM/SET member lists needed to resolve the binlog's numeric encoding to
// labels). enumValues is non-nil only for an ENUM column, setValues only for a
// SET column.
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

// columnsFromTableMap builds the decode metadata from the binlog TableMapEvent —
// the schema as it existed when the row image was WRITTEN — rather than from live
// information_schema. This is what keeps an old-image row (one still replaying
// while the source has already run an ALTER) joined to the columns it was written
// under, not the post-ALTER columns: reading live information_schema silently
// mis-assigns values across a schema change (ADD COLUMN ... AFTER, DROP, rename).
//
// It requires binlog_row_metadata=FULL (MySQL 8.0.1+): without it the event
// carries no column names or ENUM/SET labels, so the function errors and the
// caller surfaces "enable FULL" rather than decoding against a schema it can't
// see. Preflight enforces FULL, so this error is defence in depth.
func columnsFromTableMap(tm *replication.TableMapEvent) (*tableSchema, error) {
	// len(ColumnType) == ColumnCount (go-mysql slices it to that width) and is an
	// int already — no uint64→int conversion for gosec to flag.
	n := len(tm.ColumnType)

	// Column names are populated iff binlog_row_metadata=FULL — that's the FULL
	// check (there is no dedicated flag). Under FULL there is exactly one name per
	// column; otherwise the slice is empty.
	names := tm.ColumnNameString()
	if len(names) != n {
		return nil, fmt.Errorf(
			"binlog table map for %s.%s carries %d column names for %d columns; set binlog_row_metadata=FULL",
			tm.Schema, tm.Table, len(names), n)
	}

	// go-mysql pre-aligns ENUM/SET labels to the GLOBAL column index (resolving
	// ENUM/SET that the wire disguises as MYSQL_TYPE_STRING), so committed needs
	// no enum-column counter. Both maps are nil when the table has no such column;
	// indexing a nil map is safe.
	enums := tm.EnumStrValueMap()
	sets := tm.SetStrValueMap()

	cols := make([]columnInfo, n)
	for i := 0; i < n; i++ {
		ci := columnInfo{name: strings.ToLower(names[i])}

		// JSON is never disguised (always type 0xf5) and go-mysql has no exported
		// IsJSONColumn, so a raw byte check is correct here. IsNumericColumn
		// resolves the real type and matches mysqlCategoryForTypeName's numeric set
		// exactly; ENUM, SET, BIT, dates, text, blob, geometry all fall to text.
		switch {
		case tm.ColumnType[i] == mysql.MYSQL_TYPE_JSON:
			ci.cat = sql.CatJSON
		case tm.IsNumericColumn(i):
			ci.cat = sql.CatNumber
		default:
			ci.cat = sql.CatText
		}

		if labels, ok := enums[i]; ok {
			ci.enumValues = labels
		}
		if labels, ok := sets[i]; ok {
			ci.setValues = labels
		}
		cols[i] = ci
	}
	return &tableSchema{cols: cols}, nil
}
