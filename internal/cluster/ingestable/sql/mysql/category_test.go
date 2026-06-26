package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// TestMySQLCategoryForTypeName checks the MySQL type-name → JSON category mapping.
// One mapper now serves both ingest paths — information_schema data_type on the
// binlog path (via the schema cache) and database/sql DatabaseTypeName on the
// snapshot path — so a row decoded either way lands on the same JSON shape. MySQL
// has no native bool (tinyint is a number), so there is no bool category; ENUM and
// SET classify as text (their labels are strings after decodeEnumSet).
func TestMySQLCategoryForTypeName(t *testing.T) {
	for _, tc := range []struct {
		typeName string
		want     sql.JSONCategory
	}{
		{"INT", sql.CatNumber},
		{"BIGINT", sql.CatNumber},
		{"TINYINT", sql.CatNumber}, // mysql bool
		{"MEDIUMINT", sql.CatNumber},
		{"FLOAT", sql.CatNumber},
		{"DOUBLE", sql.CatNumber},
		{"DECIMAL", sql.CatNumber},
		{"YEAR", sql.CatNumber},
		{"JSON", sql.CatJSON},
		{"VARCHAR", sql.CatText},
		{"DATETIME", sql.CatText},
		{"ENUM", sql.CatText},
		{"SET", sql.CatText},
	} {
		t.Run(tc.typeName, func(t *testing.T) {
			require.Equal(t, tc.want, mysqlCategoryForTypeName(tc.typeName))
		})
	}
}
