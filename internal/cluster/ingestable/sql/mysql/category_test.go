package mysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// TestMySQLCategoryAgreement is the load-bearing property: the canal-type mapper
// (binlog/CDC) and the type-name mapper (snapshot) must classify the same
// logical type the same way, or a snapshot row and the same row over the binlog
// would disagree on its JSON shape. MySQL has no native bool (tinyint is a
// number), so there is no bool category on either path.
func TestMySQLCategoryAgreement(t *testing.T) {
	for _, tc := range []struct {
		name      string
		canalType int
		typeName  string
		want      sql.JSONCategory
	}{
		{"int", schema.TYPE_NUMBER, "INT", sql.CatNumber},
		{"bigint", schema.TYPE_NUMBER, "BIGINT", sql.CatNumber},
		{"tinyint (mysql bool)", schema.TYPE_NUMBER, "TINYINT", sql.CatNumber},
		{"mediumint", schema.TYPE_MEDIUM_INT, "MEDIUMINT", sql.CatNumber},
		{"float", schema.TYPE_FLOAT, "FLOAT", sql.CatNumber},
		{"double", schema.TYPE_FLOAT, "DOUBLE", sql.CatNumber},
		{"decimal", schema.TYPE_DECIMAL, "DECIMAL", sql.CatNumber},
		{"json", schema.TYPE_JSON, "JSON", sql.CatJSON},
		{"varchar", schema.TYPE_STRING, "VARCHAR", sql.CatText},
		{"datetime", schema.TYPE_DATETIME, "DATETIME", sql.CatText},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, mysqlCategoryForCanalType(tc.canalType), "canal type (CDC)")
			require.Equal(t, tc.want, mysqlCategoryForTypeName(tc.typeName), "type name (snapshot)")
		})
	}
}
