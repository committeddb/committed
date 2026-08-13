package dialects_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// compositeCfg is the SQL-text fixture: a two-column composite key.
func compositeCfg() *sql.Config {
	return &sql.Config{
		Table: "pca",
		Mappings: []sql.Mapping{
			{JsonPath: "$.t", Column: "tenant_id", SQLType: "INT"},
			{JsonPath: "$.p", Column: "project_id", SQLType: "INT"},
			{JsonPath: "$.v", Column: "v", SQLType: "TEXT"},
		},
		PrimaryKey: []string{"tenant_id", "project_id"},
	}
}

// The composite-PK finding's exact failure, inverted into the pin: the DDL
// must declare a REAL multi-column PRIMARY KEY — never a stringified list as
// one column name — and every key-consuming statement must bind each column,
// in config order (the encoding contract).
func TestCompositePrimaryKeySQLText(t *testing.T) {
	pg := &dialects.PostgreSQLDialect{}
	my := &dialects.MySQLDialect{}
	cfg := compositeCfg()

	require.Contains(t, pg.CreateDDL(cfg), `,PRIMARY KEY ("tenant_id","project_id")`)
	require.Contains(t, my.CreateDDL(cfg), ",PRIMARY KEY (`tenant_id`,`project_id`)")

	require.Equal(t, `DELETE FROM "pca" WHERE "tenant_id" = $1 AND "project_id" = $2`,
		pg.CreateDeleteSQL(cfg))
	require.Equal(t, "DELETE FROM `pca` WHERE `tenant_id` = ? AND `project_id` = ?",
		my.CreateDeleteSQL(cfg))

	require.Contains(t, pg.CreateGenerationUpsertSQL(cfg),
		` ON CONFLICT ("tenant_id","project_id") DO UPDATE SET `)
}

// Single-column configs must render byte-identically to the pre-composite
// forms — the compat contract the tolerance corpus leans on.
func TestSingleColumnSQLTextUnchanged(t *testing.T) {
	pg := &dialects.PostgreSQLDialect{}
	my := &dialects.MySQLDialect{}
	cfg := &sql.Config{
		Table: "w",
		Mappings: []sql.Mapping{
			{JsonPath: "$.pk", Column: "pk", SQLType: "TEXT"},
		},
		PrimaryKey: []string{"pk"},
	}
	require.Contains(t, pg.CreateDDL(cfg), `,PRIMARY KEY ("pk")`)
	require.Equal(t, `DELETE FROM "w" WHERE "pk" = $1`, pg.CreateDeleteSQL(cfg))
	require.Equal(t, "DELETE FROM `w` WHERE `pk` = ?", my.CreateDeleteSQL(cfg))
}
