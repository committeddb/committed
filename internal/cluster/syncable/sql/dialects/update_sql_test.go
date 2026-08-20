package dialects_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// CreateUpdateSQL is the decorator's update-only apply: the ruleConfig's
// leading mappings are the key columns (skipped positionally into the
// WHERE), the rest form the SET; bind order is SET values first, key
// values last, each once.
func TestCreateUpdateSQL(t *testing.T) {
	cfg := &sql.Config{
		Table:      "t",
		PrimaryKey: []string{"job_id"},
		Mappings: []sql.Mapping{
			{Column: "job_id"}, {Column: "latest"}, {Column: "n"},
		},
	}
	require.Equal(t,
		`UPDATE "t" SET "latest"=$1,"n"=$2 WHERE "job_id" = $3`,
		(&dialects.PostgreSQLDialect{}).CreateUpdateSQL(cfg))
	require.Equal(t,
		"UPDATE `t` SET `latest`=?,`n`=? WHERE `job_id` = ?",
		(&dialects.MySQLDialect{}).CreateUpdateSQL(cfg))
}

func TestCreateUpdateSQLCompositeKey(t *testing.T) {
	cfg := &sql.Config{
		Table:      "t",
		PrimaryKey: []string{"a", "b"},
		Mappings: []sql.Mapping{
			{Column: "a"}, {Column: "b"}, {Column: "x"},
		},
	}
	require.Equal(t,
		`UPDATE "t" SET "x"=$1 WHERE "a" = $2 AND "b" = $3`,
		(&dialects.PostgreSQLDialect{}).CreateUpdateSQL(cfg))
}

// The enriched update: a decorator rule carrying a lookup arm — the FK
// column binds a plain placeholder, the enriched column resolves through
// the dimension subquery binding the FK's canonical rendering, keys last,
// everything bound once.
func TestCreateEnrichedUpdateSQL(t *testing.T) {
	cfg := &sql.Config{
		Table:      "t",
		PrimaryKey: []string{"job_id"},
		Mappings: []sql.Mapping{
			{Column: "job_id"}, {Column: "cust"}, {Column: "cust_name"},
		},
	}
	enrich := map[string]sql.SpineEnrichment{
		"cust_name": {DimTable: "t__lookup_dim", SelectField: "name", CastType: "TEXT"},
	}
	require.Equal(t,
		`UPDATE "t" SET "cust"=$1,"cust_name"=(SELECT "lookup_fields"->>'name' FROM "t__lookup_dim" WHERE "lookup_key" = $2)::TEXT WHERE "job_id" = $3`,
		(&dialects.PostgreSQLDialect{}).CreateEnrichedUpdateSQL(cfg, enrich))
	require.Equal(t,
		"UPDATE `t` SET `cust`=?,`cust_name`=(SELECT `lookup_fields`->>'$.name' FROM `t__lookup_dim` WHERE `lookup_key` = ?) WHERE `job_id` = ?",
		(&dialects.MySQLDialect{}).CreateEnrichedUpdateSQL(cfg, enrich))
}
