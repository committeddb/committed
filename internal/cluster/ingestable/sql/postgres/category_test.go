package postgres

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// TestPgCategoryAgreement is the load-bearing property: the OID mapper (CDC) and
// the type-name mapper (snapshot) must classify the same logical type the same
// way, or a row ingested by snapshot and the same row updated via CDC would
// disagree on its JSON shape.
func TestPgCategoryAgreement(t *testing.T) {
	for _, tc := range []struct {
		name     string
		oid      uint32
		typeName string
		want     sql.JSONCategory
	}{
		{"int4", pgtype.Int4OID, "INT4", sql.CatNumber},
		{"int8", pgtype.Int8OID, "INT8", sql.CatNumber},
		{"int2", pgtype.Int2OID, "INT2", sql.CatNumber},
		{"float8", pgtype.Float8OID, "FLOAT8", sql.CatNumber},
		{"numeric", pgtype.NumericOID, "NUMERIC", sql.CatNumber},
		{"bool", pgtype.BoolOID, "BOOL", sql.CatBool},
		{"json", pgtype.JSONOID, "JSON", sql.CatJSON},
		{"jsonb", pgtype.JSONBOID, "JSONB", sql.CatJSON},
		{"text", pgtype.TextOID, "TEXT", sql.CatText},
		{"varchar", pgtype.VarcharOID, "VARCHAR", sql.CatText},
		{"timestamp", pgtype.TimestampOID, "TIMESTAMP", sql.CatText},
		{"uuid", pgtype.UUIDOID, "UUID", sql.CatText},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, pgCategoryForOID(tc.oid), "OID (CDC)")
			require.Equal(t, tc.want, pgCategoryForTypeName(tc.typeName), "type name (snapshot)")
		})
	}
}
