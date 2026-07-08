package postgres

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// TestTupleToEntity_UnchangedToastReselected is the TOAST regression: a Postgres
// UPDATE that leaves a TOASTed column unchanged omits it from the new tuple
// (pgoutput status 'u'). Without the re-select it serializes as JSON null and
// clobbers the column downstream; with the resolver it carries the current value.
func TestTupleToEntity_UnchangedToastReselected(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		RelationID:   1,
		Namespace:    "public",
		RelationName: "t",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.TextOID},
			{Name: "big", DataType: pgtype.TextOID},
		},
	}
	relations := map[uint32]*pglogrepl.RelationMessage{1: rel}

	// UPDATE new tuple: id is sent ('t'); big was left unchanged ('u').
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("1")},
			{DataType: 'u'},
		},
	}
	config := &sql.Config{
		Type:       &cluster.Type{ID: "t", Name: "t"},
		PrimaryKey: []string{"id"},
		Mappings: []sql.Mapping{
			{JsonName: "id", SQLColumn: "id"},
			{JsonName: "big", SQLColumn: "big"},
		},
	}
	pgCfg := &pgConfig{tables: []string{"public.t"}}

	// Without a resolver: the unchanged TOAST column is null — the bug this fixes.
	eNull := tupleToEntity(context.Background(), tuple, 1, relations, config, pgCfg, false, nil)
	require.NotNil(t, eNull)
	var pNull map[string]any
	require.NoError(t, json.Unmarshal(eNull.Data, &pNull))
	require.Nil(t, pNull["big"], "without re-select an unchanged TOAST column serializes as null (the bug)")

	// With a resolver: the current value is re-selected by primary key and kept.
	resolve := func(_ context.Context, table string, pk map[string]string, cols []string) (map[string]string, error) {
		require.Equal(t, "public.t", table)
		require.Equal(t, map[string]string{"id": "1"}, pk)
		require.Equal(t, []string{"big"}, cols)
		return map[string]string{"big": "restored-toast-value"}, nil
	}
	eFilled := tupleToEntity(context.Background(), tuple, 1, relations, config, pgCfg, false, resolve)
	require.NotNil(t, eFilled)
	var pFilled map[string]any
	require.NoError(t, json.Unmarshal(eFilled.Data, &pFilled))
	require.Equal(t, "restored-toast-value", pFilled["big"], "with re-select the unchanged column is preserved")
	require.Equal(t, "1", pFilled["id"])
}
