package postgres

import (
	"context"
	"encoding/json"
	"fmt"
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
	eNull := tupleToEntity(context.Background(), tuple, 1, relations, config, pgCfg, false, nil, nil)
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
	eFilled := tupleToEntity(context.Background(), tuple, 1, relations, config, pgCfg, false, resolve, nil)
	require.NotNil(t, eFilled)
	var pFilled map[string]any
	require.NoError(t, json.Unmarshal(eFilled.Data, &pFilled))
	require.Equal(t, "restored-toast-value", pFilled["big"], "with re-select the unchanged column is preserved")
	require.Equal(t, "1", pFilled["id"])
}

// TestTupleToEntity_UnchangedToastMixedCaseColumn is the recurrence regression:
// when the physical column is a quoted mixed-case identifier ("CreatedAt"), the
// unchanged-TOAST re-select must quote the PHYSICAL name, not a lowercased
// "createdat" — which Postgres rejects ("column does not exist"), a failure the
// caller swallows and then emits the column as null, silently clobbering the
// real value on every such UPDATE. The physical casing lives in the relation
// metadata; the re-select must use it for both the column list and the PK.
func TestTupleToEntity_UnchangedToastMixedCaseColumn(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		RelationID:   1,
		Namespace:    "public",
		RelationName: "Orders",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "Id", DataType: pgtype.TextOID},
			{Name: "CreatedAt", DataType: pgtype.TextOID},
		},
	}
	relations := map[uint32]*pglogrepl.RelationMessage{1: rel}

	// UPDATE new tuple: Id is sent ('t'); CreatedAt was left unchanged ('u').
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("1")},
			{DataType: 'u'},
		},
	}
	config := &sql.Config{
		Type:       &cluster.Type{ID: "t", Name: "t"},
		PrimaryKey: []string{"Id"},
		Mappings: []sql.Mapping{
			{JsonName: "id", SQLColumn: "Id"},
			{JsonName: "createdat", SQLColumn: "CreatedAt"},
		},
	}
	pgCfg := &pgConfig{tables: []string{"public.Orders"}}

	// resolve simulates Postgres: the physical column is "CreatedAt" and the PK
	// is "Id". A lowercased "createdat"/"id" (the pre-fix query) does not resolve.
	resolve := func(_ context.Context, table string, pk map[string]string, cols []string) (map[string]string, error) {
		out := map[string]string{}
		for _, c := range cols {
			if c != "CreatedAt" {
				return nil, fmt.Errorf("column %q does not exist", c)
			}
			out[c] = "2020-01-01T00:00:00Z"
		}
		if _, ok := pk["Id"]; !ok {
			return nil, fmt.Errorf("primary key not resolved to its physical name; got %v", pk)
		}
		return out, nil
	}

	e := tupleToEntity(context.Background(), tuple, 1, relations, config, pgCfg, false, resolve, nil)
	require.NotNil(t, e)
	var p map[string]any
	require.NoError(t, json.Unmarshal(e.Data, &p))
	require.Equal(t, "2020-01-01T00:00:00Z", p["createdat"],
		"the mixed-case column's real value must survive the unchanged-TOAST re-select, not go null")
	require.Equal(t, "1", p["id"])
}
