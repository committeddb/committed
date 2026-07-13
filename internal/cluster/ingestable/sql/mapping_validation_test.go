package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestValidateMappingColumns covers the deeper mixed-case fix: a mapping must
// resolve (case-insensitively) to a real source column, or the config is rejected
// at POST instead of silently emitting null on every row.
func TestValidateMappingColumns(t *testing.T) {
	cols := map[string][]string{"users": {"id", "CreatedAt", "email"}}

	t.Run("mixed-case mapping that resolves is accepted", func(t *testing.T) {
		cfg := &Config{Tables: []string{"users"}, Mappings: []Mapping{
			{JsonName: "createdAt", SQLColumn: "createdat"}, // lowercase config vs CamelCase source
			{JsonName: "id", SQLColumn: "ID"},               // uppercase config vs lowercase source
		}}
		require.NoError(t, validateMappingColumns(cfg, cols))
	})

	t.Run("nonexistent column is rejected", func(t *testing.T) {
		cfg := &Config{Tables: []string{"users"}, Mappings: []Mapping{
			{JsonName: "x", SQLColumn: "created_att"}, // typo
		}}
		err := validateMappingColumns(cfg, cols)
		require.Error(t, err)
		require.Contains(t, err.Error(), "created_att")
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("source columns colliding on case are rejected", func(t *testing.T) {
		cfg := &Config{Tables: []string{"t"}, Mappings: []Mapping{{JsonName: "a", SQLColumn: "col"}}}
		err := validateMappingColumns(cfg, map[string][]string{"t": {"Col", "col"}})
		require.Error(t, err)
		require.Contains(t, err.Error(), "differ only by case")
	})

	// A multi-table ingestable emits every table's rows under ONE entity key, so a
	// primaryKey column must exist in EVERY watched table — not just the union.
	// A PK absent from one table decodes to nothing for that table's rows and
	// CompositeKey collapses them all onto "<nil>" (silent overwrite + wrong-key
	// deletes). Mappings may still be table-specific (union); the key can't be.
	t.Run("primaryKey missing from one of several tables is rejected", func(t *testing.T) {
		multi := map[string][]string{
			"orders": {"id", "amount"},
			"events": {"amount", "ts"}, // no "id"
		}
		cfg := &Config{
			Tables:     []string{"orders", "events"},
			PrimaryKey: []string{"id"}, // present in orders, absent from events
			Mappings:   []Mapping{{JsonName: "a", SQLColumn: "amount"}},
		}
		err := validateMappingColumns(cfg, multi)
		require.Error(t, err)
		require.Contains(t, err.Error(), "id")
		require.Contains(t, err.Error(), "events", "the error must name the table missing the PK column")
	})

	t.Run("primaryKey present in every table is accepted (case-insensitively)", func(t *testing.T) {
		multi := map[string][]string{
			"orders": {"ID", "amount"}, // CamelCase
			"events": {"id", "ts"},     // lowercase
		}
		cfg := &Config{
			Tables:     []string{"orders", "events"},
			PrimaryKey: []string{"Id"}, // resolves case-insensitively in both
			Mappings:   []Mapping{},
		}
		require.NoError(t, validateMappingColumns(cfg, multi))
	})

	t.Run("mixed-case primaryKey resolves; nonexistent primaryKey rejected", func(t *testing.T) {
		ok := &Config{
			Tables:     []string{"users"},
			PrimaryKey: []string{"ID"}, // mixed case vs source column CreatedAt/id... "id" not present; use a real col
			Mappings:   []Mapping{{JsonName: "e", SQLColumn: "email"}},
		}
		// "ID" must resolve case-insensitively to a real column; add "id" to source.
		ok2cols := map[string][]string{"users": {"id", "CreatedAt", "email"}}
		require.NoError(t, validateMappingColumns(ok, ok2cols))

		bad := &Config{
			Tables:     []string{"users"},
			PrimaryKey: []string{"user_idd"}, // typo
			Mappings:   []Mapping{{JsonName: "e", SQLColumn: "email"}},
		}
		err := validateMappingColumns(bad, ok2cols)
		require.Error(t, err)
		require.Contains(t, err.Error(), "user_idd")
		require.Contains(t, err.Error(), "primaryKey")
	})
}
