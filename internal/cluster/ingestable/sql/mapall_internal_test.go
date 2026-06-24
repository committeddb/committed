package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExpandMapAllColumns(t *testing.T) {
	t.Run("maps every column 1:1 in source order", func(t *testing.T) {
		cfg := &Config{Tables: []string{"movie"}}
		require.NoError(t, expandMapAllColumns(cfg, map[string][]string{
			"movie": {"movie_id", "title", "year"},
		}))
		require.Equal(t, []Mapping{
			{JsonName: "movie_id", SQLColumn: "movie_id"},
			{JsonName: "title", SQLColumn: "title"},
			{JsonName: "year", SQLColumn: "year"},
		}, cfg.Mappings)
	})

	t.Run("explicit mapping overrides the inferred jsonName, keeping source order", func(t *testing.T) {
		cfg := &Config{
			Tables:   []string{"movie"},
			Mappings: []Mapping{{JsonName: "movieId", SQLColumn: "movie_id"}},
		}
		require.NoError(t, expandMapAllColumns(cfg, map[string][]string{
			"movie": {"movie_id", "title"},
		}))
		require.Equal(t, []Mapping{
			{JsonName: "movieId", SQLColumn: "movie_id"}, // overridden, still first (source order)
			{JsonName: "title", SQLColumn: "title"},
		}, cfg.Mappings)
	})

	t.Run("excludeColumns drops columns from the set", func(t *testing.T) {
		cfg := &Config{Tables: []string{"users"}, ExcludeColumns: []string{"password_hash"}}
		require.NoError(t, expandMapAllColumns(cfg, map[string][]string{
			"users": {"id", "email", "password_hash"},
		}))
		require.Equal(t, []Mapping{
			{JsonName: "id", SQLColumn: "id"},
			{JsonName: "email", SQLColumn: "email"},
		}, cfg.Mappings)
	})

	t.Run("unions columns across tables, first-seen wins", func(t *testing.T) {
		cfg := &Config{Tables: []string{"a", "b"}}
		require.NoError(t, expandMapAllColumns(cfg, map[string][]string{
			"a": {"id", "name"},
			"b": {"id", "val"}, // shared id mapped once
		}))
		require.Equal(t, []Mapping{
			{JsonName: "id", SQLColumn: "id"},
			{JsonName: "name", SQLColumn: "name"},
			{JsonName: "val", SQLColumn: "val"},
		}, cfg.Mappings)
	})

	t.Run("override for a non-existent column is a build error", func(t *testing.T) {
		cfg := &Config{
			Tables:   []string{"movie"},
			Mappings: []Mapping{{JsonName: "x", SQLColumn: "nope"}},
		}
		err := expandMapAllColumns(cfg, map[string][]string{"movie": {"movie_id"}})
		require.ErrorContains(t, err, `"nope" not found`)
	})

	t.Run("excludeColumns for a non-existent column is a build error (typo would leak the secret)", func(t *testing.T) {
		cfg := &Config{Tables: []string{"users"}, ExcludeColumns: []string{"pasword_hash"}}
		err := expandMapAllColumns(cfg, map[string][]string{"users": {"id", "password_hash"}})
		require.ErrorContains(t, err, `"pasword_hash" not found`)
	})

	t.Run("a column both excluded and explicitly mapped is contradictory", func(t *testing.T) {
		cfg := &Config{
			Tables:         []string{"users"},
			Mappings:       []Mapping{{JsonName: "pw", SQLColumn: "password_hash"}},
			ExcludeColumns: []string{"password_hash"},
		}
		err := expandMapAllColumns(cfg, map[string][]string{"users": {"id", "password_hash"}})
		require.ErrorContains(t, err, "both excluded and explicitly mapped")
	})

	t.Run("excluding every column leaves nothing to map", func(t *testing.T) {
		cfg := &Config{Tables: []string{"t"}, ExcludeColumns: []string{"a", "b"}}
		err := expandMapAllColumns(cfg, map[string][]string{"t": {"a", "b"}})
		require.ErrorContains(t, err, "no columns to map")
	})
}
