package cluster_test

import (
	"testing"

	toml "github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestType_Configuration pins the read-back rule: a retained document comes
// back verbatim; without one, the document is synthesized from the fields —
// each declared key in the current spelling, and exactly one migration
// intent for a version after the first (the transform when there is one,
// nonConvertible when declared, else none).
func TestType_Configuration(t *testing.T) {
	retained := &cluster.Type{ID: "t", Name: "T", Version: 3, Document: []byte("# mine\n[type]\nname = \"T\"\n"), DocumentMimeType: "text/toml"}
	cfg, err := retained.Configuration()
	require.NoError(t, err)
	require.Equal(t, &cluster.Configuration{ID: "t", Name: "T", MimeType: "text/toml", Data: retained.Document}, cfg)

	decode := func(t *testing.T, typ *cluster.Type) map[string]map[string]any {
		t.Helper()
		cfg, err := typ.Configuration()
		require.NoError(t, err)
		require.Equal(t, "text/toml", cfg.MimeType)
		var doc map[string]map[string]any
		require.NoError(t, toml.Unmarshal(cfg.Data, &doc), "synthesized document must be valid TOML:\n%s", cfg.Data)
		return doc
	}

	t.Run("first version declares no migration", func(t *testing.T) {
		doc := decode(t, &cluster.Type{ID: "t", Name: "T", Version: 1})
		require.Equal(t, map[string]any{"name": "T"}, doc["type"])
		require.NotContains(t, doc, "migration")
	})
	t.Run("a later version without a program declared none", func(t *testing.T) {
		doc := decode(t, &cluster.Type{ID: "t", Name: "T", Version: 2})
		require.Equal(t, map[string]any{"none": true}, doc["migration"])
	})
	t.Run("a transform is the intent when there is one", func(t *testing.T) {
		doc := decode(t, &cluster.Type{ID: "t", Name: "T", Version: 2, Migration: []byte(".a |= 1\n| .b")})
		require.Equal(t, map[string]any{"transform": ".a |= 1\n| .b"}, doc["migration"])
	})
	t.Run("nonConvertible is the intent when declared", func(t *testing.T) {
		doc := decode(t, &cluster.Type{ID: "t", Name: "T", Version: 2, NonConvertible: true})
		require.Equal(t, map[string]any{"nonConvertible": true}, doc["migration"])
	})
	t.Run("every declared field in the current spelling", func(t *testing.T) {
		doc := decode(t, &cluster.Type{
			ID: "t", Name: "It's", Version: 1, SchemaType: "JSONSchema", Schema: []byte(`{"q":"'''"}`),
			Validate: cluster.ValidateSchema, EntityKind: cluster.EntityKindEvent, Discriminator: "$.kind",
		})
		require.Equal(t, map[string]any{
			"name": "It's", "schemaType": "JSONSchema", "schema": `{"q":"'''"}`,
			"validate": "schema", "entityKind": "event", "discriminator": "$.kind",
		}, doc["type"])
	})
}
