package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

// These tests pin decode tolerances that deployed type configs may
// depend on (the golden corpus from
// .claude-scratch/tickets/viper-containment.md). They are written
// against the current pipeline and must stay green across any decoder
// change. ParseType's storage parameter is unused, so nil is fine.

// Case-variant section/field names decode identically to the canonical
// lowercase form, and user data (the schema document, jq programs)
// keeps its case byte-exact.
func TestParseTypeToleratesCaseVariantKeys(t *testing.T) {
	variant := `
[TYPE]
Name       = "Person"
SchemaType = "JSONSchema"
Schema     = '{"type":"object","required":["camelCaseField"]}'
Validate   = 1
EntityKind = "event"

[Migration]
Transform = '. + {camelCase: "Value"}'
`
	c := &cluster.Configuration{ID: "person", MimeType: "text/toml", Data: []byte(variant)}
	name, tipe, err := db.ParseType(c, nil)
	require.NoError(t, err)

	require.Equal(t, "Person", name)
	require.Equal(t, "JSONSchema", tipe.SchemaType)
	require.Equal(t, `{"type":"object","required":["camelCaseField"]}`, string(tipe.Schema), "schema is user data — byte-exact")
	require.Equal(t, cluster.ValidateSchema, tipe.Validate)
	require.Equal(t, cluster.EntityKindEvent, tipe.EntityKind)
	require.Equal(t, `. + {camelCase: "Value"}`, string(tipe.Migration), "jq program is user data — byte-exact")
	require.True(t, tipe.MigrationExplicit)
}

// JSON-mimetype type configs decode like their TOML twin, including
// the int-typed validate flag and IsSet-dependent optionality.
func TestParseTypeJSONMimeType(t *testing.T) {
	jsonConfig := `{
  "type": {
    "name": "Person",
    "schemaType": "JSONSchema",
    "schema": "{\"type\":\"object\"}",
    "validate": 1
  }
}`
	c := &cluster.Configuration{ID: "person", MimeType: "application/json", Data: []byte(jsonConfig)}
	name, tipe, err := db.ParseType(c, nil)
	require.NoError(t, err)

	require.Equal(t, "Person", name)
	require.Equal(t, cluster.ValidateSchema, tipe.Validate, "validate arrives as a JSON number and must coerce to int")
	require.Equal(t, `{"type":"object"}`, string(tipe.Schema))
}

// Optionality is presence-based (IsSet semantics): a bare type config
// has zero values for everything optional and no migration intent.
func TestParseTypeOptionalityIsPresenceBased(t *testing.T) {
	c := &cluster.Configuration{ID: "simple", MimeType: "text/toml", Data: []byte("[type]\nname = \"simple\"")}
	_, tipe, err := db.ParseType(c, nil)
	require.NoError(t, err)

	require.Equal(t, 0, tipe.Version)
	require.Empty(t, tipe.SchemaType)
	require.Empty(t, tipe.Schema)
	require.Equal(t, cluster.NoValidation, tipe.Validate)
	require.False(t, tipe.MigrationExplicit, "no [migration] section means no explicit intent")

	// migration.none = false is present but false: still not explicit.
	c = &cluster.Configuration{ID: "simple", MimeType: "text/toml", Data: []byte("[type]\nname = \"simple\"\n\n[migration]\nnone = false")}
	_, tipe, err = db.ParseType(c, nil)
	require.NoError(t, err)
	require.False(t, tipe.MigrationExplicit)
}
