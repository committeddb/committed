package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

// Type and restatement documents have a closed vocabulary too: a typo is a
// NotAdmissible field error naming the key, never a silently inert setting.
func TestParseType_RejectsUnknownKeys(t *testing.T) {
	_, _, err := db.ParseType(&cluster.Configuration{
		ID: "11111111-1111-4111-8111-111111111111", MimeType: "text/toml",
		Data: []byte("[type]\nname = \"t\"\nschemaTyp = \"jsonschema\"\n"),
	}, nil)
	require.Error(t, err)
	require.True(t, cluster.IsNotAdmissible(err))
	require.Equal(t, "type.schemaTyp", cluster.NewConfigError(err).Field)
	require.Contains(t, err.Error(), `did you mean "schemaType"?`)

	_, _, err = db.ParseType(&cluster.Configuration{
		ID: "11111111-1111-4111-8111-111111111111", MimeType: "text/toml",
		Data: []byte("[type]\nname = \"t\"\n[migrations]\nnone = true\n"),
	}, nil)
	require.Error(t, err)
	require.Equal(t, "migrations", cluster.NewConfigError(err).Field)
	require.Contains(t, err.Error(), `did you mean "migration"?`)
}

func TestParseRestatement_RejectsUnknownKeys(t *testing.T) {
	_, err := db.ParseRestatement(&cluster.Configuration{
		ID: "r1", MimeType: "text/toml",
		Data: []byte("[restatement]\ntype = \"t\"\nfromIndex = 1\ntoIdx = 5\nreadAsVersion = 2\n"),
	})
	require.Error(t, err)
	require.Equal(t, "restatement.toIdx", cluster.NewConfigError(err).Field)
	require.Contains(t, err.Error(), `did you mean "toIndex"?`)
}

func TestTypeAndRestatementVocabularies_EqualTheReads(t *testing.T) {
	typeDoc := `
[type]
name = "t"
version = 2
schemaType = "jsonschema"
schema = '{"type":"object"}'
validate = 0
schemaChangeTopic = ""
entityKind = "event"
discriminator = "kind"
[migration]
transform = "."
validateAgainst = '{"v":1}'
`
	read := cluster.ObserveConfigReads(func() {
		_, _, err := db.ParseType(&cluster.Configuration{ID: "11111111-1111-4111-8111-111111111111", MimeType: "text/toml", Data: []byte(typeDoc)}, nil)
		require.NoError(t, err)
		_, err = db.ParseRestatement(&cluster.Configuration{ID: "r1", MimeType: "text/toml", Data: []byte(
			"[restatement]\ntype = \"t\"\nfromIndex = 1\ntoIndex = 5\nfromVersion = 1\nreadAsVersion = 2\npredicate = \".\"\n")})
		require.NoError(t, err)
	})
	for _, c := range []struct {
		section  string
		declared []string
	}{
		{"type", db.TypeKeys}, {"migration", db.MigrationKeys}, {"restatement", db.RestatementKeys},
	} {
		undeclared, unread := cluster.VocabularyDiff(c.declared, read[c.section])
		require.Empty(t, undeclared, "[%s]: keys read but not declared", c.section)
		require.Empty(t, unread, "[%s]: keys declared but never read", c.section)
	}
}
