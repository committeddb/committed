package db_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

// preflightTOML builds a type config whose [migration] section carries the
// given extra keys (transform, none, validateAgainst) verbatim.
func preflightTOML(migrationSection string) *cluster.Configuration {
	data := fmt.Sprintf("[type]\nname = \"Person\"\n\n[migration]\n%s\n", migrationSection)
	return &cluster.Configuration{ID: "person", MimeType: "text/toml", Data: []byte(data)}
}

// TestParseType_ValidateAgainst_OK: a transform that handles the sample
// passes pre-flight and parses as usual.
func TestParseType_ValidateAgainst_OK(t *testing.T) {
	cfg := preflightTOML(
		"transform = '. + {email: \"unknown@example.com\"}'\nvalidateAgainst = '{\"name\":\"alice\"}'")

	_, typ, err := db.ParseType(cfg, nil)
	require.NoError(t, err)
	require.Equal(t, `. + {email: "unknown@example.com"}`, string(typ.Migration))
	require.True(t, typ.MigrationExplicit)
}

// TestParseType_ValidateAgainst_RuntimeFailure: a program that compiles but
// breaks on the sample payload is rejected with the cause — the pre-flight
// catching exactly the class of error compile-time validation can't.
func TestParseType_ValidateAgainst_RuntimeFailure(t *testing.T) {
	cfg := preflightTOML(
		"transform = 'error(\"cannot derive email for \" + .name)'\nvalidateAgainst = '{\"name\":\"alice\"}'")

	_, _, err := db.ParseType(cfg, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "validateAgainst")
	require.Contains(t, err.Error(), "cannot derive email for alice")
}

// TestParseType_ValidateAgainst_BadSampleJSON: a sample that isn't JSON is
// itself a pre-flight failure (the program can't run against it).
func TestParseType_ValidateAgainst_BadSampleJSON(t *testing.T) {
	cfg := preflightTOML("transform = '.'\nvalidateAgainst = 'not json'")

	_, _, err := db.ParseType(cfg, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "validateAgainst")
}

// TestParseType_ValidateAgainst_RequiresTransform: validateAgainst with no
// transform (none = true, or no [migration] keys at all) is a config error —
// there is no program to validate.
func TestParseType_ValidateAgainst_RequiresTransform(t *testing.T) {
	for name, section := range map[string]string{
		"with none = true": "none = true\nvalidateAgainst = '{}'",
		"alone":            "validateAgainst = '{}'",
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := db.ParseType(preflightTOML(section), nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "requires migration.transform")
		})
	}
}

// TestProposeType_ValidateAgainstFailure_NothingProposed: the end-to-end
// guarantee behind POST /type/{id} returning 400 — a failing pre-flight
// surfaces as a ConfigError and the type's version history is untouched.
func TestProposeType_ValidateAgainstFailure_NothingProposed(t *testing.T) {
	d, s := newWalDB(t)
	proposeTypeTOML(t, d, "person", "Person", `{"type":"object"}`, "")

	cfg := &cluster.Configuration{ID: "person", MimeType: "text/toml", Data: []byte(
		"[type]\nname = \"Person\"\nschemaType = \"JSONSchema\"\nschema = '{\"type\":\"object\",\"required\":[\"email\"]}'\n\n" +
			"[migration]\ntransform = 'error(\"boom\")'\nvalidateAgainst = '{\"name\":\"alice\"}'\n")}

	err := d.ProposeType(testCtx(t), cfg)
	var cerr *cluster.ConfigError
	require.True(t, errors.As(err, &cerr), "a pre-flight failure must be a ConfigError (HTTP 400)")

	got, err := s.ResolveType(cluster.LatestTypeRef("person"))
	require.NoError(t, err)
	require.Equal(t, 1, got.Version, "the rejected version must not land")
}
