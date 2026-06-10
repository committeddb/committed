package parser_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

// Pins envelope-level decode tolerances (the golden corpus from
// .claude-scratch/tickets/viper-containment.md): the [syncable] /
// [ingestable] / [database] envelopes keep accepting case-variant
// section and field names, JSON-mimetype payloads keep working, and
// ${VAR} secret interpolation keeps expanding values. Written against
// the current pipeline; must stay green across any decoder change.

func TestParseSyncableEnvelopeToleratesCaseVariantKeys(t *testing.T) {
	fakeSyncable := &clusterfakes.FakeSyncable{}
	fakeParser := &clusterfakes.FakeSyncableParser{}
	fakeParser.ParseReturns(fakeSyncable, nil)

	p := parser.New()
	p.AddSyncableParser("foo", fakeParser)

	toml := `
[SYNCABLE]
Name = "bar"
Type = "foo"
Mode = "always-current"
`
	name, s, mode, err := p.ParseSyncable("text/toml", []byte(toml), nil)
	require.NoError(t, err)
	require.Equal(t, "bar", name)
	require.Equal(t, fakeSyncable, s)
	require.Equal(t, 1, int(mode), "always-current")
	require.Equal(t, 1, fakeParser.ParseCallCount())
}

func TestParseSyncableEnvelopeJSONMimeType(t *testing.T) {
	fakeSyncable := &clusterfakes.FakeSyncable{}
	fakeParser := &clusterfakes.FakeSyncableParser{}
	fakeParser.ParseReturns(fakeSyncable, nil)

	p := parser.New()
	p.AddSyncableParser("foo", fakeParser)

	jsonConfig := `{"syncable": {"name": "bar", "type": "foo"}}`
	name, s, _, err := p.ParseSyncable("application/json", []byte(jsonConfig), nil)
	require.NoError(t, err)
	require.Equal(t, "bar", name)
	require.Equal(t, fakeSyncable, s)
}

func TestParseIngestableEnvelopeToleratesCaseVariantKeys(t *testing.T) {
	fakeIngestable := &clusterfakes.FakeIngestable{}
	fakeParser := &clusterfakes.FakeIngestableParser{}
	fakeParser.ParseReturns(fakeIngestable, nil)

	p := parser.New()
	p.AddIngestableParser("foo", fakeParser)

	toml := "[INGESTABLE]\nName = \"bar\"\nType = \"foo\"\n"
	name, i, err := p.ParseIngestable("text/toml", []byte(toml))
	require.NoError(t, err)
	require.Equal(t, "bar", name)
	require.Equal(t, fakeIngestable, i)
}

func TestParseDatabaseEnvelopeToleratesCaseVariantKeys(t *testing.T) {
	fakeDB := &clusterfakes.FakeDatabase{}
	fakeParser := &clusterfakes.FakeDatabaseParser{}
	fakeParser.ParseReturns(fakeDB, nil)

	p := parser.New()
	p.AddDatabaseParser("foo", fakeParser)

	toml := "[DATABASE]\nName = \"bar\"\nType = \"foo\"\n"
	name, d, err := p.ParseDatabase("text/toml", []byte(toml))
	require.NoError(t, err)
	require.Equal(t, "bar", name)
	require.Equal(t, fakeDB, d)
}

// ${VAR} interpolation happens at the parse boundary and must keep
// expanding values regardless of decoder; the raw template never
// reaches the sub-parser.
func TestParseDatabaseInterpolatesSecrets(t *testing.T) {
	t.Setenv("TOLERANCE_TEST_SECRET", "s3cr3t")

	fakeDB := &clusterfakes.FakeDatabase{}
	fakeParser := &clusterfakes.FakeDatabaseParser{}
	fakeParser.ParseReturns(fakeDB, nil)

	p := parser.New()
	p.AddDatabaseParser("foo", fakeParser)

	toml := "[database]\nname = \"bar\"\ntype = \"foo\"\npassword = \"${TOLERANCE_TEST_SECRET}\"\n"
	_, _, err := p.ParseDatabase("text/toml", []byte(toml))
	require.NoError(t, err)

	require.Equal(t, 1, fakeParser.ParseCallCount())
	v := fakeParser.ParseArgsForCall(0)
	require.Equal(t, "s3cr3t", v.GetString("database.password"))

	// A missing secret is a *config.MissingVarError at the boundary.
	toml = "[database]\nname = \"bar\"\ntype = \"foo\"\npassword = \"${TOLERANCE_TEST_UNSET}\"\n"
	_, _, err = p.ParseDatabase("text/toml", []byte(toml))
	require.Error(t, err)
	require.Contains(t, err.Error(), "TOLERANCE_TEST_UNSET")
}
