package parser_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

// requireVocabulary asserts a section's declared vocabulary is exactly the
// keys the readers asked for: nothing read that would be rejected, and no
// declared key that nobody reads (a typo-tolerant hole).
func requireVocabulary(t *testing.T, section string, declared []string, read map[string][]string) {
	t.Helper()
	undeclared, unread := cluster.VocabularyDiff(declared, read[section])
	require.Empty(t, undeclared, "[%s]: keys read but not declared — they would be rejected at POST", section)
	require.Empty(t, unread, "[%s]: keys declared but never read — a typo there would be silently inert again", section)
}

// TestEnvelopeVocabularies_EqualTheReads pins each envelope section's
// declaration to the union of every helper that reads it — ParseX plus the
// side helpers that parse their own copy of the bytes.
func TestEnvelopeVocabularies_EqualTheReads(t *testing.T) {
	p := parser.New()
	ip := &clusterfakes.FakeIngestableParser{}
	ip.ParseReturns(&clusterfakes.FakeIngestable{}, nil)
	p.AddIngestableParser("foo", ip)
	sp := &clusterfakes.FakeSyncableParser{}
	sp.ParseReturns(&clusterfakes.FakeSyncable{}, nil)
	p.AddSyncableParser("foo", sp)
	dp := &clusterfakes.FakeDatabaseParser{}
	dp.ParseReturns(&clusterfakes.FakeDatabase{}, nil)
	p.AddDatabaseParser("foo", dp)

	ingestable := []byte("[ingestable]\nname = \"a\"\ntype = \"foo\"\ncensus = true\ncensusValues = true\ncensusValueLimit = 5\n[foo]\nx = 1\n")
	syncable := []byte("[syncable]\nname = \"a\"\ntype = \"foo\"\nmode = \"always-current\"\nzone = \"z\"\ncheckpointEvery = 10\ncheckpointMaxAge = \"1m\"\n[foo]\nx = 1\n")
	database := []byte("[database]\nname = \"a\"\ntype = \"foo\"\n[foo]\nx = 1\n")

	read := cluster.ObserveConfigReads(func() {
		_, _, err := p.ParseIngestable("text/toml", ingestable)
		require.NoError(t, err)
		_, _ = p.IngestableTopics("text/toml", ingestable)
		v, err := cluster.ParseConfigBytes("text/toml", ingestable)
		require.NoError(t, err)
		_ = cluster.ParseCensusOptions(v)

		_, _, _, err = p.ParseSyncable("text/toml", syncable, nil)
		require.NoError(t, err)
		_, _ = p.SyncableTopics("text/toml", syncable)
		_, _ = p.SyncableDerivedTopics("text/toml", syncable)
		_, _ = p.SyncableMode("text/toml", syncable)
		_, _ = p.SyncableZone("text/toml", syncable)
		_, _ = p.SyncableDatabases("text/toml", syncable)
		v, err = cluster.ParseConfigBytes("text/toml", syncable)
		require.NoError(t, err)
		_, _ = cluster.ParseCheckpointPolicy(v)

		_, _, err = p.ParseDatabase("text/toml", database)
		require.NoError(t, err)
	})
	requireVocabulary(t, "ingestable", parser.IngestableEnvelopeKeys, read)
	requireVocabulary(t, "syncable", parser.SyncableEnvelopeKeys, read)
	requireVocabulary(t, "database", parser.DatabaseEnvelopeKeys, read)
}
