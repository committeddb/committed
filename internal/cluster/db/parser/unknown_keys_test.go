package parser_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

// The envelope's vocabulary is closed: a typo in [ingestable]/[syncable]/
// [database], or a misspelled section, is a NotAdmissible field error at
// POST — not a key that silently does nothing.
func TestParse_RejectsUnknownEnvelopeKeysAndSections(t *testing.T) {
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

	cases := []struct {
		name, toml, field, near string
	}{
		{"ingestable typo", "[ingestable]\nname = \"a\"\ntype = \"foo\"\ncensusValue = true\n", "ingestable.censusValue", "censusValues"},
		{"ingestable unknown section", "[ingestable]\nname = \"a\"\ntype = \"foo\"\n[fooo]\nx = 1\n", "fooo", "foo"},
		{"syncable typo", "[syncable]\nname = \"a\"\ntype = \"foo\"\nzoen = \"z\"\n", "syncable.zoen", "zone"},
		{"syncable unknown section", "[syncable]\nname = \"a\"\ntype = \"foo\"\n[sync]\nx = 1\n", "sync", ""},
		{"database typo", "[database]\nname = \"a\"\ntype = \"foo\"\nnaem = \"b\"\n", "database.naem", "name"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var err error
			switch c.toml[1] {
			case 'i':
				_, _, err = p.ParseIngestable("text/toml", []byte(c.toml))
			case 's':
				_, _, _, err = p.ParseSyncable("text/toml", []byte(c.toml), nil)
			default:
				_, _, err = p.ParseDatabase("text/toml", []byte(c.toml))
			}
			require.Error(t, err)
			require.True(t, cluster.IsNotAdmissible(err), "a stored config with this typo must park on the upgraded binary, not retry")
			ce := cluster.NewConfigError(err)
			require.Equal(t, c.field, ce.Field)
			if c.near != "" {
				require.Contains(t, ce.Issue, `did you mean "`+c.near+`"?`)
			}
		})
	}

	// A section under a removed spelling is refused by the removal ledger
	// with the rename named, not as an anonymous unknown section.
	sp2 := &clusterfakes.FakeSyncableParser{}
	sp2.ParseReturns(&clusterfakes.FakeSyncable{}, nil)
	p.AddSyncableParser("projection", sp2)
	_, _, _, err := p.ParseSyncable("text/toml", []byte("[syncable]\nname = \"a\"\ntype = \"projection\"\n[sql-projection]\ntopic = \"t\"\n"), nil)
	require.Error(t, err)
	require.True(t, cluster.IsNotAdmissible(err))
	require.Contains(t, err.Error(), `"sql-projection" was removed in 0.8.0`)
	require.Contains(t, err.Error(), "[projection]")
}

// TestParseSyncable_RemovedSpellingIsRefusedAndParks: the removed
// "sql-projection" type is refused at admission as not-admissible (a stored
// config under it parks rather than retrying), naming the rename; the
// config-only extractors report nothing for it instead of erroring, so a
// parked legacy config cannot wedge the type, restatement, and database
// writes that enumerate every stored syncable.
func TestParseSyncable_RemovedSpellingIsRefusedAndParks(t *testing.T) {
	p := parser.New()
	doc := []byte("[syncable]\nname = \"legacy\"\ntype = \"sql-projection\"\n[sql-projection]\ntopic = \"t\"\ndb = \"d\"\ntable = \"x\"\n")
	_, _, _, err := p.ParseSyncable("text/toml", doc, nil)
	require.Error(t, err)
	require.True(t, cluster.IsNotAdmissible(err), "a removed spelling cannot heal by retry")
	require.Contains(t, err.Error(), `rename the type to "projection"`)

	topics, err := p.SyncableTopics("text/toml", doc)
	require.NoError(t, err)
	require.Nil(t, topics)
	derived, err := p.SyncableDerivedTopics("text/toml", doc)
	require.NoError(t, err)
	require.Nil(t, derived)
	require.NoError(t, p.SyncableSchemaChange("text/toml", doc, doc, nil), "the guard fails open across the rename")
}
