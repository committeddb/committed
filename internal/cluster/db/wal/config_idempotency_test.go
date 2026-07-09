package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// TestConfigApply_IdenticalReplayAppendsNoVersion is the config-apply
// replay-idempotency regression: re-applying a byte-identical config-upsert must
// NOT append a duplicate version. A crash-apply-window replay re-delivers the
// entry (entity fsynced, applied-index not), so a node that crashed mid-apply
// would otherwise diverge its version history and rollback-by-number from nodes
// that didn't — GET .../versions/{n} then returns different bytes per node.
// saveType already dedupes; database/syncable/ingestable now do too.
func TestConfigApply_IdenticalReplayAppendsNoVersion(t *testing.T) {
	for _, tc := range []struct {
		name      string
		section   string
		register  func(*parser.Parser)
		newEntity func(*cluster.Configuration) (*cluster.Entity, error)
		versions  func(*wal.Storage, string) ([]cluster.VersionInfo, error)
	}{
		{
			"database", "database",
			func(p *parser.Parser) {
				fp := &clusterfakes.FakeDatabaseParser{}
				fp.ParseReturns(&clusterfakes.FakeDatabase{}, nil)
				p.AddDatabaseParser("test", fp)
			},
			cluster.NewUpsertDatabaseEntity,
			func(s *wal.Storage, id string) ([]cluster.VersionInfo, error) { return s.DatabaseVersions(id) },
		},
		{
			"syncable", "syncable",
			func(p *parser.Parser) {
				fp := &clusterfakes.FakeSyncableParser{}
				fp.ParseReturns(&clusterfakes.FakeSyncable{}, nil)
				p.AddSyncableParser("test", fp)
			},
			cluster.NewUpsertSyncableEntity,
			func(s *wal.Storage, id string) ([]cluster.VersionInfo, error) { return s.SyncableVersions(id) },
		},
		{
			"ingestable", "ingestable",
			func(p *parser.Parser) {
				fp := &clusterfakes.FakeIngestableParser{}
				fp.ParseReturns(&clusterfakes.FakeIngestable{}, nil)
				p.AddIngestableParser("test", fp)
			},
			cluster.NewUpsertIngestableEntity,
			func(s *wal.Storage, id string) ([]cluster.VersionInfo, error) { return s.IngestableVersions(id) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := parser.New()
			tc.register(p)
			s, err := wal.Open(t.TempDir(), p, nil, nil, testOpenOptions...)
			require.Nil(t, err)
			defer s.Close()

			cfg := &cluster.Configuration{
				ID:       "cfg-idem",
				MimeType: "text/toml",
				Data:     []byte("[" + tc.section + "]\nname = \"cfg-idem\"\ntype = \"test\""),
			}
			e, err := tc.newEntity(cfg)
			require.Nil(t, err)

			// First apply → version 1.
			saveEntity(t, e, s, 1, 1)
			v, err := tc.versions(s, "cfg-idem")
			require.Nil(t, err)
			require.Len(t, v, 1, "first apply creates version 1")

			// A byte-identical replay at a new index must NOT append a version.
			saveEntity(t, e, s, 1, 2)
			v, err = tc.versions(s, "cfg-idem")
			require.Nil(t, err)
			require.Len(t, v, 1, "an identical replay must not append a new version")
		})
	}
}
