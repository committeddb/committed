package wal_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"

	pb "go.etcd.io/raft/v3/raftpb"
)

// TestSnapshot_RestoreDegradesOnUnbuildableDatabaseConfig pins the convergence
// that makes RestoreSnapshot re-derive its database-handle cache through the
// SAME degrade policy as Open's loadDatabases. A snapshot's bbolt can carry a
// database config that is valid cluster-wide yet unbuildable on the restoring
// node (the classic case: a ${VAR} secret present on the leader but not here).
// Open records that as a config-build error and keeps the node in quorum;
// RestoreSnapshot used to hard-fail the reload instead, which processSnapshot
// turns into a node-fataling — a node-local env gap taking the node down. After
// the convergence the two paths share one policy: degrade, do not fatal.
func TestSnapshot_RestoreDegradesOnUnbuildableDatabaseConfig(t *testing.T) {
	// Source: a parser that BUILDS the database config, so the upsert applies and
	// the raw config lands in the snapshot's bbolt.
	srcParser := parser.New()
	okParser := &clusterfakes.FakeDatabaseParser{}
	okParser.ParseReturns(&clusterfakes.FakeDatabase{}, nil)
	srcParser.AddDatabaseParser("degrades", okParser)

	src := NewStorageWithParser(t, nil, srcParser)
	defer src.Cleanup()

	cfg := createDatabaseConfiguration("degrades")
	insertDatabases(t, src, []*cluster.Configuration{cfg}, 1, 1)

	snap, err := src.CreateSnapshot(1, &pb.ConfState{})
	require.Nil(t, err)
	require.Equal(t, uint64(1), snap.Metadata.GetIndex())

	// Dest: the SAME config, but a parser that fails to build it here — the
	// missing-secret asymmetry. Register the sub-parser under the config's inner
	// type so dispatch reaches it (parsing then returns the failure).
	dstParser := parser.New()
	failParser := &clusterfakes.FakeDatabaseParser{}
	failParser.ParseReturns(nil, errors.New("build database: missing ${SECRET}"))
	dstParser.AddDatabaseParser("degrades", failParser)

	dst := NewStorageWithParser(t, nil, dstParser)
	defer dst.Cleanup()

	// EventIndex must be >= snap.Metadata.Index for the restore to be allowed;
	// a type upsert at index 1 bumps it without touching the database parser.
	tp, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "events", Name: "Events", Version: 1})
	require.Nil(t, err)
	saveEntity(t, tp, dst, 1, 1)
	require.Equal(t, uint64(1), dst.EventIndex())

	require.Nil(t, dst.RestoreSnapshot(snap),
		"restore must degrade on an unbuildable database config, not abort and fatal the node")

	require.GreaterOrEqual(t, dst.ConfigBuildErrorCount(), 1,
		"the unbuildable database config must be recorded as a build error, mirroring Open")
	_, err = dst.Database("degrades")
	require.Error(t, err, "the unbuildable database must not be cached")

	// Deterministic pin of the restore/validate race: refreshAfterRestore runs
	// validateConfigSecrets on its own goroutine, and Parser.Validate PASSES for
	// this config (it is a side-effect-free structural check that never invokes
	// the registered database parser whose build fails). That weaker success
	// must not clear the build failure recorded above — before the evidence
	// ranking it did, so this test flaked green-or-red on goroutine timing.
	require.Nil(t, dst.ValidateConfigSecretsForTest())
	require.GreaterOrEqual(t, dst.ConfigBuildErrorCount(), 1,
		"a passing Validate must not clear a recorded build failure")
	_, err = dst.Database("degrades")
	require.Error(t, err, "the database is still unbuildable and must stay uncached")
}
