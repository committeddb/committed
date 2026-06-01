package wal_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/db"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
)

// storageWithTemplatedDB returns a storage holding one persisted database
// config whose connectionString templates ${TEST_SECRET}. TEST_SECRET
// must be set when this runs because the initial save interpolates it.
func storageWithTemplatedDB(t *testing.T) *StorageWrapper {
	var p db.Parser = parser.New()
	fake := &clusterfakes.FakeDatabaseParser{}
	fake.ParseReturns(&clusterfakes.FakeDatabase{}, nil)
	p.AddDatabaseParser("sql", fake)

	s := NewStorageWithParser(t, index(3).terms(3, 4, 5), p)
	cfg := &cluster.Configuration{
		ID:       "mydb",
		MimeType: "application/json",
		Data:     []byte(`{"database":{"type":"sql","name":"mydb"},"sql":{"connectionString":"user:${TEST_SECRET}@tcp"}}`),
	}
	insertDatabases(t, s, []*cluster.Configuration{cfg}, 6, 6)
	return s
}

// TestValidateConfigSecrets_DegradesOnMissingVar asserts that reopening a
// node whose persisted config templates an env var that is no longer set
// does NOT fatal-exit (which would take the node out of quorum over a
// node-local env gap). Instead the node starts, records the config as
// degraded (surfaced via the build-errors gauge), and keeps serving.
func TestValidateConfigSecrets_DegradesOnMissingVar(t *testing.T) {
	t.Setenv("TEST_SECRET", "s3cr3t") // required for the initial save below
	s := storageWithTemplatedDB(t)
	defer s.Cleanup()

	require.NoError(t, os.Unsetenv("TEST_SECRET"))

	s2, err := s.CloseAndReopen()
	require.NoError(t, err, "a missing ${secret} must degrade the config, not fail Open")
	defer s2.Cleanup()

	require.Equal(t, 1, s2.ConfigBuildErrorCount(),
		"the unbuildable database config must be recorded as degraded")
}

// TestConfigApply_MissingSecretDegradesNotFatal is the headline bug: a
// config PROPOSED AT RUNTIME whose ${secret} is unset on this node must
// not fatal-crash the apply (which, on a freshly-rolled secret, would
// crash every follower at once → quorum loss). The raw config is
// persisted on every replica; only the local build degrades.
func TestConfigApply_MissingSecretDegradesNotFatal(t *testing.T) {
	var p db.Parser = parser.New()
	fake := &clusterfakes.FakeDatabaseParser{}
	fake.ParseReturns(&clusterfakes.FakeDatabase{}, nil)
	p.AddDatabaseParser("sql", fake)

	s := NewStorageWithParser(t, index(3).terms(3, 4, 5), p)
	defer s.Cleanup()

	// The MissingVarError comes from the ${VAR} interpolation step, not
	// the (stub) dialect, so it fires for any unset var. This name is
	// never set in the environment.
	cfg := &cluster.Configuration{
		ID:       "mydb",
		MimeType: "application/json",
		Data:     []byte(`{"database":{"type":"sql","name":"mydb"},"sql":{"connectionString":"user:${COMMITTED_UNSET_SECRET_XYZ}@tcp"}}`),
	}
	e, err := cluster.NewUpsertDatabaseEntity(cfg)
	require.NoError(t, err)

	// saveEntity drives Save + ApplyCommitted and asserts the apply
	// returned nil — i.e. the missing secret did NOT crash the node.
	saveEntity(t, e, s, 6, 6)

	// The raw config is persisted (replicas converge regardless of env)...
	cfgs, err := s.Databases()
	require.NoError(t, err)
	require.Len(t, cfgs, 1)
	require.Equal(t, "mydb", cfgs[0].ID)

	// ...but the live connection is not built here, and it's recorded as
	// degraded (surfaced via the gauge).
	_, err = s.Database("mydb")
	require.Error(t, err, "the degraded database must not be cached")
	require.Equal(t, 1, s.ConfigBuildErrorCount())
}

// TestValidateConfigSecrets_PassesWhenVarSet asserts the same node
// reopens cleanly — with no degraded configs — while the templated env
// var remains set.
func TestValidateConfigSecrets_PassesWhenVarSet(t *testing.T) {
	t.Setenv("TEST_SECRET", "s3cr3t")
	s := storageWithTemplatedDB(t)
	defer s.Cleanup()

	s2, err := s.CloseAndReopen()
	require.NoError(t, err)
	defer s2.Cleanup()

	require.Equal(t, 0, s2.ConfigBuildErrorCount(),
		"no configs should be degraded when the secret is present")
}
