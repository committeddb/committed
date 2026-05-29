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

// TestValidateConfigSecrets_FailsFastOnMissingVar asserts that reopening
// a node whose persisted config templates an env var that is no longer
// set is a fatal Open error — not a silent empty credential.
func TestValidateConfigSecrets_FailsFastOnMissingVar(t *testing.T) {
	t.Setenv("TEST_SECRET", "s3cr3t") // required for the initial save below
	s := storageWithTemplatedDB(t)
	defer s.Cleanup()

	require.NoError(t, os.Unsetenv("TEST_SECRET"))

	_, err := s.CloseAndReopen()
	require.Error(t, err)
	require.Contains(t, err.Error(), "TEST_SECRET")
	require.Contains(t, err.Error(), "mydb")
}

// TestValidateConfigSecrets_PassesWhenVarSet asserts the same node
// reopens cleanly while the templated env var remains set.
func TestValidateConfigSecrets_PassesWhenVarSet(t *testing.T) {
	t.Setenv("TEST_SECRET", "s3cr3t")
	s := storageWithTemplatedDB(t)
	defer s.Cleanup()

	s2, err := s.CloseAndReopen()
	require.NoError(t, err)
	defer s2.Cleanup()
}
