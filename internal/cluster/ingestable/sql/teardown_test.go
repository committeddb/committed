package sql_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// stubDialect is a minimal sql.Dialect for the teardown-delegation tests: every
// method is an inert stub, so a test can focus on one behavior.
type stubDialect struct{}

func (stubDialect) Ingest(context.Context, *sql.Config, cluster.Position, chan<- *cluster.Proposal, chan<- cluster.Position) error {
	return nil
}
func (stubDialect) Preflight(*sql.Config) error { return nil }

func (stubDialect) Status(context.Context, *sql.Config, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}
func (stubDialect) SourceColumns(*sql.Config) (map[string][]string, error) { return nil, nil }

// teardownDialect adds the optional source-teardown capability, recording the
// call so the test can assert delegation.
type teardownDialect struct {
	stubDialect
	called bool
	err    error
}

func (d *teardownDialect) TeardownSource(*sql.Config) error {
	d.called = true
	return d.err
}

// TestIngestableTeardownDelegatesToDialect verifies sql.Ingestable.Teardown
// delegates to the dialect when it owns source-side replication resources (drop
// the slot), and is a clean no-op when the dialect owns none (MySQL).
func TestIngestableTeardownDelegatesToDialect(t *testing.T) {
	// A dialect that owns teardown: Teardown calls it and returns its result.
	td := &teardownDialect{err: errors.New("boom")}
	require.EqualError(t, sql.New(td, &sql.Config{}).Teardown(), "boom")
	require.True(t, td.called, "Teardown must delegate to the dialect's TeardownSource")

	// A dialect with no teardown capability: Teardown is a clean no-op.
	require.NoError(t, sql.New(stubDialect{}, &sql.Config{}).Teardown())
}
