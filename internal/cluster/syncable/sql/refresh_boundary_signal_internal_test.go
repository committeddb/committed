package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/committeddb/committed/internal/cluster"
)

// captureWarns swaps the global logger for an observer at Warn level (the same
// pattern the parser tests use) and returns the sink plus a restore func.
func captureWarns() (*observer.ObservedLogs, func()) {
	core, logs := observer.New(zap.WarnLevel)
	return logs, zap.ReplaceGlobals(zap.New(core))
}

func refreshMarker(gen uint64) *cluster.Entity {
	return cluster.NewRefreshBoundaryEntity(&cluster.Type{ID: "topic-1"}, gen)
}

// A keyless (sweep == nil) sink no-ops the refresh-boundary marker BY DESIGN, but
// a re-snapshot boundary (generation > 1) must warn that the sink was NOT
// reconciled and needs a rebuild — the operator signal the ingest side can't give
// because it doesn't know the sink type. The initial snapshot (generation 1) has
// nothing to reconcile, so it stays quiet.
func TestApplyRefreshBoundary_KeylessSignalsOnlyOnResnapshot(t *testing.T) {
	logs, restore := captureWarns()
	defer restore()

	c := &Syncable{} // sweep == nil → keyless/append

	require.NoError(t, c.applyRefreshBoundary(context.Background(), nil, refreshMarker(1)))
	require.Zero(t, logs.FilterMessageSnippet("refresh boundary on a").Len(),
		"the initial snapshot boundary (generation 1) must not warn")

	require.NoError(t, c.applyRefreshBoundary(context.Background(), nil, refreshMarker(2)))
	require.Equal(t, 1, logs.FilterMessageSnippet("refresh boundary on a").Len(),
		"a re-snapshot boundary (generation > 1) must warn the keyless sink was not reconciled")
}

// A projection sink likewise no-ops the marker (a source entity fans out to many
// rows, so a topic sweep doesn't map onto it) but must warn on a re-snapshot
// boundary that it was not reconciled.
func TestProjectionApplyEntity_RefreshBoundarySignalsOnlyOnResnapshot(t *testing.T) {
	logs, restore := captureWarns()
	defer restore()

	p := &Projection{name: "proj-1"}

	require.NoError(t, p.applyEntity(context.Background(), nil, nil, refreshMarker(1)))
	require.Zero(t, logs.FilterMessageSnippet("refresh boundary on a").Len(),
		"the initial snapshot boundary (generation 1) must not warn")

	require.NoError(t, p.applyEntity(context.Background(), nil, nil, refreshMarker(2)))
	require.Equal(t, 1, logs.FilterMessageSnippet("refresh boundary on a").Len(),
		"a re-snapshot boundary (generation > 1) must warn the projection was not reconciled")
}
