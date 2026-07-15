package sql_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	sql "github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// notSQLIngestable stands in for a different ingestable kind, so ValidateReplace
// has a non-*sql.Ingestable prior to fail open against.
type notSQLIngestable struct{}

func (notSQLIngestable) Ingest(context.Context, cluster.Position, chan<- *cluster.Proposal, chan<- cluster.Position) error {
	return nil
}

func (notSQLIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}
func (notSQLIngestable) Close() error { return nil }

func ingestableWith(topicID, topicName string, pk ...string) *sql.Ingestable {
	return sql.New(nil, &sql.Config{
		Type:       &cluster.Type{ID: topicID, Name: topicName},
		PrimaryKey: pk,
	})
}

// A primaryKey change (composite widening) is rejected with a
// *PrimaryKeyChangeError that satisfies the generic cluster.RebuildRequiredError
// — all the generic layers ever see — carrying the affected topic and the old
// and new keys for a deploy pipeline.
func TestIngestable_ValidateReplace_PrimaryKeyWidened(t *testing.T) {
	prior := ingestableWith("t1", "principals", "tconst")
	next := ingestableWith("t1", "principals", "tconst", "ordering")

	err := next.ValidateReplace(prior)
	require.Error(t, err)

	var rebuild cluster.RebuildRequiredError
	require.ErrorAs(t, err, &rebuild)
	require.Equal(t, "ingestable_primary_key_change_requires_recreate", rebuild.Code())

	details, ok := rebuild.Details().(*sql.PrimaryKeyChangeError)
	require.True(t, ok)
	require.Equal(t, "t1", details.TopicID)
	require.Equal(t, []string{"tconst"}, details.OldPrimaryKey)
	require.Equal(t, []string{"tconst", "ordering"}, details.NewPrimaryKey)
	require.Equal(t, "t1", details.AffectedTopic())
}

// Arity reduction (composite → single) — the manifestation that silently
// mis-pages the resume cursor — is rejected.
func TestIngestable_ValidateReplace_PrimaryKeyNarrowed(t *testing.T) {
	prior := ingestableWith("t1", "principals", "tconst", "ordering")
	next := ingestableWith("t1", "principals", "tconst")
	require.Error(t, next.ValidateReplace(prior))
}

// A same-arity column swap ([a,b] → [a,c]) still decodes cleanly at the cursor
// layer and so is exactly the silent case a decode guard can't catch — the
// config boundary must reject it.
func TestIngestable_ValidateReplace_SameArityColumnSwap(t *testing.T) {
	prior := ingestableWith("t1", "principals", "tconst", "ordering")
	next := ingestableWith("t1", "principals", "tconst", "nconst")
	require.Error(t, next.ValidateReplace(prior))
}

// Column order is part of the composite encoding (CompositeKey marshals in
// column order), so reordering re-keys every row and must be rejected.
func TestIngestable_ValidateReplace_OrderSensitive(t *testing.T) {
	prior := ingestableWith("t1", "principals", "tconst", "ordering")
	next := ingestableWith("t1", "principals", "ordering", "tconst")
	require.Error(t, next.ValidateReplace(prior))
}

// An identical primaryKey validates fine — nothing to rebuild.
func TestIngestable_ValidateReplace_Identical(t *testing.T) {
	prior := ingestableWith("t1", "principals", "tconst", "ordering")
	next := ingestableWith("t1", "principals", "tconst", "ordering")
	require.NoError(t, next.ValidateReplace(prior))
}

// A pure case change in a PK column name re-keys nothing: CompositeKey looks up
// values by lowercased column name, so ["ID"] and ["id"] produce identical
// entity keys. Rejecting it would be a false positive that forces a needless
// recreate.
func TestIngestable_ValidateReplace_CaseOnlyChangeAllowed(t *testing.T) {
	prior := ingestableWith("t1", "movies", "ID")
	next := ingestableWith("t1", "movies", "id")
	require.NoError(t, next.ValidateReplace(prior))
}

// Fail-open: replacing a prior of a different ingestable kind (nothing to
// compare a primaryKey against) is allowed rather than blocked.
func TestIngestable_ValidateReplace_FailsOpenAgainstNonSQLPrior(t *testing.T) {
	next := ingestableWith("t1", "movies", "tconst")
	require.NoError(t, next.ValidateReplace(notSQLIngestable{}))
}

// The propose path enriches the rejection (via cluster.DependentsAware) with the
// syncables that consume the affected topic; they surface in both the details
// payload and the human message.
func TestPrimaryKeyChangeError_SetDependents(t *testing.T) {
	err := &sql.PrimaryKeyChangeError{
		TopicID:       "t1",
		TopicName:     "principals",
		OldPrimaryKey: []string{"tconst"},
		NewPrimaryKey: []string{"tconst", "ordering"},
	}

	// Reachable through the generic interface the propose path uses.
	var aware cluster.DependentsAware = err
	require.Equal(t, "t1", aware.AffectedTopic())
	aware.SetDependents([]cluster.DependentSyncable{
		{ID: "sync-a", Name: "principals_sink"},
		{ID: "sync-b"},
	})

	require.Equal(t, []cluster.DependentSyncable{
		{ID: "sync-a", Name: "principals_sink"},
		{ID: "sync-b"},
	}, err.DependentSyncables)
	require.Contains(t, err.Error(), "principals_sink (sync-a)")
	require.Contains(t, err.Error(), "sync-b")
}
