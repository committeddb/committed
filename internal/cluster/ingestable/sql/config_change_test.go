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

// ingestableSrc builds an *Ingestable carrying a topic, a connection string,
// and a primary key — the source-identity fields ValidateReplace compares.
func ingestableSrc(topicID, conn string, pk ...string) *sql.Ingestable {
	return sql.New(nil, &sql.Config{
		Type:             &cluster.Type{ID: topicID, Name: topicID},
		ConnectionString: conn,
		PrimaryKey:       pk,
	})
}

// A topic re-point (same source, different produced topic) starts the new topic
// mid-stream with no snapshot of the source's existing rows — silent data loss.
// It is rejected with a *SourceIdentityChangeError.
func TestIngestable_ValidateReplace_TopicChangeRejected(t *testing.T) {
	prior := ingestableSrc("t1", "postgres://h/db", "id")
	next := ingestableSrc("t2", "postgres://h/db", "id")

	err := next.ValidateReplace(prior)
	require.Error(t, err)
	var rebuild cluster.RebuildRequiredError
	require.ErrorAs(t, err, &rebuild)
	require.Equal(t, "ingestable_source_change_requires_recreate", rebuild.Code())
	details, ok := rebuild.Details().(*sql.SourceIdentityChangeError)
	require.True(t, ok)
	require.Equal(t, []string{"topic"}, details.ChangedFields)
}

// A server re-point (different host) leaves the persisted binlog/WAL position
// pointing at a position that does not exist on the new server. Rejected.
func TestIngestable_ValidateReplace_ServerRepointRejected(t *testing.T) {
	prior := ingestableSrc("t1", "postgres://host-a:5432/db", "id")
	next := ingestableSrc("t1", "postgres://host-b:5432/db", "id")

	var details *sql.SourceIdentityChangeError
	require.ErrorAs(t, next.ValidateReplace(prior), &details)
	require.Equal(t, []string{"connectionString"}, details.ChangedFields)
}

// A database-name change on the same host is also a re-point.
func TestIngestable_ValidateReplace_DatabaseChangeRejected(t *testing.T) {
	prior := ingestableSrc("t1", "postgres://host/db-a", "id")
	next := ingestableSrc("t1", "postgres://host/db-b", "id")

	var details *sql.SourceIdentityChangeError
	require.ErrorAs(t, next.ValidateReplace(prior), &details)
	require.Equal(t, []string{"connectionString"}, details.ChangedFields)
}

// A credential-only change (same host + database, rotated user/password) is NOT
// a re-point: the persisted position is still valid. Rejecting it would force a
// needless full re-snapshot on every routine password rotation.
func TestIngestable_ValidateReplace_CredentialRotationAllowed(t *testing.T) {
	prior := ingestableSrc("t1", "postgres://alice:pw1@host:5432/db", "id")
	next := ingestableSrc("t1", "postgres://bob:pw2@host:5432/db", "id")
	require.NoError(t, next.ValidateReplace(prior))
}

// The rejection carries no connection string — a secret. Neither the message nor
// the details payload may echo the host, user, or password.
func TestIngestable_ValidateReplace_SourceChangeCarriesNoSecret(t *testing.T) {
	prior := ingestableSrc("t1", "postgres://alice:hunter2@host-a/db", "id")
	next := ingestableSrc("t1", "postgres://alice:hunter2@host-b/db", "id")

	err := next.ValidateReplace(prior)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "hunter2")
	require.NotContains(t, err.Error(), "host-a")
	require.NotContains(t, err.Error(), "host-b")
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
