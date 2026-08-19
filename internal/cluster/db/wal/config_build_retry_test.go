package wal_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

// A config that validated at admission but failed its node-local build
// (the field case: a transient init-DDL deadline) used to stay degraded
// until a node restart. The retry pass must re-queue its build through
// the pump, keep the loud degraded evidence while the failure persists,
// and heal — worker started, evidence cleared — once the environment
// recovers.
func TestRetryDegradedBuilds_RequeuesUntilHealed(t *testing.T) {
	syncCh := make(chan *db.SyncableWithID, 8)
	p := parser.New()

	s := OpenStorage(t, t.TempDir(), p, syncCh, nil)
	defer s.Cleanup()

	// No degraded configs: a retry pass queues nothing.
	s.RetryDegradedBuildsForTest()
	select {
	case msg := <-syncCh:
		t.Fatalf("retry with nothing degraded queued %v", msg.ID)
	case <-time.After(50 * time.Millisecond):
	}

	// A config whose sub-parser is not registered degrades at its
	// deferred build — the transient-environment stand-in.
	ent, err := cluster.NewUpsertSyncableEntity(&cluster.Configuration{
		ID: "flaky", MimeType: "application/json",
		Data: []byte(`{"syncable": {"name": "flaky", "type": "later"}}`),
	})
	require.NoError(t, err)
	saveEntity(t, ent, s, 1, 1)
	select {
	case msg := <-syncCh:
		require.Nil(t, msg.Build(), "the build must degrade while the environment is broken")
	case <-time.After(2 * time.Second):
		t.Fatal("apply path did not queue the build")
	}
	require.Equal(t, 1, s.ConfigBuildErrorCount())

	// Environment still broken: the retry re-queues, the build still
	// degrades, the evidence stays.
	s.RetryDegradedBuildsForTest()
	select {
	case msg := <-syncCh:
		require.Equal(t, "flaky", msg.ID)
		require.NotNil(t, msg.Build)
		require.Nil(t, msg.Build())
	case <-time.After(2 * time.Second):
		t.Fatal("retry did not re-queue the degraded build")
	}
	require.Equal(t, 1, s.ConfigBuildErrorCount())

	// Environment heals (the sub-parser appears): the next retry's build
	// succeeds and clears the evidence.
	sp := &clusterfakes.FakeSyncableParser{}
	sp.ParseReturns(&clusterfakes.FakeSyncable{}, nil)
	p.AddSyncableParser("later", sp)

	s.RetryDegradedBuildsForTest()
	select {
	case msg := <-syncCh:
		require.Equal(t, "flaky", msg.ID)
		require.NotNil(t, msg.Build(), "a healed environment must build")
	case <-time.After(2 * time.Second):
		t.Fatal("retry did not re-queue after heal")
	}
	require.Equal(t, 0, s.ConfigBuildErrorCount(), "a successful build clears the degraded evidence")

	// Healed: the next pass queues nothing.
	s.RetryDegradedBuildsForTest()
	select {
	case msg := <-syncCh:
		t.Fatalf("retry after heal queued %v", msg.ID)
	case <-time.After(50 * time.Millisecond):
	}
}
