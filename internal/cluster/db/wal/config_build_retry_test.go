package wal_test

import (
	"errors"
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
	// An ENVIRONMENTAL failure: the sub-parser is registered but its
	// destination is unreachable — the class the retry loop exists for.
	sp := &clusterfakes.FakeSyncableParser{}
	sp.ParseReturns(nil, errors.New("dial tcp 10.0.0.9:5432: connection refused"))
	p.AddSyncableParser("flakysink", sp)

	s := OpenStorage(t, t.TempDir(), p, syncCh, nil)
	defer s.Cleanup()

	// No degraded configs: a retry pass queues nothing.
	s.RetryDegradedBuildsForTest()
	select {
	case msg := <-syncCh:
		t.Fatalf("retry with nothing degraded queued %v", msg.ID)
	case <-time.After(50 * time.Millisecond):
	}

	ent, err := cluster.NewUpsertSyncableEntity(&cluster.Configuration{
		ID: "flaky", MimeType: "application/json",
		Data: []byte(`{"syncable": {"name": "flaky", "type": "flakysink"}}`),
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
	require.False(t, s.ConfigBuildErrors()[0].NotAdmissible, "an environmental failure is retryable")

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

	// Environment heals (the destination comes back): the next retry's
	// build succeeds and clears the evidence.
	sp.ParseReturns(&clusterfakes.FakeSyncable{}, nil)

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

// The field zombie's fix: a DETERMINISTIC admission rejection (here an
// unregistered syncable type — same class as a config invalidated by a
// later binary's tightened rules) parks loudly instead of retrying
// forever. The evidence stays visible with NotAdmissible set; the retry
// pass queues nothing; only a re-POST or delete resolves it.
func TestRetryDegradedBuilds_NotAdmissibleParks(t *testing.T) {
	syncCh := make(chan *db.SyncableWithID, 8)
	p := parser.New()

	s := OpenStorage(t, t.TempDir(), p, syncCh, nil)
	defer s.Cleanup()

	ent, err := cluster.NewUpsertSyncableEntity(&cluster.Configuration{
		ID: "legacy", MimeType: "application/json",
		Data: []byte(`{"syncable": {"name": "legacy", "type": "removed-kind"}}`),
	})
	require.NoError(t, err)
	saveEntity(t, ent, s, 1, 1)
	select {
	case msg := <-syncCh:
		require.Nil(t, msg.Build(), "the build degrades — the config cannot parse under this binary")
	case <-time.After(2 * time.Second):
		t.Fatal("apply path did not queue the build")
	}
	require.Equal(t, 1, s.ConfigBuildErrorCount())
	require.True(t, s.ConfigBuildErrors()[0].NotAdmissible,
		"a deterministic rejection is marked not-admissible")

	// The retry pass PARKS it: nothing queued, ever.
	s.RetryDegradedBuildsForTest()
	select {
	case msg := <-syncCh:
		t.Fatalf("a not-admissible config must never be retried, but %v was queued", msg.ID)
	case <-time.After(100 * time.Millisecond):
	}
	require.Equal(t, 1, s.ConfigBuildErrorCount(), "the evidence stays loudly visible")
}
