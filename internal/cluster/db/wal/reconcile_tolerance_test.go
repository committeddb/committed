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

// TestReconcileSyncableList_DegradedConfigsArePresent pins reconcile-robustness:
// a config that fails to parse on this node must NOT abort the whole reconcile
// list — it is returned present-but-degraded (nil Syncable), so the good
// configs still install and the degraded one keeps its worker rather than being
// cancelled as a phantom delete.
func TestReconcileSyncableList_DegradedConfigsArePresent(t *testing.T) {
	syncCh := make(chan *db.SyncableWithID, 8)
	p := parser.New()
	sp := &clusterfakes.FakeSyncableParser{}
	sp.ParseReturns(&clusterfakes.FakeSyncable{}, nil)
	p.AddSyncableParser("test", sp)

	s := OpenStorage(t, t.TempDir(), p, syncCh, nil)
	defer s.Cleanup()

	// A parseable config (type "test") — its apply-path worker send is drained.
	goodEnt, err := cluster.NewUpsertSyncableEntity(&cluster.Configuration{
		ID: "good", MimeType: "application/json",
		Data: []byte(`{"syncable": {"name": "good", "type": "test"}}`),
	})
	require.NoError(t, err)
	saveEntity(t, goodEnt, s, 1, 1)
	select {
	case <-syncCh:
	case <-time.After(2 * time.Second):
		t.Fatal("apply path did not send the good syncable")
	}

	// An unparseable config (unknown type) — saveSyncable degrades it (no worker
	// send), and it lives in the bucket as a degraded config.
	badEnt, err := cluster.NewUpsertSyncableEntity(&cluster.Configuration{
		ID: "bad", MimeType: "application/json",
		Data: []byte(`{"syncable": {"name": "bad", "type": "nonexistent"}}`),
	})
	require.NoError(t, err)
	saveEntity(t, badEnt, s, 1, 2)

	s.RequestSyncReconcile()
	var req *db.SyncableWithID
	select {
	case req = <-syncCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no reconcile request sent")
	}
	require.NotNil(t, req.ReconcileList)

	listed, err := req.ReconcileList()
	require.NoError(t, err, "one unparseable config must not abort the whole reconcile list")

	byID := make(map[string]*db.SyncableWithID, len(listed))
	for _, e := range listed {
		byID[e.ID] = e
	}
	require.Contains(t, byID, "good")
	require.NotNil(t, byID["good"].Syncable, "a parseable config carries its syncable")
	require.Contains(t, byID, "bad", "an unparseable config must still be PRESENT")
	require.Nil(t, byID["bad"].Syncable, "an unparseable config is present-but-degraded (nil syncable)")
}
