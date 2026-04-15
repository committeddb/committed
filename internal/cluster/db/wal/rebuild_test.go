package wal_test

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/philborlin/committed/internal/cluster/db/wal"
	"github.com/stretchr/testify/require"
)

// TestRebuild_EndToEnd simulates the operator-facing rebuild
// procedure from docs/operations/rebuild.md: when a node can't
// continue, stop it, blow away its data directory, copy a healthy
// peer's data directory in place (the rsync step), and restart.
//
// The test exercises every part of that flow locally:
//
//  1. Build a "healthy" node with applied state (a Type + a database
//     configured through the normal Apply path).
//  2. Build a "failed" node with only partial state.
//  3. Close both handles. Simulate the fatal-exit by leaving the
//     failed node's directory behind; it won't be touched again.
//  4. Copy the healthy node's directory over the failed node's —
//     the rsync analogue.
//  5. Reopen the failed node's directory. The node should come up
//     clean with the healthy node's AppliedIndex / EventIndex, and
//     every metadata query should return what the healthy node
//     had.
//
// The invariant P_local == R_local must hold after reopen (the
// rebuild story falls apart if it doesn't: the next Ready iteration
// would fatal-exit).
func TestRebuild_EndToEnd(t *testing.T) {
	healthyDir := t.TempDir()
	failedDir := t.TempDir()

	// --- Step 1: build the healthy node. -----------------------------
	healthy, err := wal.Open(healthyDir, parser.New(), nil, nil, testOpenOptions...)
	require.Nil(t, err)

	tp, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "events", Name: "Events", Version: 1})
	require.Nil(t, err)
	saveEntity(t, tp, healthy, 1, 1)

	// Also apply a user event, so the permanent event log has more
	// than just metadata to copy.
	evt := &cluster.Entity{
		Type: &cluster.Type{ID: "events"},
		Key:  []byte("k1"),
		Data: []byte("payload"),
	}
	saveEntity(t, evt, healthy, 1, 2)

	require.Equal(t, uint64(2), healthy.AppliedIndex())
	require.Equal(t, uint64(2), healthy.EventIndex())
	require.Nil(t, healthy.Close())

	// --- Step 2: build the failed node with diverged state. ---------
	failed, err := wal.Open(failedDir, parser.New(), nil, nil, testOpenOptions...)
	require.Nil(t, err)

	// Apply only one entry — half what the healthy node has. This
	// simulates a follower that fell behind before its disk was
	// declared unrecoverable (or a brand-new node that has barely
	// started).
	saveEntity(t, tp, failed, 1, 1)
	require.Equal(t, uint64(1), failed.AppliedIndex())
	require.Nil(t, failed.Close())

	// --- Step 3–4: rsync the healthy directory over the failed. -----
	// Clear the failed dir first — rm -rf /var/lib/committed/* from
	// the runbook.
	require.Nil(t, os.RemoveAll(failedDir))
	require.Nil(t, os.MkdirAll(failedDir, 0o755))
	copyDir(t, healthyDir, failedDir)

	// --- Step 5: restart the failed node, now holding rsync'd state.
	recovered, err := wal.Open(failedDir, parser.New(), nil, nil, testOpenOptions...)
	require.Nil(t, err)
	defer recovered.Close()

	// The rebuilt node should see everything the healthy node had.
	require.Equal(t, uint64(2), recovered.AppliedIndex(),
		"AppliedIndex should match the healthy peer after rebuild")
	require.Equal(t, uint64(2), recovered.EventIndex(),
		"EventIndex should match the healthy peer after rebuild")
	require.Equal(t, recovered.EventIndex(), recovered.AppliedIndex(),
		"storage invariant P_local == R_local must hold on a fresh rebuild")

	// Queries through the normal API should return the migrated
	// metadata.
	got, err := recovered.Type("events")
	require.Nil(t, err)
	require.Equal(t, "events", got.ID)
	require.Equal(t, "Events", got.Name)

	// The Reader should be able to stream the events the healthy
	// node had applied — this is the path a syncable would use.
	r := recovered.Reader("fresh-syncable")
	var count int
	for {
		_, _, err := r.Read()
		if err == io.EOF {
			break
		}
		require.Nil(t, err)
		count++
	}
	// Reader filters only SyncableIndex proposals (see reader.go); a
	// Type entity and a user event both flow through, so a fresh
	// syncable sees 2.
	require.Equal(t, 2, count, "rebuilt node should replay every non-internal entry through the Reader")
}

// copyDir replicates the rsync from the healthy peer: walks src and
// reproduces every file and directory under dst. Doesn't preserve
// permissions beyond what runbook consumers care about (bbolt + wal
// files stay readable by the same process that created them).
func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	require.Nil(t, filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode())
	}))
}
