package http_test

import (
	"bytes"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/backup"
	"github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// GET /v1/node/backup streams an archive of the running node that
// `committed restore` reconstitutes into a node with the same applied
// state — taken while the engine serves.
func TestNodeBackup_StreamsARestorableArchive(t *testing.T) {
	e := newEngine(t)
	ids := []string{"ta", "tb", "tc", "td", "te"}
	for _, id := range ids {
		e.addType(t, id, id)
	}
	applied := e.d.AppliedIndex()
	require.Greater(t, applied, uint64(0))

	req := httptest.NewRequest("GET", "http://localhost/v1/node/backup", nil)
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Equal(t, "application/x-tar", w.Result().Header.Get("Content-Type"))
	require.Contains(t, w.Result().Header.Get("Content-Disposition"), "committed-node1-")

	target := filepath.Join(t.TempDir(), "restored")
	m, err := backup.Restore(bytes.NewReader(w.Body.Bytes()), target, time.Now())
	require.NoError(t, err)
	require.True(t, m.Live)
	require.Equal(t, uint64(1), m.NodeID)
	require.GreaterOrEqual(t, m.AppliedIndex, applied, "captured at or after the types were applied")

	restored, err := wal.Open(target, parser.New(), nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	t.Cleanup(func() { _ = restored.Close() })
	require.Equal(t, m.AppliedIndex, restored.AppliedIndex())
	cfgs, err := restored.Types()
	require.NoError(t, err)
	got := make([]string, 0, len(cfgs))
	for _, c := range cfgs {
		got = append(got, c.ID)
	}
	for _, id := range ids {
		require.Contains(t, got, id)
	}
}
