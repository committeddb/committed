package cmd

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

type kindRecorder struct{ kinds []string }

func (r *kindRecorder) AddSyncableParser(name string, _ cluster.SyncableParser) {
	r.kinds = append(r.kinds, name)
}

// TestRegisterSyncableKinds pins the node's syncable registration to the
// declared list, and the list to the packages under internal/cluster/syncable
// that are kinds — so a kind added to the tree cannot be left unwired (its
// configs would be refused at POST as an unknown type, silently to the
// author of the package).
func TestRegisterSyncableKinds(t *testing.T) {
	rec := &kindRecorder{}
	registerSyncableKinds(rec, nil, nil, t.TempDir())
	require.Equal(t, syncableKinds, rec.kinds)

	// Every kind package under syncable/ is registered. Not every package
	// there is a kind: stages and stagestore are the projection runtime, and
	// sql holds two kinds (the mirror and the projection).
	notKinds := map[string]bool{"stages": true, "stagestore": true}
	registered := map[string]bool{}
	for _, k := range syncableKinds {
		registered[k] = true
	}
	entries, err := os.ReadDir(filepath.Join("..", "internal", "cluster", "syncable"))
	require.NoError(t, err)
	var missing []string
	for _, e := range entries {
		if !e.IsDir() || notKinds[e.Name()] || registered[e.Name()] {
			continue
		}
		missing = append(missing, e.Name())
	}
	sort.Strings(missing)
	require.Empty(t, missing, "syncable packages with no registered kind")
}
