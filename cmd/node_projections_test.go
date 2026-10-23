package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

func TestProjectionStoreMemory(t *testing.T) {
	t.Setenv("COMMITTED_PROJECTION_STORE_CACHE_BYTES", "")
	t.Setenv("COMMITTED_PROJECTION_STORE_MEMTABLE_BYTES", "")
	m, err := loadProjectionStoreMemory()
	require.NoError(t, err)
	require.Equal(t, stagestore.DefaultMemory, m)

	t.Setenv("COMMITTED_PROJECTION_STORE_CACHE_BYTES", "4294967296")
	t.Setenv("COMMITTED_PROJECTION_STORE_MEMTABLE_BYTES", " 134217728 ")
	m, err = loadProjectionStoreMemory()
	require.NoError(t, err)
	require.Equal(t, stagestore.Memory{CacheBytes: 4 << 30, MemTableBytes: 128 << 20}, m)

	for name, env := range map[string][2]string{
		"not a number":     {"256MiB", ""},
		"zero cache":       {"0", ""},
		"zero memtable":    {"", "0"},
		"memtable at 4GiB": {"", "4294967296"},
	} {
		t.Setenv("COMMITTED_PROJECTION_STORE_CACHE_BYTES", env[0])
		t.Setenv("COMMITTED_PROJECTION_STORE_MEMTABLE_BYTES", env[1])
		_, err := loadProjectionStoreMemory()
		require.Error(t, err, name)
	}
}
