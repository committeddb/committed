package cmd

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// loadProjectionStoreMemory reads the projection stage store's memory
// budget, defaulting each part to stagestore.DefaultMemory.
func loadProjectionStoreMemory() (stagestore.Memory, error) {
	m := stagestore.DefaultMemory
	var err error
	if m.CacheBytes, err = bytesEnv("COMMITTED_PROJECTION_STORE_CACHE_BYTES", m.CacheBytes); err != nil {
		return m, err
	}
	if m.MemTableBytes, err = bytesEnv("COMMITTED_PROJECTION_STORE_MEMTABLE_BYTES", m.MemTableBytes); err != nil {
		return m, err
	}
	if err := m.Validate(); err != nil {
		return m, fmt.Errorf("projection store memory: %w", err)
	}
	return m, nil
}

func bytesEnv(name string, fallback uint64) (uint64, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a non-negative integer number of bytes", name)
	}
	return value, nil
}
