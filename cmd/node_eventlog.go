package cmd

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/pkg/segmentlog"
)

const defaultSegmentedCacheBytes = 160 << 20

// loadEventLogOptions selects the segmented event log, the only engine a node
// runs (wal refuses a tidwall log and names the conversion). Cache settings are
// runtime budgets, not a promise to allocate that memory on startup.
func loadEventLogOptions() ([]wal.Option, error) {
	recent, err := eventCacheBytesEnv("COMMITTED_EVENT_CACHE_RECENT_BYTES")
	if err != nil {
		return nil, err
	}
	historical, err := eventCacheBytesEnv("COMMITTED_EVENT_CACHE_HISTORICAL_BYTES")
	if err != nil {
		return nil, err
	}
	return []wal.Option{wal.WithSegmentedEventLog(segmentlog.LogOptions{
		Encoding: segmentlog.Options{Compression: segmentlog.ZstdDefault},
		Cache:    segmentlog.CacheOptions{RecentBytes: recent, HistoricalBytes: historical},
	})}, nil
}

func eventCacheBytesEnv(name string) (uint64, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return defaultSegmentedCacheBytes, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a non-negative integer number of bytes", name)
	}
	return value, nil
}
