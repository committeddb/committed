package wal

import (
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// Backend construction belongs in test fixtures, not the application adapter.
func eventLogTestBackends() map[string]func(string) (eventlog.EventLog, error) {
	return map[string]func(string) (eventlog.EventLog, error){
		"tidwall": func(path string) (eventlog.EventLog, error) {
			return tidwall.Create(path, 1, tidwall.Options{SegmentBytes: 128, Compress: true})
		},
		"segmented-cached": func(path string) (eventlog.EventLog, error) {
			return segmented.Create(path, 1, segmentlog.LogOptions{SegmentBytes: 128, Encoding: segmentlog.Options{Compression: segmentlog.ZstdDefault}, Cache: segmentlog.CacheOptions{RecentBytes: 1024, HistoricalBytes: 1024}})
		},
		"segmented": func(path string) (eventlog.EventLog, error) {
			return segmented.Create(path, 1, segmentlog.LogOptions{SegmentBytes: 128, Encoding: segmentlog.Options{Compression: segmentlog.ZstdDefault}})
		},
	}
}
