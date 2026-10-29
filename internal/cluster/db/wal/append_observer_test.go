package wal

import (
	"bytes"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// productionAppendObserver records what Storage appends, then passes it on.
type productionAppendObserver struct {
	eventlog.Appender
	calls   int
	records []eventlog.Record
}

func (o *productionAppendObserver) Append(records []eventlog.Record) error {
	o.calls++
	for _, r := range records {
		o.records = append(o.records, eventlog.Record{ID: r.ID, Payload: bytes.Clone(r.Payload)})
	}
	return o.Appender.Append(records)
}

// observedEntryStore routes an entry store's appends through an observer.
type observedEntryStore struct {
	entryStore
	appender eventlog.Appender
}

func (s *observedEntryStore) Append(records []eventlog.Record) error {
	return s.appender.Append(records)
}

func (s *observedEntryStore) LastAppended() (uint64, bool, error) { return s.appender.LastAppended() }
