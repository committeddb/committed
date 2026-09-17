// Package segmented implements eventlog.EventLog using pkg/segmentlog.
package segmented

import (
	"bytes"
	"context"
	"errors"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/pkg/segmentlog"
)

type Log struct{ log *segmentlog.Log }

var _ eventlog.EventLog = (*Log)(nil)

// Wrap transfers ownership of an existing managed engine to this adapter.
func Wrap(log *segmentlog.Log) *Log { return &Log{log: log} }

func Create(path string, start uint64, opts segmentlog.LogOptions) (*Log, error) {
	l, e := segmentlog.CreateLog(path, start, opts)
	if e != nil {
		return nil, translate(e)
	}
	return Wrap(l), nil
}

func Open(path string, opts segmentlog.Options) (*Log, error) {
	l, e := segmentlog.OpenLog(path, opts)
	if e != nil {
		return nil, translate(e)
	}
	return Wrap(l), nil
}

// CreateBolt creates an experimental segmented log with a bbolt metadata catalog.
func CreateBolt(path string, start uint64, opts segmentlog.LogOptions) (*Log, error) {
	l, e := segmentlog.CreateBoltLog(path, start, opts)
	if e != nil {
		return nil, translate(e)
	}
	return Wrap(l), nil
}

// OpenBolt opens only the experimental bbolt catalog format.
func OpenBolt(path string, opts segmentlog.Options) (*Log, error) {
	l, e := segmentlog.OpenBoltLog(path, opts)
	if e != nil {
		return nil, translate(e)
	}
	return Wrap(l), nil
}

func translate(err error) error {
	if err == nil {
		return nil
	}
	pairs := [][2]error{{segmentlog.ErrNotFound, eventlog.ErrNotFound}, {segmentlog.ErrInvalid, eventlog.ErrInvalid}, {segmentlog.ErrCorrupt, eventlog.ErrCorrupt}, {segmentlog.ErrClosed, eventlog.ErrClosed}, {segmentlog.ErrLogPoisoned, eventlog.ErrPoisoned}, {segmentlog.ErrLocked, eventlog.ErrLocked}, {segmentlog.ErrUnsupported, eventlog.ErrUnsupported}}
	for _, p := range pairs {
		if errors.Is(err, p[0]) {
			err = errors.Join(p[1], err)
		}
	}
	return err
}

func (l *Log) Append(records []eventlog.Record) error {
	raw := make([]segmentlog.Record, len(records))
	for i, r := range records {
		raw[i] = segmentlog.Record{ID: r.ID, Payload: r.Payload}
	}
	return translate(l.log.Append(raw))
}

func (l *Log) LastAppended() (uint64, bool, error) {
	id, ok, e := l.log.LastAppended()
	return id, ok, translate(e)
}

func converted(r segmentlog.Record, e error) (eventlog.Record, error) {
	return eventlog.Record{ID: r.ID, Payload: r.Payload}, translate(e)
}
func (l *Log) Read(id uint64) (eventlog.Record, error) { return converted(l.log.Read(id)) }
func (l *Log) Seek(id uint64) (eventlog.Record, error) { return converted(l.log.Seek(id)) }
func (l *Log) Scan(ctx context.Context, b eventlog.Coverage, visit func(eventlog.Record) error) error {
	if visit == nil {
		return eventlog.ErrInvalid
	}
	return translate(l.log.Scan(ctx, segmentlog.Coverage{Start: b.Start, End: b.End}, func(r segmentlog.Record) error { return visit(eventlog.Record{ID: r.ID, Payload: r.Payload}) }))
}

func (l *Log) Rewrite(ctx context.Context, generation uint64, transform eventlog.Transform) (result eventlog.RewriteResult, err error) {
	if transform == nil {
		return result, eventlog.ErrInvalid
	}
	r, e := l.log.Rewrite(ctx, generation, func(r segmentlog.Record) ([]byte, bool, error) {
		original := bytes.Clone(r.Payload)
		payload, keep, e := transform(eventlog.Record{ID: r.ID, Payload: r.Payload})
		if e == nil && (!keep || !bytes.Equal(original, payload)) {
			result.ChangedRecords++
		}
		return payload, keep, e
	})
	result.Published = r.Published
	return result, translate(e)
}

func (l *Log) Reclaim(ctx context.Context) (eventlog.ReclaimResult, error) {
	r, e := l.log.Reclaim(ctx)
	return eventlog.ReclaimResult{RemovedFiles: r.RemovedFiles, RemovedBytes: r.RemovedBytes, SkippedEntries: r.SkippedEntries}, translate(e)
}
func (l *Log) Close() error { return translate(l.log.Close()) }
