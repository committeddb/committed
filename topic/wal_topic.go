package topic

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"path/filepath"

	"github.com/philborlin/committed/types"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/wal"
)

var walFactory types.WALFactory = &types.TSDBWALFactory{}

// WALTopic is a topic backed by a WAL
type WALTopic struct {
	name         string
	walDir       string
	wal          types.WAL
	reg          prometheus.Registerer
	readerAlerts []*topicReaderAlert

	// Provides a lookup table that given a segment, returns the first raft index in that segment
	// This is used to find which segment to start with when NewReader is given an index
	// This needs to be in the snapshot
	// This should not be published over the raft because different nodes may record their entries
	// in different segments depending on when truncations occur based on node restarts and/or flushes.
	firstIndexInSegment map[int]uint64
}

// New creates a new topic
func New(name string, baseDir string) (*WALTopic, error) {
	walDir := filepath.Join(baseDir, name)
	oneMB := 1 * 1024 * 1024
	w, err := walFactory.NewSize(nil, nil, walDir, oneMB, false)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not create wal for topic %s", name)
	}
	m := make(map[int]uint64)
	return &WALTopic{name: name, walDir: walDir, wal: w, firstIndexInSegment: m}, nil
}

// Restore creates a topic with an existing wal
func Restore(name string, walDir string) (*WALTopic, error) {
	w, err := walFactory.Open(nil, nil, walDir)
	if err != nil {
		return nil, errors.Wrapf(err, "Could no open wal for topic %s", name)
	}
	m := make(map[int]uint64)
	return &WALTopic{name: name, walDir: walDir, wal: w, firstIndexInSegment: m}, nil
}

// Append appends to the wal of this topic
func (t *WALTopic) Append(data Data) error {
	var buf bytes.Buffer

	_ = gob.NewEncoder(&buf).Encode(data)

	if err := t.wal.Log(buf.Bytes()); err != nil {
		return errors.Wrapf(err, "Error appending to wal for topic: %s", t.name)
	}

	for _, a := range t.readerAlerts {
		if a.waiting {
			a.appendC <- true
		}
	}

	return nil
}

// Close closes the underlying wal
func (t *WALTopic) Close() error {
	return t.wal.Close()
}

// Name implements Topic
func (t *WALTopic) Name() string {
	return t.name
}

// NewReader gets a reader of a wal
// TODO The index might not exist in this wal so we want to find either the index or the next index after the one given
// This is because during recovery in a multi-topic syncable we may not know exactly what we are looking for and
// we are just checking to make sure we didn't miss anything.
func (t *WALTopic) NewReader(index uint64) (Reader, error) {
	return t.newWalReader(index)
}

// TODO This is a testing only function. Can we test a different way?
func (t *WALTopic) addReaderAlert(a *topicReaderAlert) {
	t.readerAlerts = append(t.readerAlerts, a)
}

type walTopicReader struct {
	r              types.LiveReader
	topic          *WALTopic
	currentSegment int
	alert          *topicReaderAlert
}

type topicReaderAlert struct {
	waiting bool
	appendC chan bool
}

func (t *WALTopic) newWalReader(index uint64) (*walTopicReader, error) {
	// TODO Find the segment with the highest index that doesn't go over index
	currentSegment := 0
	highestIndexWithoutGoingOver := uint64(0)
	for k := range t.firstIndexInSegment {
		nextIndex := t.firstIndexInSegment[k]
		if nextIndex > highestIndexWithoutGoingOver && nextIndex <= index {
			highestIndexWithoutGoingOver = nextIndex
			currentSegment = k
		}
	}

	alert := &topicReaderAlert{appendC: make(chan bool)}
	t.addReaderAlert(alert)
	walTopicReader := &walTopicReader{topic: t, alert: alert}
	lr, err := walTopicReader.newLiveReader(currentSegment)
	if err != nil {
		return nil, errors.Wrap(err, "Could not create walTopicReader")
	}
	walTopicReader.r = lr
	walTopicReader.currentSegment = currentSegment

	return walTopicReader, nil
}

// Next gets the next AcceptedProposal or blocks
func (r *walTopicReader) Next(ctx context.Context) (*types.AcceptedProposal, error) {
	return r.next(ctx, false)
}

func (r *walTopicReader) next(ctx context.Context, newSegment bool) (*types.AcceptedProposal, error) {
	if r.r.Next() {
		ap, err := types.NewAcceptedProposal(r.r.Record())
		if err != nil {
			fmt.Print(errors.Wrapf(err, "[Topic: %s] found a wal record that could not be decoded, skipping", r.topic.name))
			return r.next(ctx, newSegment)
		}

		if newSegment {
			r.topic.firstIndexInSegment[r.currentSegment] = ap.Index
		}

		return ap, nil
	}

	if r.r.Err() == io.EOF {
		_, last, err := r.topic.wal.Segments()
		if err != nil {
			return nil, errors.Wrapf(err, "[Topic: %s] next() could not get the number of segments", r.topic.name)
		}

		if last > r.currentSegment {
			lr, err := r.newLiveReader(r.currentSegment + 1)
			if err != nil {
				return nil, err
			}

			r.currentSegment = r.currentSegment + 1
			r.r = lr
			return r.next(ctx, true)
		}

		// TODO Figure out how to handle ctx.Done()
		r.alert.waiting = true
		<-r.alert.appendC
		return r.next(ctx, false)
	}

	return nil, errors.Wrapf(r.r.Err(), "[Topic: %s] the wal is corrupt", r.topic.name)
}

func (r *walTopicReader) newLiveReader(firstSegment int) (types.LiveReader, error) {
	_, lastSegment, err := r.topic.wal.Segments()
	if err != nil {
		return nil, errors.Wrapf(err, "[Topic: %s] newLiveReader() could not get the number of segments", r.topic.name)
	}

	// Look for the highest segment in the firstIndexInSegment map
	m := r.topic.firstIndexInSegment
	for !inMap(m, lastSegment) {
		if lastSegment == firstSegment {
			break
		}
		lastSegment = lastSegment - 1
	}

	// sr := wal.SegmentRange{Dir: r.topic.walDir, First: firstSegment, Last: lastSegment}
	// We want to do one segment at a time because we can't track which segment we are on
	// to update the currentSegment. If we lose the current segment we will unnecessarily
	// replay the same data to each syncable attached to the topic
	sr := wal.SegmentRange{Dir: r.topic.walDir, First: firstSegment, Last: firstSegment}
	srr, err := walFactory.NewSegmentsRangeReader(sr)
	if err != nil {
		return nil, errors.Wrapf(err, "[Topic: %s] could not create NewSegmentsRangeReader", r.topic.name)
	}

	lr := walFactory.NewLiveReader(nil, r.topic.reg, srr)

	return lr, nil
}

func inMap(m map[int]uint64, segment int) bool {
	_, ok := m[segment]
	return ok
}
