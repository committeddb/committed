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

// Topic represents a topic in the system
type Topic struct {
	Name         string
	WalDir       string
	wal          types.WAL
	reg          prometheus.Registerer
	readerAlerts []*topicReaderAlert

	// Provides a lookup table that given a segment, returns the first raft index in that segment
	// This is used to find which segment to start with when NewReader is given an index
	// This needs to be in the snapshot
	FirstIndexInSegment map[int]uint64
}

// Data represets the data that will be stored in the topic wal
type Data struct {
	Index uint64
	Term  uint64
	Data  string
}

// New creates a new topic
func New(name string, baseDir string) (*Topic, error) {
	walDir := filepath.Join(baseDir, name)
	oneMB := 1 * 1024 * 1024
	w, err := walFactory.NewSize(nil, nil, walDir, oneMB, false)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not create wal for topic %s", name)
	}
	return &Topic{Name: name, WalDir: walDir, wal: w}, nil
}

// Restore creates a topic with an existing wal
func Restore(name string, walDir string) (*Topic, error) {
	w, err := walFactory.Open(nil, nil, walDir)
	if err != nil {
		return nil, errors.Wrapf(err, "Could no open wal for topic %s", name)
	}
	return &Topic{Name: name, WalDir: walDir, wal: w}, nil
}

// Close closes the underlying wal
func (t *Topic) Close() error {
	return t.wal.Close()
}

// Append appends to the wal of this topic
func (t *Topic) Append(data Data) error {
	var buf bytes.Buffer

	_ = gob.NewEncoder(&buf).Encode(data)

	if err := t.wal.Log(buf.Bytes()); err != nil {
		return errors.Wrapf(err, "Error appending to wal for topic: %s", t.Name)
	}

	for _, a := range t.readerAlerts {
		if a.waiting {
			a.appendC <- true
		}
	}

	return nil
}

// NewReader gets a reader of a wal
func (t *Topic) NewReader(index uint64) (Reader, error) {
	return t.newWalReader(index)
}

// TODO This is a testing only function. Can we test a different way?
func (t *Topic) addReaderAlert(a *topicReaderAlert) {
	t.readerAlerts = append(t.readerAlerts, a)
}

// Reader has methods that will read data from the wal
type Reader interface {
	Next(ctx context.Context) (*types.AcceptedProposal, error)
}

type walTopicReader struct {
	r              types.LiveReader
	topic          *Topic
	currentSegment int
	alert          *topicReaderAlert
}

type topicReaderAlert struct {
	waiting bool
	appendC chan bool
}

func (t *Topic) newWalReader(index uint64) (*walTopicReader, error) {
	// TODO Find the segment with the highest index that doesn't go over index
	firstSegment := 0
	highestIndexWithoutGoingOver := uint64(0)
	for k := range t.FirstIndexInSegment {
		nextIndex := t.FirstIndexInSegment[k]
		if nextIndex > highestIndexWithoutGoingOver && nextIndex <= index {
			highestIndexWithoutGoingOver = nextIndex
			firstSegment = k
		}
	}

	alert := &topicReaderAlert{appendC: make(chan bool)}
	t.addReaderAlert(alert)
	walTopicReader := &walTopicReader{topic: t, alert: alert}
	lr, err := walTopicReader.newLiveReader(firstSegment)
	if err != nil {
		return nil, errors.Wrap(err, "Could not create walTopicReader")
	}
	walTopicReader.r = lr
	walTopicReader.currentSegment = firstSegment

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
			fmt.Print(errors.Wrapf(err, "[Topic: %s] found a wal record that could not be decoded, skipping", r.topic.Name))
			return r.next(ctx, newSegment)
		}

		if newSegment {
			r.topic.FirstIndexInSegment[r.currentSegment] = ap.Index
		}

		return ap, nil
	}

	if r.r.Err() == io.EOF {
		_, last, err := r.topic.wal.Segments()
		if err != nil {
			return nil, errors.Wrapf(err, "[Topic: %s] next() could not get the number of segments", r.topic.Name)
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

		r.alert.waiting = true
		<-r.alert.appendC
		return r.next(ctx, false)
	}

	return nil, errors.Wrapf(r.r.Err(), "[Topic: %s] the wal is corrupt", r.topic.Name)
}

func (r *walTopicReader) newLiveReader(firstSegment int) (types.LiveReader, error) {
	_, lastSegment, err := r.topic.wal.Segments()
	if err != nil {
		return nil, errors.Wrapf(err, "[Topic: %s] newLiveReader() could not get the number of segments", r.topic.Name)
	}

	// Look for the highest segment in the FirstIndexInSegment map
	m := r.topic.FirstIndexInSegment
	for !inMap(m, lastSegment) {
		if lastSegment == firstSegment {
			break
		}
		lastSegment = lastSegment - 1
	}

	sr := wal.SegmentRange{Dir: r.topic.WalDir, First: firstSegment, Last: lastSegment}
	srr, err := walFactory.NewSegmentsRangeReader(sr)
	if err != nil {
		return nil, errors.Wrapf(err, "[Topic: %s] could not create NewSegmentsRangeReader", r.topic.Name)
	}

	lr := walFactory.NewLiveReader(nil, r.topic.reg, srr)

	return lr, nil
}

func inMap(m map[int]uint64, segment int) bool {
	_, ok := m[segment]
	return ok
}
