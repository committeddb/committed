package syncable

import (
	"bytes"
	"context"
	"encoding/gob"

	"github.com/philborlin/committed/types"
)

// TopicSyncable is a Syncable that includes the topics it syncs on
type TopicSyncable interface {
	Syncable
	topics() []string
}

// Syncable represents a synchable concept
type Syncable interface {
	Init() error
	Sync(ctx context.Context, bytes []byte) error
	Close() error
}

type syncableWrapper struct {
	Syncable TopicSyncable
	topics   map[string]struct{}
}

func newSyncableWrapper(s TopicSyncable) *syncableWrapper {
	var topics = make(map[string]struct{})
	for _, item := range s.topics() {
		topics[item] = struct{}{}
	}
	return &syncableWrapper{s, topics}
}

func (s syncableWrapper) containsTopic(t string) bool {
	_, ok := s.topics[t]
	return ok
}

// Init implements Syncable
func (s syncableWrapper) Init() error {
	return nil
}

// Sync implements Syncable
func (s syncableWrapper) Sync(ctx context.Context, byteSlice []byte) error {
	p, err := decode(byteSlice)
	if err == nil && s.containsTopic(p.Topic) {
		return s.Syncable.Sync(ctx, []byte(p.Proposal))
	}

	return err
}

// Close implements Syncable
func (s syncableWrapper) Close() error {
	return s.Close()
}

func decode(byteSlice []byte) (types.Proposal, error) {
	p := types.Proposal{}
	err := gob.NewDecoder(bytes.NewReader(byteSlice)).Decode(&p)
	return p, err
}
