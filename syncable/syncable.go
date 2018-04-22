package syncable

import (
	"bytes"
	"context"
	"encoding/gob"

	"github.com/philborlin/committed/util"
)

// TopicSyncable is a Syncable that includes the topics it syncs on
type TopicSyncable interface {
	Syncable
	topics() []string
}

// Syncable represents a synchable concept
type Syncable interface {
	Sync(ctx context.Context, bytes []byte) error
	Close() error
}

type syncableWrapper struct {
	syncable TopicSyncable
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

func (s syncableWrapper) Sync(ctx context.Context, byteSlice []byte) error {
	p := util.Proposal{}
	err := gob.NewDecoder(bytes.NewReader(byteSlice)).Decode(p)
	if err == nil && s.containsTopic(p.Topic) {
		return s.syncable.Sync(ctx, byteSlice)
	}

	return nil
}

func (s syncableWrapper) Close() error {
	return s.Close()
}
