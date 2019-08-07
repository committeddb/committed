package syncable

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	"github.com/philborlin/committed/topic"
	"github.com/philborlin/committed/types"
	"github.com/pkg/errors"
)

// TODO We need to store the index of the last synced accepted proposal in the global snapshot

// TopicSyncable is a Syncable that includes the topics it syncs on
type TopicSyncable interface {
	Syncable
	topics() []string
}

// Syncable represents a synchable concept
type Syncable interface {
	Init(ctx context.Context) error
	Sync(ctx context.Context, entry *types.AcceptedProposal) error
	Close() error
}

// Bridge manages the interactions between a topic and a syncable
type Bridge struct {
	Name      string
	Syncable  Syncable
	topics    map[string]*topic.Topic
	lastIndex uint64
}

// BridgeSnapshot is the snapshot struct
type BridgeSnapshot struct {
	LastIndex uint64
}

// NewBridge creates a wrapper that will
func NewBridge(name string, s TopicSyncable, topics map[string]*topic.Topic) *Bridge {
	// Create a map that only has entries for the topics we are listening to
	var tmap = make(map[string]*topic.Topic)
	for _, item := range s.topics() {
		tmap[item] = topics[item]
	}

	return &Bridge{Name: name, Syncable: s, topics: tmap}
}

// GetSnapshot implements Snapshotter
func (b *Bridge) GetSnapshot() ([]byte, error) {
	s := &BridgeSnapshot{LastIndex: b.lastIndex}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s); err != nil {
		return nil, errors.Wrapf(err, "Could not encode snapshot %v", s)
	}

	return buf.Bytes(), nil
}

// ApplySnapshot implements Snapshotter
func (b *Bridge) ApplySnapshot(snap []byte) error {
	var s BridgeSnapshot
	dec := gob.NewDecoder(bytes.NewBuffer(snap))
	if err := dec.Decode(&s); err != nil {
		return errors.Wrap(err, "Could not decode snapshot")
	}

	b.lastIndex = s.LastIndex

	return nil
}

// Init implements Syncable
// To close the syncable send a message to the ctx.Done() channel
// It is the caller's responsibility to listen to any errors on the errorC channel passed in
func (b *Bridge) Init(ctx context.Context, errorC chan<- error) error {
	if len(b.topics) == 0 {
		return fmt.Errorf("[%s.bridge]No topics so there is nothing to sync", b.Name)
	}

	if len(b.topics) > 1 {
		// There is going to be some serious syncronization work that needs to happen to support multiple
		// topics. Deferring this until later.
		return fmt.Errorf("[%s.bridge] We don't support more than one topic in a syncable yet", b.Name)
	}

	err := b.Syncable.Init(ctx)
	if err != nil {
		return errors.Wrapf(err, "[%s.bridge] Init of internal syncable failed", b.Name)
	}

	for _, t := range b.topics {
		reader, err := t.NewReader(0)
		if err != nil {
			return errors.Wrapf(err, "[%s.bridge] Could not create reader", b.Name)
		}

		go func(t *topic.Topic) {
			for {
				select {
				case <-ctx.Done():
					if ctx.Err() != nil {
						errorC <- errors.Wrapf(err, "[%s.bridge] Context had an error", ctx.Err())
					}
					err := b.Syncable.Close()
					if err != nil {
						errorC <- errors.Wrapf(err, "[%s.bridge] Problem closing wrapped syncable", err)
					}
					return
				default:
					ap, err := reader.Next(ctx)
					if err != nil {
						errorC <- errors.Wrapf(err,
							"[%s.bridge] Problem getting the next accepted proposal from topic %s",
							err,
							t.Name)
					}
					b.Syncable.Sync(ctx, ap)
					b.lastIndex = ap.Index
				}
			}
		}(t)
	}

	return nil
}
