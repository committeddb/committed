package syncable

import (
	"context"
)

// type TopicSyncable struct {
// 	Syncable Syncable
// 	Topics   []string
// }

// TopicSyncable is a Syncable that includes the topics it syncs on
type TopicSyncable interface {
	Syncable
	Topics() []string
}

// Syncable represents a synchable concept
type Syncable interface {
	Sync(ctx context.Context, bytes []byte) error
	Close() error
}
