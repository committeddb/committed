package syncable

import "context"

// Syncable represents a synchable concept
type Syncable interface {
	Sync(ctx context.Context, bytes []byte) error
	Close() error
}
