package syncable

import (
	"context"

	"github.com/philborlin/committed/types"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

// Syncable represents a synchable concept
//counterfeiter:generate . Syncable
type Syncable interface {
	Init(ctx context.Context) error
	Sync(ctx context.Context, entry *types.AcceptedProposal) error
	Close() error
	Topics() []string
}

// TestSyncable returns a test implementation of the Syncable interface
type TestSyncable struct {
	topics []string
}

// Init implements Syncable
func (s *TestSyncable) Init(ctx context.Context) error {
	return nil
}

// Sync implements Syncable
func (s *TestSyncable) Sync(ctx context.Context, entry *types.AcceptedProposal) error {
	return nil
}

// Close implements Syncable
func (s *TestSyncable) Close() error {
	return nil
}

// Topics implements Syncable
func (s *TestSyncable) Topics() []string {
	return s.topics
}
