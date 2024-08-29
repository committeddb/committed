package cluster

import (
	"context"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//go:generate protoc --go_out=paths=source_relative:. ./clusterpb/cluster.proto

// TODO There should be a single Propose(p *Proposal) error and then utility functions for preparing different types of proposals
type Cluster interface {
	Propose(p *Proposal) error
	ProposeType(t *Type) error
	ProposeDeleteType(id string) error
	ProposeSyncable(c *Configuration) error
	ProposeDatabase(c *Configuration) error
	Type(id string) (*Type, error)
	Close() error
	// The caller should run this on a separate go routine - or do we want to do this so close() can cancel all contexts?
	Sync(ctx context.Context, id string, s Syncable) error
	AddSyncableParser(name string, p SyncableParser)
	AddDatabaseParser(name string, p DatabaseParser)
}
