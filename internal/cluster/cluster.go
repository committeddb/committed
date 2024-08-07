package cluster

import "context"

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//go:generate protoc --go_out=paths=source_relative:. ./cluster.proto
//go:generate protoc --go_out=paths=source_relative:. ./clusterpb/cluster.proto

type Cluster interface {
	Propose(p *Proposal)
	ProposeType(t *Type) error
	ProposeDeleteType(id string) error
	Type(id string) (*Type, error)
	Close() error
	// The caller should run this on a separate go routine - or do we want to do this so close() can cancel all contexts?
	Sync(ctx context.Context, id string, s Syncable) error
}
