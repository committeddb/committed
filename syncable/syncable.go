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
