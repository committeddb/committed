package topic

import (
	"context"

	"github.com/philborlin/committed/internal/node/types"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

// Topic represents a topic in the system
//
//counterfeiter:generate . Topic
type Topic interface {
	Append(data Data) error
	Close() error
	Name() string
	NewReader(index uint64) (Reader, error)
}

// Data represets the data that will be appended to a topic
type Data struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// Reader has methods that will read data from the wal
//
//counterfeiter:generate . Reader
type Reader interface {
	Next(ctx context.Context) (*types.AcceptedProposal, error)
}
