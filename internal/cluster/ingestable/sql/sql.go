package sql

import (
	"io"

	"github.com/philborlin/committed/internal/cluster"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

type Ingestable struct {
	config  *Config
	dialect Dialect
	closer  io.Closer
}

func New(d Dialect, config *Config) *Ingestable {
	return &Ingestable{config: config, dialect: d}
}

func (i *Ingestable) Ingest(pos cluster.Position) (<-chan *cluster.Proposal, <-chan cluster.Position, error) {
	proposalChan, positionChan, closer, err := i.dialect.Open(i.config, pos)

	i.closer = closer

	return proposalChan, positionChan, err
}

func (i *Ingestable) Close() error {
	return i.closer.Close()
}
