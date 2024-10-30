package sql

import (
	"io"

	"github.com/philborlin/committed/internal/cluster"
)

//go:generate protoc --go_out=paths=source_relative:. ./dialectpb/dialect.proto

type Dialect interface {
	Open(config *Config, pos cluster.Position) (<-chan *cluster.Proposal, <-chan cluster.Position, io.Closer, error)
}

type Config struct {
	ConnectionString string
	Type             *cluster.Type
	Mappings         []Mapping
	PrimaryKey       string
}

type Mapping struct {
	JSONName  string
	SQLColumn string
}
