package sql

import (
	"context"

	"github.com/philborlin/committed/internal/cluster"
)

//go:generate protoc --go_out=paths=source_relative:. ./dialectpb/dialect.proto

type Dialect interface {
	Ingest(ctx context.Context, config *Config, pos cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error
}

type Config struct {
	ConnectionString string
	Type             *cluster.Type
	Mappings         []Mapping
	PrimaryKey       string
}

type Mapping struct {
	JsonName  string
	SQLColumn string
}
