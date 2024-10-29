package sql

import (
	"io"

	"github.com/philborlin/committed/internal/cluster"
)

//go:generate protoc --go_out=paths=source_relative:. ./dialectpb/dialect.proto

type Position []byte

type Dialect interface {
	Open(connectionString string, pos Position) (<-chan *cluster.Proposal, <-chan Position, io.Closer, error)
}

type Config struct {
	Database   cluster.Database
	Type       *cluster.Type
	Mappings   []Mapping
	PrimaryKey string
}

type Mapping struct {
	JSONName  string
	SQLColumn string
}
