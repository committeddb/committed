package db

import "github.com/philborlin/committed/internal/cluster"

// []byte - index of the position
//
//counterfeiter:generate . IngestablePosition
type IngestablePosition interface {
	Read() (cluster.Position, error)
}
