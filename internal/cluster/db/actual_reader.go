package db

import "github.com/philborlin/committed/internal/cluster"

// ActualReader streams committed Actuals out of the permanent event log in
// raft-index order. Storage.Reader(id) constructs one; callers read until
// io.EOF. Each Actual carries its own Index.
//
//counterfeiter:generate . ActualReader
type ActualReader interface {
	Read() (*cluster.Actual, error)
}
