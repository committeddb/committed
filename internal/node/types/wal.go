package types

import (
	"io"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/wal"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

// WALFactory is an interface wrap of the tsdb wal so it is testable
//counterfeiter:generate . WALFactory
type WALFactory interface {
	New(logger log.Logger, reg prometheus.Registerer, dir string, compress bool) (WAL, error)
	NewSize(logger log.Logger, reg prometheus.Registerer, dir string, segmentSize int, compress bool) (WAL, error)
	Open(logger log.Logger, reg prometheus.Registerer, dir string) (WAL, error)
	NewSegmentsRangeReader(sr ...wal.SegmentRange) (io.ReadCloser, error)
	NewLiveReader(logger log.Logger, reg prometheus.Registerer, r io.Reader) LiveReader
}

// WAL is an interface wrap of the tsdb wal so it is testable
//counterfeiter:generate . WAL
type WAL interface {
	NextSegment() error
	Log(recs ...[]byte) error
	Segments() (first, last int, err error)
	Close() (err error)
}

// LiveReader is an interface wrap of the tsdb LiveReader so that it is testable
//counterfeiter:generate . LiveReader
type LiveReader interface {
	Err() error
	Next() bool
	Record() []byte
}

// TSDBWALFactory implements WALFactory
type TSDBWALFactory struct {
}

// New implements WALFactory
func (f *TSDBWALFactory) New(logger log.Logger, reg prometheus.Registerer, dir string, compress bool) (WAL, error) {
	return wal.New(logger, reg, dir, compress)
}

// NewSize implements WALFactory
func (f *TSDBWALFactory) NewSize(logger log.Logger, reg prometheus.Registerer, dir string, segmentSize int, compress bool) (WAL, error) {
	return wal.NewSize(logger, reg, dir, segmentSize, compress)
}

// Open implements WALFactory
func (f *TSDBWALFactory) Open(logger log.Logger, reg prometheus.Registerer, dir string) (WAL, error) {
	return wal.Open(logger, reg, dir)
}

// NewSegmentsRangeReader implements WALFactory
func (f *TSDBWALFactory) NewSegmentsRangeReader(sr ...wal.SegmentRange) (io.ReadCloser, error) {
	return wal.NewSegmentsRangeReader(sr...)
}

// NewLiveReader implements WALFactory
func (f *TSDBWALFactory) NewLiveReader(logger log.Logger, reg prometheus.Registerer, r io.Reader) LiveReader {
	return wal.NewLiveReader(logger, wal.NewLiveReaderMetrics(reg), r)
}
