package iceberg

import (
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// arrowSchema mirrors envelopeSchema in Arrow terms. Field order and names
// must match the Iceberg schema — iceberg-go maps append data to table
// columns by name.
func arrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "key", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "payload", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "committed_index", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "generation", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
}

// liveRecord builds one Arrow record holding the buffer's live rows (upserts;
// tombstones contribute only to the delete filter). Rows are emitted in key
// order so the written data files carry tight key stats — that ordering is
// what lets later flushes' delete filters prune untouched files. Returns the
// live-row count; the caller releases the record.
func (s *Syncable) liveRecord() (arrow.RecordBatch, int, error) {
	keys := make([]string, 0, len(s.buffer))
	for k, row := range s.buffer {
		if !row.delete {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	b := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema())
	defer b.Release()
	keyB := b.Field(0).(*array.StringBuilder)
	payloadB := b.Field(1).(*array.StringBuilder)
	indexB := b.Field(2).(*array.Int64Builder)
	genB := b.Field(3).(*array.Int64Builder)
	for _, k := range keys {
		row := s.buffer[k]
		keyB.Append(k)
		payloadB.Append(row.payload)
		indexB.Append(int64(row.index))    //nolint:gosec // G115: a raft index is far below 2^63
		genB.Append(int64(row.generation)) //nolint:gosec // G115: a refresh epoch is a small counter
	}
	return b.NewRecordBatch(), len(keys), nil
}

// recordReader wraps a single record as the stream Append consumes.
func recordReader(rec arrow.RecordBatch) (array.RecordReader, error) {
	rdr, err := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
	if err != nil {
		return nil, fmt.Errorf("[iceberg] record reader: %w", err)
	}
	return rdr, nil
}
