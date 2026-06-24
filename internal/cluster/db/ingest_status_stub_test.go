package db_test

import (
	"context"

	"github.com/committeddb/committed/internal/cluster"
)

// Status satisfies the cluster.Ingestable interface for the ingestable stubs in
// this package. None of them model an external source, snapshot, or CDC cursor,
// so they report an empty status — the IngestableStatus behavior proper is
// covered by the SQL dialect tests, not these worker-lifecycle stubs.

func (*MemoryIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

func (*SignalingIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

func (*freezeRecordingIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

func (*positionOnlyIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

func (*positionProbeIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

func (*seqIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}
