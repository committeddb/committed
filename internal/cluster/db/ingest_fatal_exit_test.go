package db_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// fatalIngestable's Ingest returns a fatal error as soon as it runs — modeling a
// self-contained fault the lenient admission parse let through (e.g. a portless
// MySQL URL that binlogSyncerConfig rejects) or an unrecoverable stream fault. It
// never reaches ctx cancellation and never checkpoints.
type fatalIngestable struct{}

func (fatalIngestable) Ingest(context.Context, cluster.Position, chan<- *cluster.Proposal, chan<- cluster.Position) error {
	return errors.New("fatal ingest error: invalid source configuration")
}

func (fatalIngestable) Close() error { return nil }

func (fatalIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

// TestIngest_InnerFatalExitFreezesNotSilentStall: when a worker's inner Ingest
// returns a fatal error (not a ctx-driven teardown), db.ingest must FREEZE it —
// surfacing it via the supervisor as recovering→parked — instead of swallowing
// the error to a warn and idling forever while status still reports a live
// worker. That silent stall is a dead CDC worker whose downstream mirror
// diverges with no operator signal.
func TestIngest_InnerFatalExitFreezesNotSilentStall(t *testing.T) {
	d, _ := newWalDBOpts(t)
	const id = "fatal-exit"
	seedIngestableConfig(t, d, id)

	require.NoError(t, d.Ingest(context.Background(), id, fatalIngestable{}))

	require.Eventually(t, func() bool {
		st, err := d.IngestableStatus(context.Background(), id)
		return err == nil && st.WorkerState != cluster.WorkerStateRunning
	}, 5*time.Second, 20*time.Millisecond,
		"a worker whose Ingest returns a fatal error must become observably frozen "+
			"(recovering/parked), not silently idle while status reports running")
}
