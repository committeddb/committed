package db_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// parkedIngestStorage reports every ingestable as terminally parked, letting the
// test drive DB.IngestableStatus's replicated parked-check without a real supervisor
// give-up.
type parkedIngestStorage struct {
	*MemoryStorage
}

func (parkedIngestStorage) IngestableStuck(string) (cluster.IngestableStuck, bool, error) {
	return cluster.IngestableStuck{ID: "ing-1", SinceUnixNano: 1, Message: "supervisor gave up"}, true, nil
}

// TestIngestableStatus_ParkedIsNodeAgnostic proves the terminal parked state is
// reported from ANY node — even one with NO local worker handle (a follower, or the
// owner after the supervisor stopped the worker) — because DB.IngestableStatus reads
// it from the replicated IngestableStuck record before the local-handle lookup.
// Without this a dead ingest worker is a 404 on a follower and a phase:"streaming"
// lie on the owner.
func TestIngestableStatus_ParkedIsNodeAgnostic(t *testing.T) {
	d := createDBWithStorage(parkedIngestStorage{NewMemoryStorage()})
	defer d.Close()

	st, err := d.IngestableStatus(context.Background(), "ing-1")
	require.NoError(t, err, "a parked ingestable must not 404 even with no local worker")
	require.Equal(t, cluster.WorkerStateParked, st.WorkerState)
}
