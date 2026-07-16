package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// failingConnector is a database/sql connector whose every Connect fails with a
// fixed error, so BeginTx surfaces that error the way a pgx ConnectError does —
// letting a test drive the Sync/SyncBatch begin-failure path without a real DB.
type failingConnector struct{ err error }

func (c failingConnector) Connect(context.Context) (driver.Conn, error) { return nil, c.err }
func (c failingConnector) Driver() driver.Driver                        { return failingDriver{} }

type failingDriver struct{}

func (failingDriver) Open(string) (driver.Conn, error) { return nil, errors.New("unused") }

// connectPII mimics the pgx connect error that embeds connection identity — the
// user, database, and host:port that must never reach the replicated stuck status
// or the permanent dead-letter.
const connectPII = `failed to connect to ` +
	"`user=admin database=orders host=10.0.0.5:5432`" +
	`: dial tcp 10.0.0.5:5432: connect: connection refused`

func failingDB(t *testing.T) *sql.DB {
	t.Helper()
	db := sql.OpenDB(failingConnector{err: errors.New(connectPII)})
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// requireBeginErrorRedacted asserts a Sync/SyncBatch begin failure is a
// cluster.RedactedError whose replicated message drops the connection identity,
// while the node-local Error() keeps it for debugging.
func requireBeginErrorRedacted(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)

	var red cluster.RedactedError
	require.True(t, errors.As(err, &red), "a begin failure must be redactable")

	msg := red.RedactedMessage()
	require.NotContains(t, msg, "admin", "no user= in the replicated message")
	require.NotContains(t, msg, "orders", "no database= in the replicated message")
	require.NotContains(t, msg, "10.0.0.5", "no host:port in the replicated message")
	require.Contains(t, msg, "begin", "the classifier still names the failed step")

	// The full connect detail stays available node-locally.
	require.Contains(t, err.Error(), "10.0.0.5")
}

func actualFor(topic string) *cluster.Actual {
	return &cluster.Actual{
		Index:    1,
		Entities: []*cluster.Entity{{Type: &cluster.Type{ID: topic}}},
	}
}

// TestSyncable_Sync_BeginError_Redacted: a failed BeginTx in Syncable.Sync no
// longer returns the raw pgx connect error (which embeds user=/database=/
// host:port) into the replicated stuck status and permanent dead-letter.
func TestSyncable_Sync_BeginError_Redacted(t *testing.T) {
	c := &Syncable{db: failingDB(t), config: &Config{Topic: "t"}}
	_, err := c.Sync(context.Background(), actualFor("t"))
	requireBeginErrorRedacted(t, err)
}

// TestSyncable_SyncBatch_BeginError_Redacted covers the batch begin site.
func TestSyncable_SyncBatch_BeginError_Redacted(t *testing.T) {
	c := &Syncable{db: failingDB(t), config: &Config{Topic: "t"}}
	_, err := c.SyncBatch(context.Background(), []*cluster.Actual{actualFor("t")})
	requireBeginErrorRedacted(t, err)
}

// TestProjection_Sync_BeginError_Redacted covers the projection begin site.
func TestProjection_Sync_BeginError_Redacted(t *testing.T) {
	p := &Projection{db: failingDB(t), sources: map[string][]*projectionSource{"t": nil}}
	_, err := p.Sync(context.Background(), actualFor("t"))
	requireBeginErrorRedacted(t, err)
}

// TestProjection_SyncBatch_BeginError_Redacted covers the projection batch begin
// site.
func TestProjection_SyncBatch_BeginError_Redacted(t *testing.T) {
	p := &Projection{db: failingDB(t), sources: map[string][]*projectionSource{"t": nil}}
	_, err := p.SyncBatch(context.Background(), []*cluster.Actual{actualFor("t")})
	requireBeginErrorRedacted(t, err)
}
