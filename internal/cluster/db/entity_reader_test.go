package db_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// readerSyncable is a syncable that asks for the engine's entity reader.
type readerSyncable struct {
	reader chan cluster.EntityReader
}

func (r *readerSyncable) Sync(context.Context, *cluster.Actual) (cluster.ShouldSnapshot, error) {
	return false, nil
}
func (r *readerSyncable) Close() error                            { return nil }
func (r *readerSyncable) SetEntityReader(er cluster.EntityReader) { r.reader <- er }

func entityReaderFor(t *testing.T, sync func(id string, s cluster.Syncable) error) cluster.EntityReader {
	t.Helper()
	rs := &readerSyncable{reader: make(chan cluster.EntityReader, 1)}
	require.NoError(t, sync("reader", rs))
	select {
	case er := <-rs.reader:
		return er
	case <-time.After(5 * time.Second):
		t.Fatal("the engine never handed the syncable a reader")
		return nil
	}
}

// indexOf is the raft index of the first upsert of key in the event log.
func indexOf(t *testing.T, s *wal.Storage, key string) uint64 {
	t.Helper()
	r := s.Reader("index-of")
	for {
		a, err := r.Read()
		if errors.Is(err, io.EOF) {
			t.Fatalf("no upsert of %q in the log", key)
		}
		require.NoError(t, err)
		for _, e := range a.Entities {
			if string(e.Key) == key && !e.IsDelete() {
				return a.Index
			}
		}
	}
}

// The reader finds an entity by topic and key, the last of an entry's
// entities for it; an entity a scrub erased reads as absent, and its
// neighbour in the same entry still reads; a missing entity above the
// scrubbed bound is an error.
func TestEntityReaderAcrossAScrub(t *testing.T) {
	d, s := newWalDBScrub(t, 0)
	require.NoError(t, d.ProposeType(testCtx(t), createType("events").config))
	ty, err := d.ResolveType(cluster.LatestTypeRef("events"))
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(ty, []byte("alice"), []byte(`{"v":1}`)),
		cluster.NewUpsertEntity(ty, []byte("bob"), []byte(`{"v":2}`)),
		cluster.NewUpsertEntity(ty, []byte("alice"), []byte(`{"v":3}`)),
	}}))
	proposeUserDelete(t, d, "events", "alice")
	er := entityReaderFor(t, func(id string, sy cluster.Syncable) error { return d.Sync(testCtx(t), id, sy) })
	at := indexOf(t, s, "alice")

	data, found, err := er.ReadEntity(at, "events", []byte("alice"), 0, 0)
	require.NoError(t, err)
	require.True(t, found)
	require.JSONEq(t, `{"v":3}`, string(data), "the entry's last entity for the key")

	require.NoError(t, d.Scrub(testCtx(t)))
	require.Eventually(t, func() bool {
		up, _ := keyState(t, s, "alice")
		return !up
	}, 5*time.Second, 10*time.Millisecond)

	_, found, err = er.ReadEntity(at, "events", []byte("alice"), 0, 0)
	require.NoError(t, err)
	require.False(t, found, "an entity a scrub erased reads as absent")
	data, found, err = er.ReadEntity(at, "events", []byte("bob"), 0, 0)
	require.NoError(t, err)
	require.True(t, found, "the entry's other entity survives the scrub")
	require.JSONEq(t, `{"v":2}`, string(data))

	proposeUserUpsert(t, d, "events", "carol", `{"v":4}`)
	_, found, err = er.ReadEntity(indexOf(t, s, "carol"), "events", []byte("nobody"), 0, 0)
	require.Error(t, err, "nothing could have removed an entity above the scrubbed bound")
	require.False(t, found)
	require.NotContains(t, err.Error(), "nobody", "the key stays out of the error text")
}

// The reader migrates as the fold did — to the version the pointer
// recorded, not the latest one.
func TestEntityReaderMigratesToTheRecordedVersion(t *testing.T) {
	d, s := newWalDB(t)
	proposeTypeTOML(t, d, "person", "Person", `{"type":"object"}`, "")
	v1, err := s.ResolveType(cluster.LatestTypeRef("person"))
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{
		{Type: v1, Key: []byte("alice"), Data: []byte(`{"name":"alice"}`)},
	}}))
	proposeTypeWithMigration(t, d, "person", "Person", `{"type":"object","required":["v2"]}`, `. + {v2: true}`)
	proposeTypeWithMigration(t, d, "person", "Person", `{"type":"object","required":["v2","v3"]}`, `. + {v3: true}`)
	er := entityReaderFor(t, func(id string, sy cluster.Syncable) error { return d.Sync(testCtx(t), id, sy) })
	at := indexOf(t, s, "alice")

	for _, c := range []struct {
		from, to int
		want     string
	}{
		{0, 0, `{"name":"alice"}`},
		{1, 2, `{"name":"alice","v2":true}`},
		{1, 3, `{"name":"alice","v2":true,"v3":true}`},
	} {
		data, found, err := er.ReadEntity(at, "person", []byte("alice"), c.from, c.to)
		require.NoError(t, err)
		require.True(t, found)
		var got, want map[string]any
		require.NoError(t, json.Unmarshal(data, &got))
		require.NoError(t, json.Unmarshal([]byte(c.want), &want))
		require.Equal(t, want, got, "from %d to %d", c.from, c.to)
	}
}
