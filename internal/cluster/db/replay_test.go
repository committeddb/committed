package db_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/philborlin/committed/internal/cluster/db/wal"
)

// replayBehavior is shared by every replaySyncable instance the parser builds
// (the live worker's and replay's own), so a test can flip a payload from
// failing to succeeding between the dead-letter and the replay.
type replayBehavior struct {
	mu       sync.Mutex
	permFail map[string]bool
}

func (b *replayBehavior) setFail(payload string, fail bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.permFail == nil {
		b.permFail = map[string]bool{}
	}
	b.permFail[payload] = fail
}

func (b *replayBehavior) fails(payload string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.permFail[payload]
}

type replaySyncable struct{ b *replayBehavior }

func (s *replaySyncable) Sync(_ context.Context, p *cluster.Actual) (cluster.ShouldSnapshot, error) {
	if allSystem(p) {
		return cluster.ShouldSnapshot(false), nil
	}
	payload := string(p.Entities[0].Data)
	if s.b.fails(payload) {
		return cluster.ShouldSnapshot(false), cluster.Permanent(fmt.Errorf("rejected %q", payload))
	}
	return cluster.ShouldSnapshot(true), nil
}

func (s *replaySyncable) Close() error { return nil }

type replayParser struct{ b *replayBehavior }

func (p *replayParser) Parse(_ *viper.Viper, _ cluster.DatabaseStorage) (cluster.Syncable, error) {
	return &replaySyncable{b: p.b}, nil
}

// newWalDBWithSync builds a wal-backed DB whose syncable-registration channel
// is wired, so ProposeSyncable actually starts a worker (newWalDB passes a nil
// channel, which persists config but starts none).
func newWalDBWithSync(t *testing.T) (*db.DB, *wal.Storage) {
	t.Helper()
	dir := t.TempDir()
	p := parser.New()
	sync := make(chan *db.SyncableWithID)
	s, err := wal.Open(dir, p, sync, nil, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, sync, nil, db.WithTickInterval(testTickInterval))
	t.Cleanup(func() { _ = d.Close() })
	return d, s
}

// registerAndDeadLetter brings up a config-registered syncable that permanently
// fails the "bad" payload, drives one such proposal through it so the worker
// dead-letters it, and returns the dead-lettered raft index.
func registerAndDeadLetter(t *testing.T, d *db.DB, s *wal.Storage, id string, b *replayBehavior) uint64 {
	t.Helper()
	b.setFail("bad", true)
	d.AddSyncableParser("test", &replayParser{b: b})
	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID:       id,
		MimeType: "text/toml",
		Data:     []byte("[syncable]\ntype = \"test\"\nname = \"" + id + "\""),
	}))
	seedUserProposals(t, d, s, "evt", []string{"bad"})

	var index uint64
	require.Eventually(t, func() bool {
		dls, err := d.SyncableDeadLetters(id, 0, 10)
		if err == nil && len(dls) == 1 {
			index = dls[0].Index
			return true
		}
		return false
	}, 10*time.Second, 10*time.Millisecond, "worker should dead-letter the failing proposal")
	return index
}

// TestReplaySyncableDeadLetter_Success re-drives a dead-lettered proposal once
// its downstream is fixed: the re-sync succeeds and the dead-letter record is
// cleared.
func TestReplaySyncableDeadLetter_Success(t *testing.T) {
	d, s := newWalDBWithSync(t)
	b := &replayBehavior{}
	id := "repl-ok"
	index := registerAndDeadLetter(t, d, s, id, b)

	// Downstream fixed: the payload now applies.
	b.setFail("bad", false)
	require.NoError(t, d.ReplaySyncableDeadLetter(testCtx(t), id, index))

	require.Eventually(t, func() bool {
		dls, _ := d.SyncableDeadLetters(id, 0, 10)
		return len(dls) == 0
	}, 5*time.Second, 5*time.Millisecond, "a successful replay must clear the dead-letter record")
}

// TestReplaySyncableDeadLetter_NotDeadLettered returns ErrNotDeadLettered for
// an index that isn't a dead letter.
func TestReplaySyncableDeadLetter_NotDeadLettered(t *testing.T) {
	d, s := newWalDBWithSync(t)
	b := &replayBehavior{}
	id := "repl-404"
	index := registerAndDeadLetter(t, d, s, id, b)

	err := d.ReplaySyncableDeadLetter(testCtx(t), id, index+9999)
	require.ErrorIs(t, err, cluster.ErrNotDeadLettered)
}

// TestReplaySyncableDeadLetter_StillFails leaves the record in place and
// returns ErrReplaySyncFailed when the re-sync fails again.
func TestReplaySyncableDeadLetter_StillFails(t *testing.T) {
	d, s := newWalDBWithSync(t)
	b := &replayBehavior{}
	id := "repl-fail"
	index := registerAndDeadLetter(t, d, s, id, b)

	// Downstream still broken: leave "bad" failing.
	err := d.ReplaySyncableDeadLetter(testCtx(t), id, index)
	require.ErrorIs(t, err, cluster.ErrReplaySyncFailed)

	dls, derr := d.SyncableDeadLetters(id, 0, 10)
	require.NoError(t, derr)
	require.Len(t, dls, 1, "a failed replay must leave the dead-letter record in place")
}
