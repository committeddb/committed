package wal

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db"
)

// receiverSink feeds a receiving storage exactly as db/catchup's sink does:
// it pins the generation, stages segment files under the receiver's fetch
// dir and adopts them, and appends record runs.
type receiverSink struct {
	recv *Storage
	gens []uint64
}

func (r *receiverSink) Begin(gen, _ uint64) error {
	r.gens = append(r.gens, gen)
	if r.recv.EventIndex() == 0 {
		return r.recv.SetEventLogGeneration(gen)
	}
	if mine := r.recv.EventLogGeneration(); mine != gen {
		return &db.EventGenerationMismatch{Have: gen, Want: mine}
	}
	return nil
}

func (r *receiverSink) Segment(name string, size int64, rd io.Reader) error {
	dir := r.recv.EventFetchDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(dir, name)
	data, err := io.ReadAll(io.LimitReader(rd, size))
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return err
	}
	return r.recv.AdoptEventSegments([]string{path})
}

func (r *receiverSink) Records(data []byte) error { return r.recv.AppendFetchedRecords(data) }

func (r *receiverSink) End(db.EventServeResult) error { return nil }

// catchUp drives ServeEvents from peer into recv until recv holds every
// event through `to`, returning how many exchanges it took.
func catchUp(t *testing.T, peer, recv *Storage, to uint64) (calls int) {
	t.Helper()
	sink := &receiverSink{recv: recv}
	for {
		calls++
		require.Less(t, calls, 1000, "catch-up is not converging")
		res, err := peer.ServeEvents(context.Background(), recv.EventIndex(), to, sink)
		require.NoError(t, err)
		if recv.EventIndex() >= min(to, res.EventIndex) {
			require.False(t, res.More, "a complete fetch ends with nothing more")
			return calls
		}
		require.True(t, res.More || res.LastIndex > 0, "no progress and nothing more: %+v", res)
	}
}

// requireSameEventBytes asserts the two logs hold byte-identical records at
// every sequence — the bytes the receiver wrote are the peer's bytes.
func requireSameEventBytes(t *testing.T, a, b *Storage) {
	t.Helper()
	la, err := a.LastEventSeq()
	require.NoError(t, err)
	lb, err := b.LastEventSeq()
	require.NoError(t, err)
	require.Equal(t, la, lb, "the logs must be the same length")
	for seq := uint64(1); seq <= la; seq++ {
		ra, err := a.ReadEventRaw(seq)
		require.NoError(t, err)
		rb, err := b.ReadEventRaw(seq)
		require.NoError(t, err)
		require.Equal(t, ra, rb, "record %d differs", seq)
	}
}

// A serving node fills an empty receiver and a stale one, in bounded
// exchanges, with byte-identical records: whole sealed segments as files
// (some compressed), the edges as records.
func TestServeEvents_FillsAnEmptyAndAStaleReceiverByteForByte(t *testing.T) {
	peer := openFetchPeer(t, t.TempDir(), time.Hour)
	const n = 300
	seedEventLog(t, peer, 1, n)
	compressOldestSealed(t, peer)
	need := peer.EventIndex()

	empty := openFetchPeer(t, t.TempDir(), time.Hour)
	calls := catchUp(t, peer, empty, need)
	require.Greater(t, calls, 1, "a log this size takes more than one bounded exchange")
	verifyEvents(t, empty, n)
	requireSameEventBytes(t, peer, empty)
	require.Equal(t, peer.EventLogGeneration(), empty.EventLogGeneration())
	require.NoDirExists(t, filepath.Join(empty.EventFetchDir(), "x"), "staging is consumed")

	// A stale receiver wrote the first 100 itself (the same bytes: it applied
	// the same entries) and takes the rest, whole segments included.
	stale := openFetchPeer(t, t.TempDir(), time.Hour)
	seedEventLog(t, stale, 1, 100)
	catchUp(t, peer, stale, need)
	verifyEvents(t, stale, n)
	requireSameEventBytes(t, peer, stale)

	// A receiver whose own segments cycle elsewhere still aligns by sequence.
	odd, err := Open(t.TempDir(), nil, nil, nil, WithoutFsync(), WithEventSegmentSize(4096), WithSealerIdleInterval(time.Hour))
	require.NoError(t, err)
	t.Cleanup(func() { _ = odd.Close() })
	seedEventLog(t, odd, 1, 150)
	catchUp(t, peer, odd, need)
	requireSameEventBytes(t, peer, odd)
}

// ServeEvents honours its bounds: nothing below `after`, nothing past `to`
// or the serving log's end, LastIndex and More reported honestly, and a
// receiver at another generation refused before anything is sent.
func TestServeEvents_BoundsAndGeneration(t *testing.T) {
	peer := openFetchPeer(t, t.TempDir(), time.Hour)
	seedEventLog(t, peer, 1, 50)

	recv := openFetchPeer(t, t.TempDir(), time.Hour)
	seedEventLog(t, recv, 1, 10)
	sink := &receiverSink{recv: recv}
	res, err := peer.ServeEvents(context.Background(), 10, 20, sink)
	require.NoError(t, err)
	require.Equal(t, uint64(20), res.LastIndex)
	require.False(t, res.More)
	require.Equal(t, uint64(20), recv.EventIndex(), "exactly (10, 20] arrived")
	verifyEvents(t, recv, 20)

	res, err = peer.ServeEvents(context.Background(), 50, 60, sink)
	require.NoError(t, err)
	require.Zero(t, res.LastIndex, "nothing past the serving log's end")
	require.False(t, res.More)
	require.Equal(t, uint64(50), res.EventIndex)

	res, err = peer.ServeEvents(context.Background(), 60, 70, sink)
	require.NoError(t, err)
	require.Zero(t, res.LastIndex)

	require.NoError(t, recv.SetEventLogGeneration(3))
	_, err = peer.ServeEvents(context.Background(), 20, 50, sink)
	var mismatch *db.EventGenerationMismatch
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, uint64(0), mismatch.Have)
	require.Equal(t, uint64(3), mismatch.Want)
	require.Equal(t, uint64(20), recv.EventIndex(), "a refused stream sends nothing")
}

// A generation recorded for an empty log survives a reopen, so a fetch
// interrupted by a restart resumes at it instead of starting over; a reset
// empties the log for a whole refetch and is refused under a freeze.
func TestEventLogGeneration_PersistsAndResetStartsOver(t *testing.T) {
	dir := t.TempDir()
	s := openFetchPeer(t, dir, time.Hour)
	require.NoError(t, s.SetEventLogGeneration(7))
	require.Equal(t, uint64(7), s.EventLogGeneration())
	require.NoError(t, s.Close())

	s, err := Open(dir, nil, nil, nil, WithoutFsync(), WithSealerIdleInterval(time.Hour))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.Equal(t, uint64(7), s.EventLogGeneration(), "the generation is durable")
	_, completed := s.ScrubProgress()
	require.Equal(t, uint64(7), completed, "it is the completed scrub bound")

	seedEventLog(t, s, 1, 50)
	release := s.FreezeEventLayout()
	require.ErrorIs(t, s.ResetEventLog(), ErrLayoutFrozen)
	release()
	require.NoError(t, s.ResetEventLog())
	require.Zero(t, s.EventIndex())
	last, err := s.LastEventSeq()
	require.NoError(t, err)
	require.Zero(t, last)

	peer := openFetchPeer(t, t.TempDir(), time.Hour)
	seedEventLog(t, peer, 1, 50)
	catchUp(t, peer, s, 50)
	requireSameEventBytes(t, peer, s)
}
