package db_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db"
)

// peerURLFailStorage is a MemoryStorage whose PutMemberPeerURL fails, to
// exercise the durability-watermark guard in applyConfChange.
type peerURLFailStorage struct {
	*MemoryStorage
}

func (peerURLFailStorage) PutMemberPeerURL(uint64, []byte) error {
	return errors.New("disk boom")
}

// TestPersistPeerURL_FatalOnWriteError pins Tier-1 #2: a failed durable
// peer-URL write during applyConfChange must FATAL, not be logged-and-continued.
// The write runs before ApplyCommittedBatch advances the applied index, and once
// the watermark passes the conf change c.Applied suppresses its re-delivery — so
// swallowing the error would strand the peer URL permanently absent (the peer's
// raft transport can never be reconciled), the confstate-lost-in-crash-window
// class. Crashing keeps the watermark from advancing past the lost write so a
// restart re-delivers the conf change and re-runs it.
func TestPersistPeerURL_FatalOnWriteError(t *testing.T) {
	n := db.NewRaftWithPanicFatalForTest(peerURLFailStorage{NewMemoryStorage()})
	require.Panics(t, func() { n.PersistPeerURLOrFatalForTest(2, []byte("http://n2:2380")) },
		"a failed peer-URL write must fatal (not be swallowed), so the applied index never advances past a lost peer URL")
}

// TestPersistPeerURL_SucceedsQuietly is the companion: a successful write must
// NOT fatal.
func TestPersistPeerURL_SucceedsQuietly(t *testing.T) {
	n := db.NewRaftWithPanicFatalForTest(NewMemoryStorage())
	require.NotPanics(t, func() { n.PersistPeerURLOrFatalForTest(2, []byte("http://n2:2380")) },
		"a successful peer-URL write must not fatal")
}
