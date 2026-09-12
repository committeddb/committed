package db

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/backup"
)

// blockingSource is a LiveSource whose capture waits until released, so a
// second backup can be asked of the node mid-stream.
type blockingSource struct {
	Storage
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingSource) CaptureBackup(visit func(string, int64, func(io.Writer) error) error) (backup.LiveInfo, error) {
	b.once.Do(func() { close(b.started) })
	<-b.release
	return backup.LiveInfo{}, nil
}

// One live backup streams at a time per node: a second is refused as busy
// while the first runs and accepted once it is done; a storage without
// on-disk stores is refused as unsupported.
func TestLiveBackup_OneAtATime(t *testing.T) {
	src := &blockingSource{started: make(chan struct{}), release: make(chan struct{})}
	d := &DB{storage: src, raft: &Raft{id: 1}}

	done := make(chan error, 1)
	go func() {
		_, err := d.LiveBackup(io.Discard, time.Now())
		done <- err
	}()
	<-src.started
	_, err := d.LiveBackup(io.Discard, time.Now())
	require.ErrorIs(t, err, ErrLiveBackupBusy)
	close(src.release)
	// The first ends in error (an empty capture is a hollow archive), but it
	// ends — and the flag clears.
	require.Error(t, <-done)
	_, err = d.LiveBackup(io.Discard, time.Now())
	require.NotErrorIs(t, err, ErrLiveBackupBusy)

	noDisk := &DB{storage: struct{ Storage }{}, raft: &Raft{id: 1}}
	_, err = noDisk.LiveBackup(io.Discard, time.Now())
	require.ErrorIs(t, err, ErrLiveBackupUnsupported)
}

// A node catching up from a peer refuses a live backup before reading
// anything: its event log runs past its raft log until the snapshot
// installs, and the backup's freeze would hold the catch-up's adoption.
func TestLiveBackup_RefusedWhileCatchingUp(t *testing.T) {
	src := &blockingSource{started: make(chan struct{}), release: make(chan struct{})}
	d := &DB{storage: src, raft: &Raft{id: 1}}
	d.raft.catchUp.begin(0, 100)
	_, err := d.LiveBackup(io.Discard, time.Now())
	require.ErrorIs(t, err, ErrLiveBackupCatchingUp)
	d.raft.catchUp.end()
	close(src.release)
	_, err = d.LiveBackup(io.Discard, time.Now())
	require.NotErrorIs(t, err, ErrLiveBackupCatchingUp)
}
