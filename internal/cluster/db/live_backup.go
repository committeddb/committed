package db

import (
	"errors"
	"io"
	"time"

	"github.com/committeddb/committed/internal/cluster/backup"
)

// ErrLiveBackupUnsupported is a storage without on-disk stores to archive
// (the in-memory test doubles).
var ErrLiveBackupUnsupported = errors.New("this node's storage cannot be backed up live")

// ErrLiveBackupBusy is a second live backup asked of a node while one is
// streaming: one at a time, so the node's disk and the freezes a backup
// takes are not held twice over.
var ErrLiveBackupBusy = errors.New("a live backup of this node is already in progress")

// ErrLiveBackupCatchingUp is a live backup asked of a node that is filling
// its event log from a peer: its events run past its raft log until the
// snapshot installs, which is not a state a node can boot from (the
// capture refuses it too, as the backstop) — and the backup's freeze would
// hold the catch-up's adoption meanwhile.
var ErrLiveBackupCatchingUp = errors.New("this node is catching up from a peer; take the backup once it has caught up")

// LiveBackup streams a backup archive of this running node's state to w —
// the same archive `committed backup` takes from a stopped node, restored
// the same way (see backup.CreateLive and wal.Storage.CaptureBackup for what
// keeps it consistent). Returns the manifest it wrote. The stream is one
// bounded reader of the node's log layouts; a receiver that stalls is the
// caller's to cut.
func (db *DB) LiveBackup(w io.Writer, now time.Time) (*backup.Manifest, error) {
	src, ok := db.storage.(backup.LiveSource)
	if !ok {
		return nil, ErrLiveBackupUnsupported
	}
	if db.CatchingUp() {
		return nil, ErrLiveBackupCatchingUp
	}
	if !db.liveBackup.CompareAndSwap(false, true) {
		return nil, ErrLiveBackupBusy
	}
	defer db.liveBackup.Store(false)
	return backup.CreateLive(w, src, db.ID(), now)
}
