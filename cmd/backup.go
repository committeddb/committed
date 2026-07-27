package cmd

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"

	"github.com/spf13/cobra"

	"github.com/committeddb/committed/internal/cluster/backup"
	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/internal/cluster/fsutil"
)

var (
	backupDataDir string
	backupTo      string
	backupNodeID  uint64
)

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Archive a STOPPED node's data directory to a portable backup tar",
	Long: `Archive a node's on-disk state (raft logs, the permanent event log, and
the BoltDB metadata) into a single tar, with a manifest, for off-box archival
and disaster recovery. Restore it with "committed restore".

OFFLINE by design: the node whose --data directory you archive MUST be
stopped. BoltDB holds an exclusive lock while a node runs, so a backup taken
from a live directory would be inconsistent — this command takes a SHARED lock
on it and holds it for the whole archive, refusing a node that is up and
blocking one from starting mid-backup. To back up a live cluster, stop one
follower (quorum holds on the rest), back it up, and start it again, the same
rolling discipline as a rolling upgrade. See docs/operations/backup.md.

If --to ends in ".gz" the archive is gzip-compressed. The archive is written
atomically: a failed backup leaves no file at the destination.

  COMMITTED_DATA_DIR   default for --data (falls back to ./data)
  COMMITTED_NODE_ID    recorded in the manifest for provenance when --node-id
                       is not given`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runBackup()
	},
}

func runBackup() error {
	dataDir := backupDataDir
	if dataDir == "" {
		dataDir = getenvDefault("COMMITTED_DATA_DIR", "./data")
	}
	if backupTo == "" {
		return fmt.Errorf("--to is required (the destination .tar or .tar.gz path)")
	}
	nodeID := backupNodeID
	if nodeID == 0 {
		// Provenance only; best-effort from the env, 0 if unset/invalid.
		if v, err := strconv.ParseUint(os.Getenv("COMMITTED_NODE_ID"), 10, 64); err == nil {
			nodeID = v
		}
	}

	// Hold a shared lock on the node's BoltDB for the whole archive: a running
	// node holds it exclusively (so this refuses one that is up), and keeping the
	// shared lock across the walk blocks the node from starting and writing into
	// the data dir mid-copy.
	lockDB, err := lockStoppedNode(dataDir)
	if err != nil {
		return err
	}
	if lockDB != nil {
		defer func() { _ = lockDB.Close() }()
	}

	// Write to a temp file alongside the destination, then rename on success,
	// so a partial/failed backup never appears at --to.
	//
	// A backup archive holds the node's entire state — the event log and BoltDB
	// metadata, including any PII — so create it owner-only (0600), not
	// os.Create's world-readable 0666&~umask. The mode survives the rename to
	// --to. Chmod after open forces 0600 even when O_CREATE reuses a stale
	// .partial from a hard-killed prior run (which O_CREATE would otherwise leave
	// at its old, possibly looser, perms).
	tmp := backupTo + ".partial"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec // G304: the destination is operator-supplied via --to
	if err != nil {
		return fmt.Errorf("create %q: %w", tmp, err)
	}
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("secure %q: %w", tmp, err)
	}
	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}

	var w io.Writer = f
	var gz *gzip.Writer
	if strings.HasSuffix(backupTo, ".gz") {
		gz = gzip.NewWriter(f)
		w = gz
	}

	m, err := backup.Create(w, dataDir, nodeID, time.Now())
	if err != nil {
		cleanup()
		return err
	}
	if gz != nil {
		if err := gz.Close(); err != nil {
			cleanup()
			return fmt.Errorf("finalize gzip: %w", err)
		}
	}
	// fsync the archive content before Close+rename so a crash after "backed up"
	// can't leave a torn or zero-length backup that only surfaces on restore.
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("fsync %q: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("finalize %q: %w", tmp, err)
	}
	if err := os.Rename(tmp, backupTo); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("publish backup to %q: %w", backupTo, err)
	}
	// Persist the rename in the destination's parent directory.
	if err := fsutil.SyncDir(filepath.Dir(backupTo)); err != nil {
		return fmt.Errorf("fsync backup dir after publish: %w", err)
	}

	_, _ = fmt.Fprintf(os.Stdout, "backed up %d files from %s to %s\n", len(m.Files), dataDir, backupTo)
	return nil
}

// lockStoppedNode opens the node's BoltDB with a SHARED lock and returns the open
// handle. The caller must HOLD it — Close it only AFTER the archive is fully
// written. A running node holds BoltDB's EXCLUSIVE lock, so acquiring the shared
// lock both proves the node is stopped (contention -> ErrTimeout -> refuse) and,
// held for the whole walk, keeps it stopped: the node's own exclusive Open (its
// 1s lock timeout) then fails, so it cannot start and mutate the data dir
// mid-backup — the race the old "probe then release immediately" left open. A
// missing BoltDB file returns a nil handle (a fresh node with no committed data
// yet, where Create surfaces the clearer "empty data dir" error).
//
// Note: the lock guards only BoltDB. A node that races to start during the walk
// fails at its bbolt Open, but not before running the earlier WAL-recovery steps
// of Open — a small residual window a filesystem-level snapshot would close (see
// docs/operations/backup.md). This holds against the dominant race.
func lockStoppedNode(dataDir string) (*bolt.DB, error) {
	boltPath := datadir.BoltPath(datadir.MetadataDir(dataDir))
	if _, err := os.Stat(boltPath); err != nil {
		return nil, nil //nolint:nilnil // absent bbolt: no lock to take; Create surfaces the empty-dir error.
	}
	db, err := bolt.Open(boltPath, 0o600, &bolt.Options{ReadOnly: true, Timeout: 2 * time.Second})
	if errors.Is(err, bolterrors.ErrTimeout) {
		// Lock contention specifically means another process (the node) holds
		// the DB open.
		return nil, fmt.Errorf("data directory %q appears to be in use by a running node "+
			"(could not lock %s). Stop the node — or one follower of a live cluster — "+
			"before backing up; see docs/operations/backup.md", dataDir, boltPath)
	}
	if err != nil {
		return nil, fmt.Errorf("open metadata db %s: %w", boltPath, err)
	}
	return db, nil
}

func init() {
	backupCmd.Flags().StringVar(&backupDataDir, "data", "", "node data directory to archive (default $COMMITTED_DATA_DIR or ./data)")
	backupCmd.Flags().StringVar(&backupTo, "to", "", "destination backup path (.tar or .tar.gz); required")
	backupCmd.Flags().Uint64Var(&backupNodeID, "node-id", 0, "node id to record in the manifest for provenance (default $COMMITTED_NODE_ID)")
	rootCmd.AddCommand(backupCmd)
}
