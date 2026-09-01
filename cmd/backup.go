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

	// Hold the shared stopped-node lock for the whole archive: a running node
	// is refused, and a node starting mid-copy fails its own bbolt open (its
	// FIRST act — wal.Open locks before any recovery mutation) instead of
	// writing into the data dir under the walk. See datadir.LockStoppedNode.
	lockDB, err := datadir.LockStoppedNode(dataDir)
	if err != nil {
		if errors.Is(err, datadir.ErrNodeRunning) {
			return fmt.Errorf("%w; see docs/operations/backup.md", err)
		}
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

func init() {
	backupCmd.Flags().StringVar(&backupDataDir, "data", "", "node data directory to archive (default $COMMITTED_DATA_DIR or ./data)")
	backupCmd.Flags().StringVar(&backupTo, "to", "", "destination backup path (.tar or .tar.gz); required")
	backupCmd.Flags().Uint64Var(&backupNodeID, "node-id", 0, "node id to record in the manifest for provenance (default $COMMITTED_NODE_ID)")
	rootCmd.AddCommand(backupCmd)
}
