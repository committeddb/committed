package cmd

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
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
	backupDataDir  string
	backupTo       string
	backupNodeID   uint64
	backupLive     bool
	backupTarget   string
	backupToken    string
	backupInsecure bool
)

// nodeBackupPath is the node API route a live backup streams from.
const nodeBackupPath = "/v1/node/backup"

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Archive a node's state to a portable backup tar — a stopped data directory, or a running node with --live",
	Long: `Archive a node's on-disk state (raft logs, the permanent event log, and
the BoltDB metadata) into a single tar, with a manifest, for off-box archival
and disaster recovery. Restore it with "committed restore".

Two sources, one archive:

  committed backup --data <dir> --to <file>            a STOPPED node's directory
  committed backup --live --target <node> --to <file>  a RUNNING node, over its API

Offline: the node whose --data directory you archive MUST be stopped. BoltDB
holds an exclusive lock while a node runs, so this command takes a SHARED
lock on it and holds it for the whole archive, refusing a node that is up
and blocking one from starting mid-backup.

Live: the node streams the archive itself while it keeps serving — it reads
its stores in an order a restart tolerates and holds each log's maintenance
still only while that log streams. Target a node directly (not a load
balancer) and prefer a follower. The download is verified as it arrives:
every entry is hashed and checked against the trailing manifest, and a
stream the node cut short (a stall, or its own maintenance overtaking the
read, which it detects) leaves no file at --to — take it again.

If --to ends in ".gz" the archive is gzip-compressed. The archive is written
atomically: a failed backup leaves no file at the destination.

  COMMITTED_DATA_DIR   default for --data (falls back to ./data)
  COMMITTED_NODE_ID    recorded in the manifest for provenance when --node-id
                       is not given (offline; a live backup records the
                       node's own id)
  COMMITTED_API_ADDR   default for --target with --live (this host's API)
  COMMITTED_API_TOKEN  bearer token for --live (or --token)`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runBackup()
	},
}

func runBackup() error {
	if backupTo == "" {
		return fmt.Errorf("--to is required (the destination .tar or .tar.gz path)")
	}
	if backupLive {
		if backupDataDir != "" {
			return fmt.Errorf("--data is the offline source and --live the running node's; give one, not both")
		}
		return runLiveBackup()
	}
	dataDir := backupDataDir
	if dataDir == "" {
		dataDir = getenvDefault("COMMITTED_DATA_DIR", "./data")
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
			return fmt.Errorf("%w; see docs/operations/backup.md (or take it live: --live --target <node>)", err)
		}
		return err
	}
	if lockDB != nil {
		defer func() { _ = lockDB.Close() }()
	}

	var m *backup.Manifest
	if err := publishArchive(backupTo, func(w io.Writer) error {
		var err error
		m, err = backup.Create(w, dataDir, nodeID, time.Now())
		return err
	}); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(os.Stdout, "backed up %d files from %s to %s\n", len(m.Files), dataDir, backupTo)
	return nil
}

// runLiveBackup downloads the archive a running node streams from
// GET /v1/node/backup, verifying it as it arrives.
func runLiveBackup() error {
	base, err := apiBaseURL(backupTarget)
	if err != nil {
		return err
	}
	req, err := nethttp.NewRequest(nethttp.MethodGet, base+nodeBackupPath, nil)
	if err != nil {
		return err
	}
	if token := apiToken(backupToken); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	// No overall timeout: the archive can be terabytes. The node bounds its
	// own writes; Ctrl-C bounds ours.
	resp, err := apiClient(backupInsecure, 0).Do(req)
	if err != nil {
		return fmt.Errorf("backup: GET %s%s: %w", base, nodeBackupPath, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != nethttp.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("backup: %s%s returned %d: %s", base, nodeBackupPath, resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	var m *backup.Manifest
	if err := publishArchive(backupTo, func(w io.Writer) error {
		var err error
		m, err = verifyArchiveStream(io.TeeReader(resp.Body, w))
		return err
	}); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(os.Stdout, "backed up %d files live from node %d (%s) at applied index %d to %s\n",
		len(m.Files), m.NodeID, base, m.AppliedIndex, backupTo)
	return nil
}

// errArchiveCutShort is a stream that ended before its manifest and
// without the node's abort marker: the connection was cut, or one side
// stalled past the other's bound.
var errArchiveCutShort = errors.New("backup stream ended before its manifest (the connection was cut, or one side stalled); take the backup again")

// verifyArchiveStream reads a backup tar as it streams by, hashing every
// entry, and returns its manifest once the manifest has been read and every
// file matches it — so what reached the destination is known restorable
// before it is published. Everything read is also what the tee wrote.
func verifyArchiveStream(r io.Reader) (*backup.Manifest, error) {
	tr := tar.NewReader(r)
	seen := map[string]backup.FileEntry{}
	var m *backup.Manifest
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%w (%v)", errArchiveCutShort, err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		if hdr.Name == backup.AbortedName {
			// The node abandoned the stream and says why; nothing after this
			// entry is worth keeping.
			var a backup.Aborted
			if err := json.NewDecoder(tr).Decode(&a); err != nil {
				return nil, fmt.Errorf("backup: %w (reason unreadable: %v)", backup.ErrArchiveAborted, err)
			}
			return nil, fmt.Errorf("backup: %w: %s — take the backup again", backup.ErrArchiveAborted, a.Reason)
		}
		if hdr.Name == backup.ManifestName {
			var got backup.Manifest
			if err := json.NewDecoder(tr).Decode(&got); err != nil {
				return nil, fmt.Errorf("backup: decode manifest: %w", err)
			}
			m = &got
			continue
		}
		h := sha256.New()
		n, err := io.Copy(h, tr) //nolint:gosec // G110: a backup of one node's state, bounded by that node's disk
		if err != nil {
			return nil, fmt.Errorf("%w (%v)", errArchiveCutShort, err)
		}
		seen[hdr.Name] = backup.FileEntry{Path: hdr.Name, Size: n, SHA256: hex.EncodeToString(h.Sum(nil))}
	}
	// Whatever trails the archive's end marker (block padding) belongs in
	// the file too.
	if _, err := io.Copy(io.Discard, r); err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errArchiveCutShort
	}
	if m.FormatVersion != backup.FormatVersion {
		return nil, fmt.Errorf("backup: archive format %d, this binary writes %d", m.FormatVersion, backup.FormatVersion)
	}
	if len(seen) != len(m.Files) {
		return nil, fmt.Errorf("backup: archive holds %d files, its manifest lists %d", len(seen), len(m.Files))
	}
	for _, want := range m.Files {
		got, ok := seen[want.Path]
		if !ok || got != want {
			return nil, fmt.Errorf("backup: archive entry %q does not match its manifest record", want.Path)
		}
	}
	return m, nil
}

// publishArchive writes an archive to a temp file alongside `to` through
// write, then fsyncs and renames it into place, so a partial or failed
// backup never appears at `to`. A ".gz" destination is gzip-compressed.
//
// A backup archive holds the node's entire state — the event log and BoltDB
// metadata, including any PII — so the file is owner-only (0600), not
// os.Create's world-readable 0666&~umask. The mode survives the rename.
// Chmod after open forces 0600 even when O_CREATE reuses a stale .partial
// from a hard-killed prior run (which O_CREATE would otherwise leave at its
// old, possibly looser, perms).
func publishArchive(to string, write func(w io.Writer) error) error {
	tmp := to + ".partial"
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
	if strings.HasSuffix(to, ".gz") {
		gz = gzip.NewWriter(f)
		w = gz
	}
	if err := write(w); err != nil {
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
		cleanup()
		return fmt.Errorf("fsync %q: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("finalize %q: %w", tmp, err)
	}
	if err := os.Rename(tmp, to); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("publish backup to %q: %w", to, err)
	}
	// Persist the rename in the destination's parent directory.
	if err := fsutil.SyncDir(filepath.Dir(to)); err != nil {
		return fmt.Errorf("fsync backup dir after publish: %w", err)
	}
	return nil
}

func init() {
	backupCmd.Flags().StringVar(&backupDataDir, "data", "", "node data directory to archive (default $COMMITTED_DATA_DIR or ./data); offline mode")
	backupCmd.Flags().StringVar(&backupTo, "to", "", "destination backup path (.tar or .tar.gz); required")
	backupCmd.Flags().Uint64Var(&backupNodeID, "node-id", 0, "node id to record in the manifest for provenance (default $COMMITTED_NODE_ID); offline mode")
	backupCmd.Flags().BoolVar(&backupLive, "live", false, "take the backup from a running node over its API (GET /v1/node/backup)")
	backupCmd.Flags().StringVar(&backupTarget, "target", "", "with --live: base URL of the node's API (default: local COMMITTED_API_ADDR)")
	backupCmd.Flags().StringVar(&backupToken, "token", "", "with --live: API bearer token (default: COMMITTED_API_TOKEN)")
	backupCmd.Flags().BoolVar(&backupInsecure, "insecure", false, "with --live: skip TLS certificate verification for an https target")
	rootCmd.AddCommand(backupCmd)
}
