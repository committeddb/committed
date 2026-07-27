// Package backup implements committed's offline backup/restore primitive: a
// portable tar archive of a node's on-disk state plus a manifest.
//
// It is OFFLINE by design. A backup is taken from a node's data directory
// while the node is STOPPED — the directory is then quiescent, so the whole
// of it (the raft entry/state logs, the permanent event log, and the BoltDB
// metadata) is trivially consistent and can be archived with a plain file
// walk. Restore unpacks the archive into a fresh data directory; a node
// started against it recovers exactly as it would from its own disk — there
// is no raft-state reconstruction, the restored directory IS a node's
// directory.
//
// The node holds an exclusive OS lock on its BoltDB file for its whole life, so
// a backup cannot be read from a running node's directory by a second process —
// the CLI takes a shared lock on it and HOLDS it for the whole archive, so a node
// can neither be running when the backup starts nor start (and write) part-way
// through it. To back up a live cluster, stop one follower (quorum holds on the
// rest), archive it, and start it again — the same rolling discipline as a
// rolling upgrade. See docs/operations/backup.md.
package backup

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/internal/cluster/fsutil"
	"github.com/committeddb/committed/internal/version"
)

// FormatVersion is the backup archive format. Restore refuses an archive whose
// manifest declares a different version rather than guessing at an incompatible
// layout — including an older v1 archive, whose manifest carried only file paths
// with no per-file integrity, which this binary no longer decodes.
const FormatVersion = 2

// ManifestName is the reserved archive entry holding the backup metadata. Create
// writes it LAST — the per-file hashes it carries are only known after each file
// has been streamed and hashed — so Restore reads it after staging the files and
// verifies the staged tree against it before publishing.
const ManifestName = "MANIFEST.json"

// markerName is written into a restored data directory so the node (and an
// operator) can tell the directory was reconstituted from a backup rather than
// grown in place. It is informational — startup behavior is unchanged.
const markerName = "RESTORED.json"

// FileEntry is one archived file's manifest record: its data-dir-relative path
// (forward-slash), byte size, and SHA-256 (hex) of its content. Restore verifies
// each staged file against these before publishing.
type FileEntry struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// Manifest describes a backup archive. It travels as the LAST archive entry.
type Manifest struct {
	FormatVersion int    `json:"formatVersion"`
	CreatedAt     string `json:"createdAt"` // RFC3339
	// Version and Commit record the committed build that produced the archive —
	// provenance for diagnosing a restore that won't boot. FeatureLevel is the
	// data's compat axis (version.FeatureLevel): Restore REFUSES an archive whose
	// feature level exceeds the running binary's, because it may carry feature
	// entries this binary cannot correctly apply — the runtime apply-path gate
	// only holds back EMITTING a feature, not ingesting one already emitted into a
	// backup. All three are omitempty so a future read of an archive that predates
	// them treats them as zero (no gate) rather than needing a format bump.
	Version      string `json:"version,omitempty"`
	Commit       string `json:"commit,omitempty"`
	FeatureLevel uint64 `json:"featureLevel,omitempty"`
	// NodeID is the COMMITTED_NODE_ID the operator recorded for the source
	// node (0 if not supplied). The id is runtime config, not stored in the
	// data directory, so it is captured here only for provenance — restore
	// does not depend on it.
	NodeID uint64 `json:"nodeID,omitempty"`
	// Source is the data directory the backup was taken from (provenance).
	Source string `json:"source,omitempty"`
	// Files lists every archived entry with its size and SHA-256. Restore treats
	// it as the authoritative set: it verifies each staged file's hash, rejects
	// any staged entry the manifest does not list, and requires every listed file
	// to be present — so bit-rot, a truncated transfer, an injected/extra entry,
	// or a same-name override in the archive fails the restore loudly instead of
	// silently reconstituting corrupt node state. (Integrity, not authenticity: a
	// tamperer who rewrites the manifest to match altered files is not defended
	// here — that needs a signed manifest.)
	Files []FileEntry `json:"files,omitempty"`
}

// Marker is written to RESTORED.json in a restored data directory.
type Marker struct {
	RestoredAt string   `json:"restoredAt"` // RFC3339
	From       Manifest `json:"from"`
}

// Create archives the data directory at dataDir into a tar stream written to w,
// followed by a trailing MANIFEST.json entry (each file is hashed as it streams,
// so the manifest's per-file hashes are known only after the files are written).
// dataDir MUST belong to a stopped node (the caller is responsible for that — see
// the CLI's held shared lock); a live directory would produce an inconsistent
// archive. Returns the manifest it wrote.
//
// Only regular files are archived; directories are recreated by Restore from
// the file paths, so empty directories are not preserved (a node recreates the
// ones it needs on startup).
//
// A symlink — the data root itself, or any entry under it — is a hard error, NOT
// a skip. filepath.Walk does not follow symlinks (it Lstats each entry), so a
// symlinked store dir or a symlinked data root would otherwise be dropped from
// both the archive AND the manifest built from the same walk: a hollow backup
// that passes every completeness check on restore. Create fails closed instead,
// and refuses a data dir with no regular files at all.
func Create(w io.Writer, dataDir string, nodeID uint64, now time.Time) (*Manifest, error) {
	// Lstat, not Stat: Stat follows a symlinked root, which would then walk to
	// zero files and report a hollow success. Reject the symlink up front.
	info, err := os.Lstat(dataDir)
	if err != nil {
		return nil, fmt.Errorf("backup: stat data dir: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("backup: data dir %q is a symlink; point --data at the real directory so a symlinked store can't be silently dropped from the archive", dataDir)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("backup: data dir %q is not a directory", dataDir)
	}

	// One walk to collect the regular files (the directory is quiescent, so it
	// won't change between listing and copying); the manifest lists exactly
	// what we then write. Recovery residue is excluded — see canonicalArchiveEntry.
	type entry struct {
		rel  string
		full string
	}
	// A node's Open rolls a crashed scrub swap back to events.retired/ when
	// events/ is absent; the archive must follow suit, so decide once whether the
	// canonical event log lives in events/ or in events.retired/.
	eventsPresent := dirExists(datadir.EventsDir(dataDir))
	var entries []entry
	walkErr := filepath.Walk(dataDir, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dataDir, path)
		if err != nil {
			return err
		}
		keep, archiveRel := datadir.CanonicalArchiveEntry(filepath.ToSlash(rel), eventsPresent)
		if !keep {
			// Recovery residue the node's Open would sweep — never part of the
			// logical node, and (events.retired/, bbolt.db.restore.*) a carrier of
			// already-erased right-to-be-forgotten data that must not go off-box.
			return nil
		}
		// Fail closed on a non-regular canonical entry (symlink, device, …): Walk
		// won't descend a symlinked dir, so archiving it would silently omit the
		// real files.
		if !fi.Mode().IsRegular() {
			return fmt.Errorf("backup: refusing to archive %q: not a regular file (mode %s) — a symlinked or special entry under the data dir would be silently dropped; materialize the real files under the data dir", path, fi.Mode())
		}
		entries = append(entries, entry{rel: archiveRel, full: path})
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("backup: walk data dir: %w", walkErr)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("backup: data dir %q has no regular files to archive — refusing to write an empty backup", dataDir)
	}

	// Refuse to mint a hollow archive before writing anything: a data dir missing
	// a canonical subtree (most dangerously metadata/bbolt.db) would restore a
	// node that boots with fresh-empty metadata, silently losing all config and
	// checkpoints. Guards against a wrong or partially-wiped --data.
	entryPaths := make([]string, len(entries))
	for i, e := range entries {
		entryPaths[i] = e.rel
	}
	if err := datadir.RequireCompleteNodeDir(entryPaths); err != nil {
		return nil, fmt.Errorf("backup: %w", err)
	}

	manifest := &Manifest{
		FormatVersion: FormatVersion,
		CreatedAt:     now.UTC().Format(time.RFC3339),
		Version:       version.Version,
		Commit:        version.Commit,
		FeatureLevel:  version.FeatureLevel,
		NodeID:        nodeID,
		Source:        dataDir,
		Files:         make([]FileEntry, 0, len(entries)),
	}

	tw := tar.NewWriter(w)

	// Single pass: stream each file into the archive while hashing it, so its
	// FileEntry (size + SHA-256) is recorded without a second read of the data —
	// the event log can be very large, and a two-pass "hash then copy" would
	// double the read.
	for _, e := range entries {
		fe, err := copyTarFile(tw, e.rel, e.full)
		if err != nil {
			return nil, err
		}
		manifest.Files = append(manifest.Files, fe)
	}

	// The manifest is the LAST entry — its per-file hashes are only known after
	// the files are streamed. Restore reads it after staging and verifies the
	// staged tree against it before publishing. (A truncated archive drops the
	// trailing manifest, and Restore rejects it as "not a committed backup".)
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("backup: marshal manifest: %w", err)
	}
	if err := writeTarFile(tw, ManifestName, 0o600, manifestBytes); err != nil {
		return nil, err
	}

	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("backup: close archive: %w", err)
	}
	return manifest, nil
}

// dirExists reports whether path is an existing directory.
func dirExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

// Restore unpacks a backup tar stream from r into targetDir, validating the
// manifest and refusing any entry whose path escapes targetDir. targetDir must
// not already exist or must be empty — Restore never overwrites an existing
// populated directory. On success it writes a RESTORED.json marker and returns
// the manifest.
//
// Restore is atomic: it unpacks into a staging directory alongside targetDir and
// renames it into place only after the archive validates completely. A mid-restore
// failure (a truncated archive, a bad entry, a disk-full write) leaves targetDir
// untouched — no half-populated directory to block a retry (requireEmptyDir) or
// boot a Frankenstein node.
func Restore(r io.Reader, targetDir string, now time.Time) (*Manifest, error) {
	// Normalize away a trailing slash so filepath.Dir yields the real parent for
	// the staging sibling (Dir("/data/node/") would otherwise be "/data/node").
	targetDir = filepath.Clean(targetDir)
	if err := requireEmptyDir(targetDir); err != nil {
		return nil, err
	}

	// Stage in a sibling of targetDir so the final publish is a same-filesystem
	// rename (atomic); os.MkdirTemp needs the parent to exist.
	parent := filepath.Dir(targetDir)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return nil, fmt.Errorf("restore: create parent dir: %w", err)
	}
	staging, err := os.MkdirTemp(parent, ".committed-restore-*")
	if err != nil {
		return nil, fmt.Errorf("restore: create staging dir: %w", err)
	}
	// Disarmed only once the staging dir is renamed into place; until then any
	// early return removes the partial restore so nothing is left behind.
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(staging)
		}
	}()

	tr := tar.NewReader(r)
	var manifest *Manifest
	staged := make(map[string]FileEntry) // archive entry name -> {size, sha256} as written to staging
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("restore: read archive: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}

		if hdr.Name == ManifestName {
			data, err := io.ReadAll(tr)
			if err != nil {
				return nil, fmt.Errorf("restore: read manifest: %w", err)
			}
			m := &Manifest{}
			if err := json.Unmarshal(data, m); err != nil {
				return nil, fmt.Errorf("restore: parse manifest: %w", err)
			}
			if m.FormatVersion != FormatVersion {
				return nil, fmt.Errorf("restore: unsupported backup format version %d (this binary supports %d)", m.FormatVersion, FormatVersion)
			}
			// FeatureLevel gate: an archive from a NEWER binary may carry feature
			// entries this build cannot correctly apply, so refuse it early (before
			// verify/publish) with an operator-legible message rather than a cryptic
			// runtime fatal after boot. Older/equal levels are accepted.
			if m.FeatureLevel > version.FeatureLevel {
				return nil, fmt.Errorf("restore: backup was produced at feature level %d but this binary supports only %d — restore with a build at feature level %d or newer; see docs/operations/backup.md", m.FeatureLevel, version.FeatureLevel, m.FeatureLevel)
			}
			manifest = m
			continue
		}

		dest, err := safeJoin(staging, hdr.Name)
		if err != nil {
			return nil, err
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
			return nil, fmt.Errorf("restore: create dir for %q: %w", hdr.Name, err)
		}
		// Mask to the 12 Unix mode bits (perm + setuid/setgid/sticky) before the
		// int64->uint32 conversion: it's all writeFile can honor, and the bound
		// makes the narrowing provably safe (no gosec G115 truncation risk).
		n, sum, err := writeFile(dest, tr, os.FileMode(hdr.Mode&0o7777))
		if err != nil {
			return nil, err
		}
		staged[hdr.Name] = FileEntry{Path: hdr.Name, Size: n, SHA256: sum}
	}

	if manifest == nil {
		return nil, fmt.Errorf("restore: archive has no %s — not a committed backup", ManifestName)
	}

	// Verify the staged tree against the manifest BEFORE publishing: allow-list
	// (no entry the manifest didn't list), completeness (nothing listed is
	// missing), and per-file integrity (size + SHA-256). Any mismatch — bit-rot,
	// a truncated transfer, an injected or same-name-overridden entry — aborts
	// here with targetDir untouched (the deferred RemoveAll of the staging dir).
	if err := verifyStaged(staged, manifest.Files); err != nil {
		return nil, err
	}

	// Refuse to publish a hollow tree: a valid-but-incomplete archive (a tampered
	// or under-listing manifest) missing a canonical subtree would restore a node
	// that boots with fresh-empty metadata, silently losing all config.
	if err := datadir.RequireCompleteNodeDir(filePaths(manifest.Files)); err != nil {
		return nil, fmt.Errorf("restore: %w", err)
	}

	// Record provenance in the marker, not the full (possibly large) file list.
	provenance := *manifest
	provenance.Files = nil
	marker := Marker{RestoredAt: now.UTC().Format(time.RFC3339), From: provenance}
	markerBytes, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("restore: marshal marker: %w", err)
	}
	markerPath := filepath.Join(staging, markerName)
	if err := os.WriteFile(markerPath, markerBytes, 0o600); err != nil {
		return nil, fmt.Errorf("restore: write marker: %w", err)
	}
	if err := fsutil.SyncFile(markerPath); err != nil {
		return nil, fmt.Errorf("restore: fsync marker: %w", err)
	}

	// Make the whole staged tree durable before publishing: every file's content is
	// already fsync'd (writeFile / marker), so fsync every directory to persist
	// their entries. Otherwise `restore` could report success with the
	// reconstituted node still only in the page cache — a crash then leaves a torn
	// or partial data dir, which is the disaster-recovery path this tool exists for.
	if err := syncDirTree(staging); err != nil {
		return nil, fmt.Errorf("restore: fsync staged tree before publish: %w", err)
	}

	// Publish atomically. requireEmptyDir guaranteed targetDir was absent or
	// empty; remove an empty existing one so the rename lands on a clean name
	// (renaming onto an existing dir is not portable).
	if err := os.Remove(targetDir); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("restore: clear target dir: %w", err)
	}
	if err := os.Rename(staging, targetDir); err != nil {
		return nil, fmt.Errorf("restore: publish staged restore to target dir: %w", err)
	}
	published = true

	// Persist the rename itself: it is atomic for a concurrent reader but not
	// durable until the target's parent directory is fsync'd.
	if err := fsutil.SyncDir(filepath.Dir(targetDir)); err != nil {
		return nil, fmt.Errorf("restore: fsync target parent after publish: %w", err)
	}

	return manifest, nil
}

// safeJoin resolves name (an archive entry path) under base, rejecting — loudly,
// not silently neutralizing — absolute paths and any ".." traversal (the
// classic tar/zip-slip guard). A malicious or corrupt archive fails the
// restore rather than having its entry quietly relocated.
func safeJoin(base, name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("restore: empty archive entry name")
	}
	cleaned := filepath.Clean(filepath.FromSlash(name))
	sep := string(os.PathSeparator)
	if filepath.IsAbs(cleaned) || cleaned == ".." || strings.HasPrefix(cleaned, ".."+sep) {
		return "", fmt.Errorf("restore: archive entry %q escapes the target directory", name)
	}
	joined := filepath.Join(base, cleaned)
	// Defense in depth: the resolved path must still sit within base.
	baseClean := filepath.Clean(base)
	if joined != baseClean && !strings.HasPrefix(joined, baseClean+sep) {
		return "", fmt.Errorf("restore: archive entry %q escapes the target directory", name)
	}
	return joined, nil
}

func requireEmptyDir(dir string) error {
	f, err := os.Open(dir) //nolint:gosec // G304: the target dir is operator-supplied via --data
	if err != nil {
		if os.IsNotExist(err) {
			return nil // will be created
		}
		return fmt.Errorf("restore: open target dir: %w", err)
	}
	defer func() { _ = f.Close() }()
	names, err := f.Readdirnames(1)
	if err != nil && err != io.EOF {
		return fmt.Errorf("restore: read target dir: %w", err)
	}
	if len(names) > 0 {
		return fmt.Errorf("restore: target dir %q is not empty; restore refuses to overwrite existing data", dir)
	}
	return nil
}

// filePaths extracts the forward-slash paths from a manifest's file list.
func filePaths(fes []FileEntry) []string {
	out := make([]string, len(fes))
	for i, fe := range fes {
		out[i] = fe.Path
	}
	return out
}

// verifyStaged checks the staged tree against the manifest before publish. staged
// maps each written archive entry (by forward-slash name) to the size and SHA-256
// observed while staging it; want is the manifest's authoritative file list. It
// enforces three things, failing on the first violation:
//   - allow-list: every staged entry is one the manifest lists (an unlisted entry
//     is an injected/malformed archive — e.g. a stray file dropped into raft/log/
//     that would break the node's Open);
//   - completeness: every listed file is present (else the archive is truncated);
//   - integrity: each listed file's staged size and SHA-256 match (else bit-rot, a
//     torn transfer, or a same-name entry that overwrote a good one in staging).
func verifyStaged(staged map[string]FileEntry, want []FileEntry) error {
	wantByPath := make(map[string]FileEntry, len(want))
	for _, fe := range want {
		wantByPath[fe.Path] = fe
	}
	for name := range staged {
		if _, ok := wantByPath[name]; !ok {
			return fmt.Errorf("restore: archive contains %q which the manifest does not list — refusing a tampered or malformed archive", name)
		}
	}
	for _, fe := range want {
		got, ok := staged[fe.Path]
		if !ok {
			return fmt.Errorf("restore: manifest lists %q but it is missing from the archive", fe.Path)
		}
		if got.Size != fe.Size {
			return fmt.Errorf("restore: %q size mismatch: archive has %d bytes, manifest expects %d — the archive is corrupt or truncated", fe.Path, got.Size, fe.Size)
		}
		if got.SHA256 != fe.SHA256 {
			return fmt.Errorf("restore: %q checksum mismatch — the archive is corrupt or tampered; rebuild from a healthy source (see docs/operations/rebuild.md)", fe.Path)
		}
	}
	return nil
}

func writeTarFile(tw *tar.Writer, name string, mode int64, data []byte) error {
	hdr := &tar.Header{Name: name, Mode: mode, Size: int64(len(data)), Typeflag: tar.TypeReg}
	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("backup: write header %q: %w", name, err)
	}
	if _, err := tw.Write(data); err != nil {
		return fmt.Errorf("backup: write %q: %w", name, err)
	}
	return nil
}

// copyTarFile streams the file at full into the archive as name while hashing it,
// returning its FileEntry (size + SHA-256). One read feeds both the tar and the
// hash via io.MultiWriter, so recording the hash costs no extra read.
func copyTarFile(tw *tar.Writer, name, full string) (FileEntry, error) {
	fi, err := os.Stat(full)
	if err != nil {
		return FileEntry{}, fmt.Errorf("backup: stat %q: %w", full, err)
	}
	hdr, err := tar.FileInfoHeader(fi, "")
	if err != nil {
		return FileEntry{}, fmt.Errorf("backup: header %q: %w", full, err)
	}
	hdr.Name = name
	if err := tw.WriteHeader(hdr); err != nil {
		return FileEntry{}, fmt.Errorf("backup: write header %q: %w", name, err)
	}
	f, err := os.Open(full) //nolint:gosec // G304: full is a file under the operator-supplied data dir being archived
	if err != nil {
		return FileEntry{}, fmt.Errorf("backup: open %q: %w", full, err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tw, h), f)
	if err != nil {
		return FileEntry{}, fmt.Errorf("backup: copy %q: %w", name, err)
	}
	return FileEntry{Path: name, Size: n, SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

// writeFile writes r into dest (fsync'd before Close for crash-durability),
// hashing the content as it streams, and returns the bytes written and the
// SHA-256 (hex). Restore verifies these against the manifest before publishing.
func writeFile(dest string, r io.Reader, mode os.FileMode) (int64, string, error) {
	if mode == 0 {
		mode = 0o600
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode) //nolint:gosec // G304: dest is validated by safeJoin to be within the target dir
	if err != nil {
		return 0, "", fmt.Errorf("restore: create %q: %w", dest, err)
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), r) //nolint:gosec // G110: restoring an operator's own backup; tar entries are bounded by the source node's on-disk state
	if err != nil {
		_ = f.Close()
		return 0, "", fmt.Errorf("restore: write %q: %w", dest, err)
	}
	// fsync the content before Close so a crash after the publish rename cannot
	// surface a torn or zero-length file (io.Copy does not fsync). syncDirTree
	// then persists the directory entries before the rename.
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return 0, "", fmt.Errorf("restore: fsync %q: %w", dest, err)
	}
	if err := f.Close(); err != nil {
		return 0, "", fmt.Errorf("restore: close %q: %w", dest, err)
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

// syncDirTree fsyncs every directory under root (inclusive), so that renaming root
// into place persists all of its entries — not just root's top level. File contents
// are already fsync'd at write time (writeFile / the marker write); this covers the
// directory entries that point at them.
func syncDirTree(root string) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return fsutil.SyncDir(path)
		}
		return nil
	})
}
