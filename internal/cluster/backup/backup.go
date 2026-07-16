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
// The node holds an exclusive OS lock on its BoltDB file for its whole life,
// so a backup cannot be read from a running node's directory by a second
// process — the CLI enforces this with a lock probe before archiving. To back
// up a live cluster, stop one follower (quorum holds on the rest), archive it,
// and start it again — the same rolling discipline as a rolling upgrade. See
// docs/operations/backup.md.
package backup

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// FormatVersion is the backup archive format. Restore refuses an archive whose
// manifest declares a different version rather than guessing at an
// incompatible layout.
const FormatVersion = 1

// ManifestName is the reserved archive entry holding the backup metadata. It is
// written first so `tar tf` and Restore both see it before the data.
const ManifestName = "MANIFEST.json"

// markerName is written into a restored data directory so the node (and an
// operator) can tell the directory was reconstituted from a backup rather than
// grown in place. It is informational — startup behavior is unchanged.
const markerName = "RESTORED.json"

// Manifest describes a backup archive. It travels as the first archive entry.
type Manifest struct {
	FormatVersion int    `json:"formatVersion"`
	CreatedAt     string `json:"createdAt"` // RFC3339
	// NodeID is the COMMITTED_NODE_ID the operator recorded for the source
	// node (0 if not supplied). The id is runtime config, not stored in the
	// data directory, so it is captured here only for provenance — restore
	// does not depend on it.
	NodeID uint64 `json:"nodeID,omitempty"`
	// Source is the data directory the backup was taken from (provenance).
	Source string `json:"source,omitempty"`
	// Files are the archived entries' paths, relative to the data directory,
	// in forward-slash form. Restore checks every one is present after
	// unpacking, so a truncated archive fails loudly instead of restoring a
	// partial directory.
	Files []string `json:"files"`
}

// Marker is written to RESTORED.json in a restored data directory.
type Marker struct {
	RestoredAt string   `json:"restoredAt"` // RFC3339
	From       Manifest `json:"from"`
}

// Create archives the data directory at dataDir into a tar stream written to
// w, prefixed by a MANIFEST.json entry. dataDir MUST belong to a stopped node
// (the caller is responsible for that — see the CLI's lock probe); a live
// directory would produce an inconsistent archive. Returns the manifest it
// wrote.
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
	// what we then write.
	type entry struct {
		rel  string
		full string
	}
	var entries []entry
	walkErr := filepath.Walk(dataDir, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() {
			return nil
		}
		// Fail closed on anything that is not a regular file (symlink, device,
		// socket, …). Walk won't descend a symlinked dir, so archiving it would
		// silently omit the real files.
		if !fi.Mode().IsRegular() {
			return fmt.Errorf("backup: refusing to archive %q: not a regular file (mode %s) — a symlinked or special entry under the data dir would be silently dropped; materialize the real files under the data dir", path, fi.Mode())
		}
		rel, err := filepath.Rel(dataDir, path)
		if err != nil {
			return err
		}
		entries = append(entries, entry{rel: filepath.ToSlash(rel), full: path})
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("backup: walk data dir: %w", walkErr)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("backup: data dir %q has no regular files to archive — refusing to write an empty backup", dataDir)
	}

	manifest := &Manifest{
		FormatVersion: FormatVersion,
		CreatedAt:     now.UTC().Format(time.RFC3339),
		NodeID:        nodeID,
		Source:        dataDir,
		Files:         make([]string, 0, len(entries)),
	}
	for _, e := range entries {
		manifest.Files = append(manifest.Files, e.rel)
	}

	tw := tar.NewWriter(w)

	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("backup: marshal manifest: %w", err)
	}
	if err := writeTarFile(tw, ManifestName, 0o600, manifestBytes); err != nil {
		return nil, err
	}

	for _, e := range entries {
		if err := copyTarFile(tw, e.rel, e.full); err != nil {
			return nil, err
		}
	}

	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("backup: close archive: %w", err)
	}
	return manifest, nil
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
		if err := writeFile(dest, tr, os.FileMode(hdr.Mode)); err != nil {
			return nil, err
		}
	}

	if manifest == nil {
		return nil, fmt.Errorf("restore: archive has no %s — not a committed backup", ManifestName)
	}

	// Completeness: every file the manifest lists must be present, so a
	// truncated archive is caught here rather than silently restoring a
	// partial node directory.
	for _, rel := range manifest.Files {
		dest, err := safeJoin(staging, rel)
		if err != nil {
			return nil, err
		}
		if _, err := os.Stat(dest); err != nil {
			return nil, fmt.Errorf("restore: manifest lists %q but it is missing from the archive: %w", rel, err)
		}
	}

	marker := Marker{RestoredAt: now.UTC().Format(time.RFC3339), From: *manifest}
	markerBytes, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("restore: marshal marker: %w", err)
	}
	if err := os.WriteFile(filepath.Join(staging, markerName), markerBytes, 0o600); err != nil {
		return nil, fmt.Errorf("restore: write marker: %w", err)
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

func copyTarFile(tw *tar.Writer, name, full string) error {
	fi, err := os.Stat(full)
	if err != nil {
		return fmt.Errorf("backup: stat %q: %w", full, err)
	}
	hdr, err := tar.FileInfoHeader(fi, "")
	if err != nil {
		return fmt.Errorf("backup: header %q: %w", full, err)
	}
	hdr.Name = name
	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("backup: write header %q: %w", name, err)
	}
	f, err := os.Open(full) //nolint:gosec // G304: full is a file under the operator-supplied data dir being archived
	if err != nil {
		return fmt.Errorf("backup: open %q: %w", full, err)
	}
	defer func() { _ = f.Close() }()
	if _, err := io.Copy(tw, f); err != nil {
		return fmt.Errorf("backup: copy %q: %w", name, err)
	}
	return nil
}

func writeFile(dest string, r io.Reader, mode os.FileMode) error {
	if mode == 0 {
		mode = 0o600
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode) //nolint:gosec // G304: dest is validated by safeJoin to be within the target dir
	if err != nil {
		return fmt.Errorf("restore: create %q: %w", dest, err)
	}
	if _, err := io.Copy(f, r); err != nil { //nolint:gosec // G110: restoring an operator's own backup; tar entries are bounded by the source node's on-disk state
		_ = f.Close()
		return fmt.Errorf("restore: write %q: %w", dest, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("restore: close %q: %w", dest, err)
	}
	return nil
}
