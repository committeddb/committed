package backup_test

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/backup"
)

// writeTree lays out a mock node data directory: the same shape wal.Open
// uses (raft/log, raft/state, events, metadata) with a file in each.
func writeTree(t *testing.T) (string, map[string]string) {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"raft/log/00000000000000000001":   "entry-log-bytes",
		"raft/state/00000000000000000001": "state-log-bytes",
		"events/00000000000000000001":     "event-log-bytes",
		"metadata/bbolt.db":               "bolt-bytes",
	}
	for rel, content := range files {
		full := filepath.Join(dir, filepath.FromSlash(rel))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o700))
		require.NoError(t, os.WriteFile(full, []byte(content), 0o600))
	}
	return dir, files
}

// --- helpers for hand-built archives (manifest LAST, per-file hashes) ---

type tarEntry struct{ name, content string }

func hashHex(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

// fileEntry is the manifest record for content stored under name.
func fileEntry(name, content string) backup.FileEntry {
	return backup.FileEntry{Path: name, Size: int64(len(content)), SHA256: hashHex(content)}
}

// writeArchive writes the given file entries (in order) followed by the manifest
// as the LAST entry — the same layout Create produces — letting a test control
// exactly what the tar holds versus what the manifest claims.
func writeArchive(t *testing.T, w io.Writer, files []tarEntry, m backup.Manifest) {
	t.Helper()
	tw := tar.NewWriter(w)
	for _, e := range files {
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: e.name, Mode: 0o600, Size: int64(len(e.content)), Typeflag: tar.TypeReg}))
		_, err := tw.Write([]byte(e.content))
		require.NoError(t, err)
	}
	mb, err := json.MarshalIndent(m, "", "  ")
	require.NoError(t, err)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: backup.ManifestName, Mode: 0o600, Size: int64(len(mb)), Typeflag: tar.TypeReg}))
	_, err = tw.Write(mb)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
}

func paths(fes []backup.FileEntry) []string {
	out := make([]string, len(fes))
	for i, fe := range fes {
		out[i] = fe.Path
	}
	return out
}

func TestCreateRestore_RoundTrip(t *testing.T) {
	src, files := writeTree(t)

	var buf bytes.Buffer
	now := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	m, err := backup.Create(&buf, src, 7, now)
	require.NoError(t, err)
	require.Equal(t, backup.FormatVersion, m.FormatVersion)
	require.Equal(t, uint64(7), m.NodeID)
	require.Len(t, m.Files, len(files))

	dst := filepath.Join(t.TempDir(), "restored")
	rm, err := backup.Restore(&buf, dst, now)
	require.NoError(t, err)
	require.Equal(t, m.Files, rm.Files)

	// Every file came back byte-for-byte.
	for rel, content := range files {
		got, err := os.ReadFile(filepath.Join(dst, filepath.FromSlash(rel)))
		require.NoError(t, err, rel)
		require.Equal(t, content, string(got), rel)
	}
	// And the restore marker is present.
	marker, err := os.ReadFile(filepath.Join(dst, "RESTORED.json"))
	require.NoError(t, err)
	require.Contains(t, string(marker), "restoredAt")
}

// TestCreate_FailsClosedOnSymlinkedStore: a store dir symlinked in under the
// data root must FAIL the backup, not be silently skipped. filepath.Walk does not
// descend a symlinked dir, so the real files (and the manifest entries for them)
// would otherwise vanish — a hollow backup that passes restore's completeness
// check.
func TestCreate_FailsClosedOnSymlinkedStore(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "raft", "log"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "raft", "log", "0001"), []byte("x"), 0o600))
	// A real store living elsewhere, linked in under the data dir.
	realStore := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(realStore, "0001"), []byte("events"), 0o600))
	require.NoError(t, os.Symlink(realStore, filepath.Join(dir, "events")))

	var buf bytes.Buffer
	_, err := backup.Create(&buf, dir, 0, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a regular file")
}

// TestCreate_FailsClosedOnSymlinkedDataRoot: a symlinked data root must FAIL, not
// walk to zero files and report a hollow success.
func TestCreate_FailsClosedOnSymlinkedDataRoot(t *testing.T) {
	real, _ := writeTree(t)
	link := filepath.Join(t.TempDir(), "datalink")
	require.NoError(t, os.Symlink(real, link))

	var buf bytes.Buffer
	_, err := backup.Create(&buf, link, 0, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "symlink")
}

// TestCreate_RejectsEmptyDataDir: a data dir with no regular files yields no
// archivable content, so Create refuses rather than writing an empty backup that
// would "restore" a hollow node.
func TestCreate_RejectsEmptyDataDir(t *testing.T) {
	var buf bytes.Buffer
	_, err := backup.Create(&buf, t.TempDir(), 0, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "no regular files")
}

// TestRestore_MidFailureLeavesTargetCleanAndRetryable proves restore atomicity: a
// failure partway through (here a truncated archive caught by the completeness
// check, after some files are already staged) leaves NO target directory behind,
// so a retry is not blocked by a half-restored dir — and a subsequent good restore
// into the same target succeeds.
func TestRestore_MidFailureLeavesTargetCleanAndRetryable(t *testing.T) {
	// Archive whose manifest lists two files but contains only one.
	var bad bytes.Buffer
	writeArchive(t, &bad, []tarEntry{{"events/0001", "present"}}, backup.Manifest{
		FormatVersion: backup.FormatVersion,
		Files:         []backup.FileEntry{fileEntry("events/0001", "present"), fileEntry("raft/log/0001", "gone")},
	})

	parent := t.TempDir()
	target := filepath.Join(parent, "restored")

	_, err := backup.Restore(&bad, target, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing from the archive")

	// No half-restored target, and no staging leftovers in the parent.
	_, statErr := os.Stat(target)
	require.True(t, os.IsNotExist(statErr), "a mid-restore failure must leave no target dir; got %v", statErr)
	parentEntries, err := os.ReadDir(parent)
	require.NoError(t, err)
	require.Empty(t, parentEntries, "the staging dir must be cleaned up on failure")

	// Retry with a good archive into the same target: the earlier failure left
	// nothing to block it.
	src, files := writeTree(t)
	var good bytes.Buffer
	_, err = backup.Create(&good, src, 0, time.Now())
	require.NoError(t, err)
	_, err = backup.Restore(&good, target, time.Now())
	require.NoError(t, err, "a good restore must succeed after a failed one")
	for rel, content := range files {
		got, rerr := os.ReadFile(filepath.Join(target, filepath.FromSlash(rel)))
		require.NoError(t, rerr, rel)
		require.Equal(t, content, string(got), rel)
	}
}

// TestCreate_ExcludesRecoveryResidue: a backup must archive the canonical,
// post-recovery node state — never the transient recovery residue a crash/scrub
// leaves, which the node's Open sweeps. Two of these residuals (the pre-scrub
// events.retired/ log and the bbolt.db.restore.* snapshot temp) hold data a scrub
// already erased on the live node; archiving them would carry already-erased
// right-to-be-forgotten subjects into an immutable off-box archive.
func TestCreate_ExcludesRecoveryResidue(t *testing.T) {
	dir := t.TempDir()
	canonical := []string{
		"raft/log/00000000000000000001",
		"raft/state/00000000000000000001",
		"events/00000000000000000001",
		"metadata/bbolt.db",
	}
	residue := []string{
		"events.retired/00000000000000000001",     // pre-scrub log — erased PII
		"events.scrub.5/00000000000000000001",     // in-progress rewrite temp
		"raft/log.discarded/00000000000000000001", // superseded entry log
		"metadata/bbolt.db.restore.1720000000",    // orphaned snapshot temp — erased key
		"metadata/bbolt.db.compact.1720000001",    // orphaned compaction temp
	}
	for _, rel := range append(append([]string{}, canonical...), residue...) {
		full := filepath.Join(dir, filepath.FromSlash(rel))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o700))
		require.NoError(t, os.WriteFile(full, []byte("data:"+rel), 0o600))
	}

	var buf bytes.Buffer
	m, err := backup.Create(&buf, dir, 0, time.Now())
	require.NoError(t, err)
	require.ElementsMatch(t, canonical, paths(m.Files),
		"the archive must contain exactly the canonical node state, none of the swept-at-Open recovery residue")
}

// TestCreate_RemapsRetiredEventLogWhenEventsAbsent: a scrub swap crashed after
// renaming events/ out, so events.retired/ is the ONLY event log — the node's
// Open rolls it back to events/. The backup must do the same (remap it to
// events/), or it would restore an empty log. This is NOT a leak: the erasure did
// not complete, so the pre-scrub log is the node's current canonical state.
func TestCreate_RemapsRetiredEventLogWhenEventsAbsent(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"raft/log/00000000000000000001":       "entry",
		"raft/state/00000000000000000001":     "state",
		"metadata/bbolt.db":                   "bolt",
		"events.retired/00000000000000000001": "the-only-event-log",
		"events.scrub.9/00000000000000000001": "half-built-clean-log", // discarded by Open
	}
	for rel, content := range files {
		full := filepath.Join(dir, filepath.FromSlash(rel))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o700))
		require.NoError(t, os.WriteFile(full, []byte(content), 0o600))
	}

	var buf bytes.Buffer
	m, err := backup.Create(&buf, dir, 0, time.Now())
	require.NoError(t, err)
	require.Contains(t, paths(m.Files), "events/00000000000000000001",
		"the retired log must be archived remapped to events/ so the restore is not empty")
	require.NotContains(t, paths(m.Files), "events.retired/00000000000000000001")
	require.NotContains(t, paths(m.Files), "events.scrub.9/00000000000000000001")

	// Round-trip: the restored events/ holds the retired log's bytes.
	dst := filepath.Join(t.TempDir(), "r")
	_, err = backup.Restore(&buf, dst, time.Now())
	require.NoError(t, err)
	got, err := os.ReadFile(filepath.Join(dst, "events", "00000000000000000001"))
	require.NoError(t, err)
	require.Equal(t, "the-only-event-log", string(got))
}

// TestRestore_RefusesNonEmptyTarget guards against clobbering an existing
// (possibly live) node directory.
func TestRestore_RefusesNonEmptyTarget(t *testing.T) {
	src, _ := writeTree(t)
	var buf bytes.Buffer
	_, err := backup.Create(&buf, src, 0, time.Now())
	require.NoError(t, err)

	dst := t.TempDir() // exists and...
	require.NoError(t, os.WriteFile(filepath.Join(dst, "preexisting"), []byte("x"), 0o600))

	_, err = backup.Restore(&buf, dst, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "not empty")
}

// TestRestore_RejectsPathTraversal proves the zip-slip guard: a crafted
// archive entry escaping the target is rejected during staging.
func TestRestore_RejectsPathTraversal(t *testing.T) {
	var buf bytes.Buffer
	writeArchive(t, &buf, []tarEntry{{"../escape", "pwned"}}, backup.Manifest{
		FormatVersion: backup.FormatVersion,
		Files:         []backup.FileEntry{fileEntry("../escape", "pwned")},
	})

	dst := filepath.Join(t.TempDir(), "restored")
	_, err := backup.Restore(&buf, dst, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "escapes")
}

// TestRestore_RejectsWrongFormatVersion refuses an archive from an
// incompatible backup format (including an old v1 archive this binary no longer
// decodes).
func TestRestore_RejectsWrongFormatVersion(t *testing.T) {
	var buf bytes.Buffer
	writeArchive(t, &buf, nil, backup.Manifest{FormatVersion: backup.FormatVersion + 1})

	_, err := backup.Restore(&buf, filepath.Join(t.TempDir(), "r"), time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported backup format version")
}

// TestRestore_RejectsMissingManifest refuses a tar that isn't a committed
// backup.
func TestRestore_RejectsMissingManifest(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "random.txt", Mode: 0o600, Size: 3, Typeflag: tar.TypeReg}))
	_, _ = tw.Write([]byte("abc"))
	require.NoError(t, tw.Close())

	_, err := backup.Restore(&buf, filepath.Join(t.TempDir(), "r"), time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a committed backup")
}

// TestRestore_DetectsTruncatedArchive: the manifest lists a file the archive
// doesn't actually contain.
func TestRestore_DetectsTruncatedArchive(t *testing.T) {
	var buf bytes.Buffer
	writeArchive(t, &buf, nil, backup.Manifest{
		FormatVersion: backup.FormatVersion,
		Files:         []backup.FileEntry{fileEntry("events/0001", "whatever")},
	})

	_, err := backup.Restore(&buf, filepath.Join(t.TempDir(), "r"), time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing from the archive")
}

// TestRestore_DetectsCorruptFile is the core Fix A pin: a same-size bit-flip in an
// archived file (bit-rot / at-rest tamper) that the manifest hash does not match
// must fail the restore — before the corrupt bytes are ever published or a node
// opens them — rather than silently restoring wrong node state.
func TestRestore_DetectsCorruptFile(t *testing.T) {
	var buf bytes.Buffer
	writeArchive(t, &buf, []tarEntry{{"metadata/bbolt.db", "evil"}}, backup.Manifest{
		FormatVersion: backup.FormatVersion,
		Files:         []backup.FileEntry{fileEntry("metadata/bbolt.db", "good")}, // hash of "good", same size
	})

	_, err := backup.Restore(&buf, filepath.Join(t.TempDir(), "r"), time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum mismatch")
}

// TestRestore_RejectsUnlistedEntry: a tampered archive that injects a file the
// manifest does not list — e.g. a stray segment dropped into raft/log/ — is
// refused by the allow-list, not silently written into the node dir.
func TestRestore_RejectsUnlistedEntry(t *testing.T) {
	var buf bytes.Buffer
	writeArchive(t, &buf, []tarEntry{
		{"events/0001", "legit"},
		{"raft/log/injected", "evil"},
	}, backup.Manifest{
		FormatVersion: backup.FormatVersion,
		Files:         []backup.FileEntry{fileEntry("events/0001", "legit")},
	})

	_, err := backup.Restore(&buf, filepath.Join(t.TempDir(), "r"), time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not list")
}

// TestRestore_RejectsSameNameOverride: two entries for the same path — the second
// (malicious) overwrites the good one in staging (O_TRUNC). The manifest's hash is
// for the good content, so the HASH check catches the substituted bytes; an
// allow-list alone would not (both entries are "listed").
func TestRestore_RejectsSameNameOverride(t *testing.T) {
	var buf bytes.Buffer
	writeArchive(t, &buf, []tarEntry{
		{"events/0001", "good"},
		{"events/0001", "evil"},
	}, backup.Manifest{
		FormatVersion: backup.FormatVersion,
		Files:         []backup.FileEntry{fileEntry("events/0001", "good")},
	})

	_, err := backup.Restore(&buf, filepath.Join(t.TempDir(), "r"), time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum mismatch")
}

// TestCreate_RejectsHollowDataDir: a data dir missing a canonical subtree (here
// metadata/bbolt.db) is refused — it would restore a node that boots with fresh
// empty metadata, silently losing all config/checkpoints. Guards a wrong or
// partially-wiped --data.
func TestCreate_RejectsHollowDataDir(t *testing.T) {
	dir := t.TempDir()
	for _, rel := range []string{ // everything but metadata/bbolt.db
		"events/00000000000000000001",
		"raft/log/00000000000000000001",
		"raft/state/00000000000000000001",
	} {
		full := filepath.Join(dir, filepath.FromSlash(rel))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o700))
		require.NoError(t, os.WriteFile(full, []byte("x"), 0o600))
	}

	var buf bytes.Buffer
	_, err := backup.Create(&buf, dir, 0, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "metadata/bbolt.db")
}

// TestRestore_RejectsHollowArchive: an archive whose per-file hashes all verify
// but that is structurally incomplete (no metadata/bbolt.db) is refused before
// publish — otherwise the node would boot with fresh-empty metadata. This is the
// axis Fix A's hashing does NOT cover: a valid-but-hollow archive.
func TestRestore_RejectsHollowArchive(t *testing.T) {
	var buf bytes.Buffer
	writeArchive(t, &buf, []tarEntry{
		{"events/0001", "e"},
		{"raft/log/0001", "l"},
		{"raft/state/0001", "s"},
	}, backup.Manifest{
		FormatVersion: backup.FormatVersion,
		Files: []backup.FileEntry{
			fileEntry("events/0001", "e"),
			fileEntry("raft/log/0001", "l"),
			fileEntry("raft/state/0001", "s"),
		},
	})

	_, err := backup.Restore(&buf, filepath.Join(t.TempDir(), "r"), time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "metadata/bbolt.db")
}
