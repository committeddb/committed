package wal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/backup"
)

// entryPayload is a marshaled raft entry at index — what the entry logs frame.
func entryPayload(t *testing.T, index uint64, data string) []byte {
	t.Helper()
	b, err := proto.Marshal(&pb.Entry{Index: proto.Uint64(index), Term: proto.Uint64(1), Data: []byte(data)})
	require.NoError(t, err)
	return b
}

func entries(t *testing.T, indices ...uint64) [][]byte {
	t.Helper()
	out := make([][]byte, 0, len(indices))
	for _, i := range indices {
		out = append(out, entryPayload(t, i, fmt.Sprintf("entry-%d", i)))
	}
	return out
}

// scaffoldNode fills in whatever a complete node directory needs that the
// test did not create itself (backup.Create refuses a hollow tree): a clean
// one-record segment per empty log, and a placeholder metadata db.
func scaffoldNode(t *testing.T, base string) {
	t.Helper()
	for _, parts := range walLogSubdirs {
		dir := logDir(t, base, parts...)
		ents, err := os.ReadDir(dir)
		require.NoError(t, err)
		if len(ents) == 0 {
			writeSegment(t, dir, 1, [][]byte{entryPayload(t, 1, "scaffold")})
		}
	}
	meta := filepath.Join(base, "metadata")
	require.NoError(t, os.MkdirAll(meta, 0o700))
	boltPath := filepath.Join(meta, "bbolt.db")
	if _, err := os.Stat(boltPath); os.IsNotExist(err) {
		db, err := bolt.Open(boltPath, 0o600, nil) // backup.Create opens it: a real (empty) bbolt file
		require.NoError(t, err)
		require.NoError(t, db.Close())
	}
}

// archiveOf takes a backup of base exactly as `committed backup` would.
func archiveOf(t *testing.T, base string) []byte {
	t.Helper()
	scaffoldNode(t, base)
	var buf bytes.Buffer
	_, err := backup.Create(&buf, base, 1, time.Now())
	require.NoError(t, err)
	return buf.Bytes()
}

// flipInRecord flips the last payload byte of the record at ordinal in a
// plain segment file, so the record fails its frame but stays structurally
// complete.
func flipInRecord(t *testing.T, path string, ordinal int) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	spans, clean := parseSpans(data)
	require.True(t, clean)
	data[spans[ordinal].end()-1] ^= 0xFF
	require.NoError(t, os.WriteFile(path, data, 0o600))
}

func logDir(t *testing.T, base string, parts ...string) string {
	t.Helper()
	dir := filepath.Join(append([]string{base}, parts...)...)
	require.NoError(t, os.MkdirAll(dir, 0o700))
	return dir
}

func reportFor(t *testing.T, reports []*SpliceReport, dir string) *SpliceReport {
	t.Helper()
	for _, r := range reports {
		if r.Dir == dir {
			return r
		}
	}
	t.Fatalf("no report for %s", dir)
	return nil
}

func TestSplice_RecordFromBackupIntoPlainSegment(t *testing.T) {
	base := t.TempDir()
	events := logDir(t, base, "events")
	seg := writeSegment(t, events, 1, entries(t, 10, 11, 12))
	original, err := os.ReadFile(seg)
	require.NoError(t, err)
	archive := archiveOf(t, base)
	flipInRecord(t, seg, 1)
	corrupted, err := os.ReadFile(seg)
	require.NoError(t, err)

	// Dry run: the plan names the record; nothing changes.
	reports, err := SpliceNode(base, bytes.NewReader(archive), false)
	require.NoError(t, err)
	require.Len(t, reports, 3, "one report per log dir")
	rep := reportFor(t, reports, events)
	require.Equal(t, LogCorrupt, rep.Before.Status)
	require.Empty(t, rep.Refused)
	require.False(t, rep.Applied)
	require.Contains(t, rep.Plan, "splice record at sequence 2")
	after, err := os.ReadFile(seg)
	require.NoError(t, err)
	require.Equal(t, corrupted, after, "a dry run must not touch the log")

	// Commit: the record is restored byte-for-byte and the log re-scans clean.
	reports, err = SpliceNode(base, bytes.NewReader(archive), true)
	require.NoError(t, err)
	rep = reportFor(t, reports, events)
	require.True(t, rep.Applied, rep.Refused)
	require.Equal(t, LogClean, rep.After.Status)
	after, err = os.ReadFile(seg)
	require.NoError(t, err)
	require.Equal(t, original, after, "the spliced segment must equal the pre-corruption bytes")
	d, err := DiagnoseLog(events)
	require.NoError(t, err)
	require.Equal(t, LogClean, d.Status)

	// Idempotent: a clean log has nothing to splice.
	reports, err = SpliceNode(base, bytes.NewReader(archive), true)
	require.NoError(t, err)
	rep = reportFor(t, reports, events)
	require.False(t, rep.Applied)
	require.Empty(t, rep.Plan)
	require.Empty(t, rep.Refused)
}

func TestSplice_CompressedSegmentReplacedFromBackup(t *testing.T) {
	for _, backupCompressed := range []bool{true, false} {
		t.Run(fmt.Sprintf("backupCompressed=%v", backupCompressed), func(t *testing.T) {
			base := t.TempDir()
			events := logDir(t, base, "events")
			plainPath := writeSegment(t, events, 1, entries(t, 10, 11, 12))
			plain, err := os.ReadFile(plainPath)
			require.NoError(t, err)
			writeSegment(t, events, 4, entries(t, 13)) // the plain tail after the sealed segment
			zstPath := plainPath + ".zst"
			compress := func() {
				require.NoError(t, os.WriteFile(zstPath, encodeZstd(plain), 0o600))
				require.NoError(t, os.Remove(plainPath))
			}
			if backupCompressed {
				compress()
			}
			archive := archiveOf(t, base)
			if !backupCompressed {
				compress()
			}
			// One flipped byte poisons the whole zstd frame.
			data, err := os.ReadFile(zstPath)
			require.NoError(t, err)
			data[len(data)/2] ^= 0xFF
			require.NoError(t, os.WriteFile(zstPath, data, 0o600))

			reports, err := SpliceNode(base, bytes.NewReader(archive), false)
			require.NoError(t, err)
			rep := reportFor(t, reports, events)
			require.Equal(t, LogCorrupt, rep.Before.Status)
			require.Contains(t, rep.Before.Detail, "zstd")
			require.Empty(t, rep.Refused)
			require.Contains(t, rep.Plan, "replace compressed segment")

			reports, err = SpliceNode(base, bytes.NewReader(archive), true)
			require.NoError(t, err)
			rep = reportFor(t, reports, events)
			require.True(t, rep.Applied, rep.Refused)
			require.Equal(t, LogClean, rep.After.Status)
			got, err := os.ReadFile(zstPath)
			require.NoError(t, err)
			decoded, err := decodeZstd(got)
			require.NoError(t, err)
			require.Equal(t, plain, decoded, "the replacement decodes to the original segment")
			_, err = os.Stat(plainPath)
			require.True(t, os.IsNotExist(err), "no plain sibling may coexist with the .zst")
		})
	}
}

func TestSplice_RefusesCorruptionTheBackupDoesNotCover(t *testing.T) {
	base := t.TempDir()
	events := logDir(t, base, "events")
	seg := writeSegment(t, events, 1, entries(t, 10, 11))
	archive := archiveOf(t, base) // covers sequences 1–2
	writeSegment(t, events, 1, entries(t, 10, 11, 12))
	flipInRecord(t, seg, 2) // sequence 3: after the backup
	before, err := os.ReadFile(seg)
	require.NoError(t, err)

	reports, err := SpliceNode(base, bytes.NewReader(archive), true)
	require.NoError(t, err)
	rep := reportFor(t, reports, events)
	require.False(t, rep.Applied)
	require.Contains(t, rep.Refused, "covers sequences 1–2")
	after, err := os.ReadFile(seg)
	require.NoError(t, err)
	require.Equal(t, before, after, "a refused splice must not touch the log")
}

func TestSplice_RefusesALogRewrittenSinceTheBackup(t *testing.T) {
	// The backup predates a rewrite (a scrub, a truncation): another record
	// differs, so the backup's bytes are not the log's truth — a splice would
	// re-introduce them (an RTBF erasure, above all). Refuse.
	base := t.TempDir()
	events := logDir(t, base, "events")
	seg := writeSegment(t, events, 1, [][]byte{entryPayload(t, 10, "entry-10"), entryPayload(t, 11, "entry-11"), entryPayload(t, 12, "entry-12")})
	archive := archiveOf(t, base)
	// Same lengths, different bytes at record 0 (the "scrubbed" record).
	writeSegment(t, events, 1, [][]byte{entryPayload(t, 10, "ERASED10"), entryPayload(t, 11, "entry-11"), entryPayload(t, 12, "entry-12")})
	flipInRecord(t, seg, 1)
	before, err := os.ReadFile(seg)
	require.NoError(t, err)

	reports, err := SpliceNode(base, bytes.NewReader(archive), true)
	require.NoError(t, err)
	rep := reportFor(t, reports, events)
	require.False(t, rep.Applied)
	require.Contains(t, rep.Refused, "differs between the log and the backup")
	after, err := os.ReadFile(seg)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestSplice_RefusesARecordThatBreaksIndexContinuity(t *testing.T) {
	// raft/log is contiguous: the backup's record decodes to index 50 between
	// 10 and 12 — same length, aligned neighbours, wrong entry. Refuse.
	base := t.TempDir()
	raftLog := logDir(t, base, "raft", "log")
	seg := writeSegment(t, raftLog, 10, [][]byte{entryPayload(t, 10, "entry-10"), entryPayload(t, 50, "entry-50"), entryPayload(t, 12, "entry-12")})
	archive := archiveOf(t, base)
	writeSegment(t, raftLog, 10, entries(t, 10, 11, 12))
	flipInRecord(t, seg, 1)

	reports, err := SpliceNode(base, bytes.NewReader(archive), true)
	require.NoError(t, err)
	rep := reportFor(t, reports, raftLog)
	require.False(t, rep.Applied)
	require.Contains(t, rep.Refused, "does not continue the preceding record's 10")
}

func TestSplice_RefusesATamperedArchive(t *testing.T) {
	base := t.TempDir()
	events := logDir(t, base, "events")
	seg := writeSegment(t, events, 1, entries(t, 10, 11, 12))
	original, err := os.ReadFile(seg)
	require.NoError(t, err)
	archive := archiveOf(t, base)
	// Rot one byte of the segment INSIDE the archive: the manifest hash no
	// longer matches, so the archive is refused whole.
	at := bytes.Index(archive, original[:16])
	require.Positive(t, at)
	archive[at+8] ^= 0xFF
	flipInRecord(t, seg, 1)
	before, err := os.ReadFile(seg)
	require.NoError(t, err)

	_, err = SpliceNode(base, bytes.NewReader(archive), true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match its manifest record")
	after, err := os.ReadFile(seg)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestSplice_RefusesAMidCompactionDirectory(t *testing.T) {
	base := t.TempDir()
	events := logDir(t, base, "events")
	writeSegment(t, events, 1, entries(t, 10, 11, 12))
	archive := archiveOf(t, base)
	require.NoError(t, os.WriteFile(filepath.Join(events, "00000000000000000002.START"), nil, 0o600))

	reports, err := SpliceNode(base, bytes.NewReader(archive), true)
	require.NoError(t, err)
	rep := reportFor(t, reports, events)
	require.Equal(t, LogCorrupt, rep.Before.Status)
	require.False(t, rep.Applied)
	require.Contains(t, rep.Refused, "not a shape a backup can repair")
}

func TestSplice_SweepsAStaleRepairTemp(t *testing.T) {
	base := t.TempDir()
	events := logDir(t, base, "events")
	seg := writeSegment(t, events, 1, entries(t, 10))
	archive := archiveOf(t, base)
	stale := seg + repairTmpSuffix
	require.NoError(t, os.WriteFile(stale, []byte("crashed before rename"), 0o600))

	reports, err := SpliceNode(base, bytes.NewReader(archive), false)
	require.NoError(t, err)
	rep := reportFor(t, reports, events)
	require.Equal(t, LogClean, rep.Before.Status)
	_, err = os.Stat(stale)
	require.True(t, os.IsNotExist(err), "a temp left by a crash before its rename is swept")
	require.True(t, strings.HasSuffix(stale, ".repair.tmp"))
}
