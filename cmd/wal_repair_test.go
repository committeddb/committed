package cmd

import (
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/backup"
	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// seedRaftLog writes n raft entries with recognizable payloads into a fresh
// node dir through the real storage, so the entry log holds real framed
// records a splice can be planned against.
func seedRaftLog(t *testing.T, dataDir string, n int) {
	t.Helper()
	s, err := wal.Open(dataDir, nil, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	for i := 1; i <= n; i++ {
		e := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(uint64(i)), Type: pb.EntryNormal.Enum(), Data: []byte("record-payload-" + string(rune('a'+i)))}
		require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(uint64(i))}, []*pb.Entry{e}, nil))
	}
	require.NoError(t, s.Close())
}

// corruptRaftRecord flips one byte inside the payload of the record that
// carries marker, so that record fails its checksum while the log stays
// structurally readable — the mid-log corruption a backup splice repairs.
func corruptRaftRecord(t *testing.T, dataDir, marker string) (segment string, original []byte) {
	t.Helper()
	entries, err := os.ReadDir(datadir.EntryLogDir(dataDir))
	require.NoError(t, err)
	for _, e := range entries {
		path := filepath.Join(datadir.EntryLogDir(dataDir), e.Name())
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		if i := bytes.Index(data, []byte(marker)); i >= 0 {
			original = append([]byte(nil), data...)
			data[i+len(marker)/2] ^= 0xFF
			require.NoError(t, os.WriteFile(path, data, 0o600))
			return path, original
		}
	}
	t.Fatalf("no entry-log segment carries %q", marker)
	return "", nil
}

func archiveNode(t *testing.T, dataDir string, gz bool) string {
	t.Helper()
	var buf bytes.Buffer
	_, err := backup.Create(&buf, dataDir, 1, time.Now())
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "node.tar")
	if gz {
		path += ".gz"
		var zbuf bytes.Buffer
		zw := gzip.NewWriter(&zbuf)
		_, err = zw.Write(buf.Bytes())
		require.NoError(t, err)
		require.NoError(t, zw.Close())
		buf = zbuf
	}
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))
	return path
}

func withWalRepairFlags(t *testing.T, data, from string, commit bool) {
	t.Helper()
	prevData, prevFrom, prevCommit := walRepairData, walRepairFrom, walRepairCommit
	t.Cleanup(func() { walRepairData, walRepairFrom, walRepairCommit = prevData, prevFrom, prevCommit })
	walRepairData, walRepairFrom, walRepairCommit = data, from, commit
}

// The --from path is the layer that decides whether the data dir is
// mutated: a dry run plans and touches nothing, --commit applies and
// verifies, a .gz archive is detected by its suffix, and a backup that
// cannot supply the record is refused with a non-zero exit rather than a
// partial repair.
func TestRunWalSplice_DryRunCommitGzipAndRefusal(t *testing.T) {
	dataDir := t.TempDir()
	seedRaftLog(t, dataDir, 6)
	tar := archiveNode(t, dataDir, false)
	tgz := archiveNode(t, dataDir, true)
	segment, original := corruptRaftRecord(t, dataDir, "record-payload-d")
	corrupted, err := os.ReadFile(segment)
	require.NoError(t, err)

	// Dry run: the log stays corrupt and byte-identical.
	withWalRepairFlags(t, dataDir, tar, false)
	require.NoError(t, runWalSplice())
	after, err := os.ReadFile(segment)
	require.NoError(t, err)
	require.Equal(t, corrupted, after, "a dry run must not touch the log")
	diag, err := wal.DiagnoseLog(datadir.EntryLogDir(dataDir))
	require.NoError(t, err)
	require.NotEqual(t, wal.LogClean, diag.Status)

	// A gzip archive is read through its suffix; still a dry run.
	withWalRepairFlags(t, dataDir, tgz, false)
	require.NoError(t, runWalSplice())

	// Commit: the record is restored and the log re-scans clean.
	withWalRepairFlags(t, dataDir, tar, true)
	require.NoError(t, runWalSplice())
	after, err = os.ReadFile(segment)
	require.NoError(t, err)
	require.Equal(t, original, after, "the spliced segment must equal the pre-corruption bytes")
	diag, err = wal.DiagnoseLog(datadir.EntryLogDir(dataDir))
	require.NoError(t, err)
	require.Equal(t, wal.LogClean, diag.Status, diag.Detail)

	// Refusal: a backup of a different node cannot supply the record.
	other := t.TempDir()
	seedRaftLog(t, other, 2)
	foreign := archiveNode(t, other, false)
	corruptRaftRecord(t, dataDir, "record-payload-e")
	withWalRepairFlags(t, dataDir, foreign, true)
	err = runWalSplice()
	require.Error(t, err)
	require.Contains(t, err.Error(), "refused")
	diag, err = wal.DiagnoseLog(datadir.EntryLogDir(dataDir))
	require.NoError(t, err)
	require.NotEqual(t, wal.LogClean, diag.Status, "a refused splice must leave the log as it was")
}

// wal decompress needs --data, and on a data dir with no compressed segments
// it reports the dir already downgrade-ready without touching it.
func TestWalDecompress_RequiresDataAndIsANoOpOnPlainLogs(t *testing.T) {
	prev := walDecompressData
	t.Cleanup(func() { walDecompressData = prev })

	walDecompressData = ""
	require.ErrorContains(t, walDecompressCmd.RunE(walDecompressCmd, nil), "--data is required")

	dataDir := t.TempDir()
	seedRaftLog(t, dataDir, 3)
	before, err := os.ReadDir(datadir.EventsDir(dataDir))
	require.NoError(t, err)
	walDecompressData = dataDir
	require.NoError(t, walDecompressCmd.RunE(walDecompressCmd, nil))
	after, err := os.ReadDir(datadir.EventsDir(dataDir))
	require.NoError(t, err)
	require.Equal(t, len(before), len(after), "nothing to rewrite, nothing rewritten")
}
