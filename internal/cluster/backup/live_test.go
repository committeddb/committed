package backup_test

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/backup"
)

// fakeLiveSource hands out a fixed set of entries the way a running node
// would: name, size, and a writer of the content.
type fakeLiveSource struct {
	entries []tarEntry
	info    backup.LiveInfo
	// shortBy makes one entry write fewer bytes than it declared.
	shortBy map[string]int
	// failAfter makes the capture fail after that many entries, as a node
	// overtaken by its own maintenance does.
	failAfter int
	failWith  error
}

func (f *fakeLiveSource) CaptureBackup(visit func(name string, size int64, write func(io.Writer) error) error) (backup.LiveInfo, error) {
	for i, e := range f.entries {
		if f.failWith != nil && i == f.failAfter {
			return f.info, f.failWith
		}
		content := e.content
		if n := f.shortBy[e.name]; n > 0 {
			content = content[:len(content)-n]
		}
		if err := visit(e.name, int64(len(e.content)), func(w io.Writer) error {
			_, err := io.WriteString(w, content)
			return err
		}); err != nil {
			return f.info, err
		}
	}
	return f.info, nil
}

func liveEntries() []tarEntry {
	return []tarEntry{
		{"metadata/bbolt.db", "bolt-bytes"},
		{"raft/state/00000000000000000001", "state-log-bytes"},
		{"events/00000000000000000001", "event-log-bytes"},
		{"raft/log/00000000000000000001", "entry-log-bytes"},
	}
}

// A live archive is the offline archive's shape: Restore takes it as is,
// and its manifest carries what the node captured.
func TestCreateLive_RestoresLikeAnOfflineArchive(t *testing.T) {
	src := &fakeLiveSource{entries: liveEntries(), info: backup.LiveInfo{DataDir: "/var/lib/committed", AppliedIndex: 4242, EventLogGeneration: 7}}
	var buf bytes.Buffer
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	m, err := backup.CreateLive(&buf, src, 3, now)
	require.NoError(t, err)
	require.True(t, m.Live)
	require.Equal(t, uint64(3), m.NodeID)
	require.Equal(t, uint64(4242), m.AppliedIndex)
	require.Equal(t, uint64(7), m.EventLogGeneration)
	require.Equal(t, "/var/lib/committed", m.Source)
	require.Equal(t, backup.FormatVersion, m.FormatVersion)
	want := make([]backup.FileEntry, 0, 4)
	for _, e := range liveEntries() {
		want = append(want, fileEntry(e.name, e.content))
	}
	require.Equal(t, want, m.Files, "every entry hashed as it streamed")

	target := filepath.Join(t.TempDir(), "restored")
	got, err := backup.Restore(&buf, target, now)
	require.NoError(t, err)
	require.Equal(t, m.Files, got.Files)
	require.True(t, got.Live)
	for _, e := range liveEntries() {
		data, err := os.ReadFile(filepath.Join(target, filepath.FromSlash(e.name)))
		require.NoError(t, err)
		require.Equal(t, e.content, string(data))
	}
}

// A store that produced other than the size it declared is a store that
// changed under the reader: the archive is refused, not finished — and the
// cut-off entry is padded so the abort marker can still follow it and name
// the reason.
func TestCreateLive_RefusesAShortEntry(t *testing.T) {
	src := &fakeLiveSource{entries: liveEntries(), shortBy: map[string]int{"events/00000000000000000001": 3}}
	var buf bytes.Buffer
	_, err := backup.CreateLive(&buf, src, 1, time.Now())
	require.ErrorIs(t, err, backup.ErrLiveShortEntry)
	requireAborted(t, buf.Bytes(), "declared 15 bytes, wrote 12")
}

// A capture missing a canonical subtree would restore a hollow node; it is
// refused before the manifest is written, and the archive says so.
func TestCreateLive_RefusesAHollowCapture(t *testing.T) {
	src := &fakeLiveSource{entries: liveEntries()[:2]}
	var buf bytes.Buffer
	_, err := backup.CreateLive(&buf, src, 1, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing")
	require.False(t, errors.Is(err, backup.ErrLiveShortEntry))
	requireAborted(t, buf.Bytes(), "missing")
}

// requireAborted asserts an archive is well-formed tar ending in an
// ABORTED.json whose reason contains want, and that Restore refuses it by
// that reason.
func requireAborted(t *testing.T, archive []byte, want string) {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(archive))
	var last string
	var aborted backup.Aborted
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err, "the archive must stay well-formed tar to the end")
		last = hdr.Name
		if hdr.Name == backup.AbortedName {
			require.NoError(t, json.NewDecoder(tr).Decode(&aborted))
		}
	}
	require.Equal(t, backup.AbortedName, last, "the marker is the last entry")
	require.Contains(t, aborted.Reason, want)
	_, err := backup.Restore(bytes.NewReader(archive), filepath.Join(t.TempDir(), "restored"), time.Now())
	require.ErrorIs(t, err, backup.ErrArchiveAborted)
	require.ErrorContains(t, err, want)
}

// A capture that fails after the stream has begun leaves the archive
// carrying the reason as its last entry: a client can say why, and Restore
// refuses the archive naming it — never "not a committed backup".
func TestCreateLive_AnAbortedStreamCarriesItsReason(t *testing.T) {
	src := &fakeLiveSource{entries: liveEntries(), failAfter: 2, failWith: errors.New("the node installed a snapshot during the backup")}
	var buf bytes.Buffer
	_, err := backup.CreateLive(&buf, src, 1, time.Now())
	require.ErrorContains(t, err, "installed a snapshot")

	tr := tar.NewReader(bytes.NewReader(buf.Bytes()))
	var names []string
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		names = append(names, hdr.Name)
	}
	require.Equal(t, []string{"metadata/bbolt.db", "raft/state/00000000000000000001", backup.AbortedName}, names)
	requireAborted(t, buf.Bytes(), "installed a snapshot")
}
