package cmd

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/backup"
)

// nodeArchive is what a node streams from /v1/node/backup: here, an offline
// archive of a complete node directory — the same bytes.
func nodeArchive(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	_, err := backup.Create(&buf, makeCompleteNode(t), 3, time.Now())
	require.NoError(t, err)
	return buf.Bytes()
}

func setLiveFlags(t *testing.T, target, to string) {
	t.Helper()
	prev := []any{backupLive, backupTarget, backupToken, backupInsecure, backupTo, backupDataDir}
	t.Cleanup(func() {
		backupLive, backupTarget, backupToken, backupInsecure, backupTo, backupDataDir = prev[0].(bool), prev[1].(string), prev[2].(string), prev[3].(bool), prev[4].(string), prev[5].(string)
	})
	backupLive, backupTarget, backupToken, backupInsecure, backupTo, backupDataDir = true, target, "tok", false, to, ""
}

// `backup --live` downloads the node's archive with the bearer token,
// verifies every entry against its manifest, and publishes it — plain or
// gzip by the destination's suffix — restorable by `committed restore`.
func TestRunBackup_LiveDownloadsAndVerifies(t *testing.T) {
	archive := nodeArchive(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, nodeBackupPath, r.URL.Path)
		require.Equal(t, "Bearer tok", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/x-tar")
		_, _ = w.Write(archive)
	}))
	defer srv.Close()

	for _, suffix := range []string{".tar", ".tar.gz"} {
		out := filepath.Join(t.TempDir(), "node"+suffix)
		setLiveFlags(t, srv.URL, out)
		require.NoError(t, runBackup())

		f, err := os.Open(out)
		require.NoError(t, err)
		var r io.Reader = f
		if suffix == ".tar.gz" {
			gz, err := gzip.NewReader(f)
			require.NoError(t, err)
			r = gz
		}
		m, err := backup.Restore(r, filepath.Join(t.TempDir(), "restored"), time.Now())
		_ = f.Close()
		require.NoError(t, err, "the published file restores")
		require.Equal(t, uint64(3), m.NodeID)
		info, err := os.Stat(out)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}
}

// A stream the node cut short — no trailing manifest — publishes nothing
// and says to take the backup again; a stream the node aborted says why;
// so does a node that refuses.
func TestRunBackup_LiveRefusesACutOrRefusedStream(t *testing.T) {
	archive := nodeArchive(t)
	cut := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(archive[:len(archive)/2])
	}))
	defer cut.Close()
	out := filepath.Join(t.TempDir(), "node.tar")
	setLiveFlags(t, cut.URL, out)
	err := runBackup()
	require.ErrorIs(t, err, errArchiveCutShort)
	require.NoFileExists(t, out)
	require.NoFileExists(t, out+".partial")

	var aborted bytes.Buffer
	tw := tar.NewWriter(&aborted)
	marker, _ := json.Marshal(backup.Aborted{Reason: "the node installed a snapshot during the backup"})
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: backup.AbortedName, Mode: 0o600, Size: int64(len(marker)), Typeflag: tar.TypeReg}))
	_, _ = tw.Write(marker)
	require.NoError(t, tw.Close())
	abort := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(aborted.Bytes())
	}))
	defer abort.Close()
	setLiveFlags(t, abort.URL, out)
	err = runBackup()
	require.ErrorIs(t, err, backup.ErrArchiveAborted)
	require.ErrorContains(t, err, "installed a snapshot")
	require.NoFileExists(t, out)

	backupDataDir = "/some/dir"
	err = runBackup()
	require.ErrorContains(t, err, "give one, not both")
	backupDataDir = ""

	busy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"code":"backup_in_progress"}`, http.StatusConflict)
	}))
	defer busy.Close()
	setLiveFlags(t, busy.URL, out)
	err = runBackup()
	require.ErrorContains(t, err, "409")
	require.NoFileExists(t, out)
}
