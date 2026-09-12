package backup

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/internal/version"
)

// A live backup is the same archive as the offline one — the same entries,
// the same manifest, restored by the same command — taken from a RUNNING
// node. The node hands this package a consistent reading of each store
// (a LiveSource); this package writes the tar and the manifest. What makes
// the reading consistent is the node's business (wal.Storage.CaptureBackup): the
// order the stores are read in, and what each read holds still.

// LiveInfo is what a running node reports about the state it captured.
type LiveInfo struct {
	DataDir string
	// AppliedIndex is the applied index in the captured metadata — the
	// point the archive restores to.
	AppliedIndex uint64
	// EventLogGeneration is the scrub bound the captured event log's bytes
	// reflect (see db.EventServer.EventLogGeneration).
	EventLogGeneration uint64
}

// LiveSource is a running node's consistent reading of its stores.
// CaptureBackup calls visit once per archive entry, in the node's consistent order, with
// the entry's data-dir-relative name, its exact size, and a writer of
// exactly that many bytes; it returns what it captured.
type LiveSource interface {
	CaptureBackup(visit func(name string, size int64, write func(w io.Writer) error) error) (LiveInfo, error)
}

// ErrLiveShortEntry is a live capture that wrote other than the size it
// declared for an entry — the store changed under the reader.
var ErrLiveShortEntry = errors.New("live backup: an entry's content did not match its declared size")

// AbortedName is the archive entry a live capture writes LAST when it
// fails after the stream has begun — the status is already out, so the
// reason travels in-band: a client reads it instead of the manifest, and
// Restore refuses the archive naming it.
const AbortedName = "ABORTED.json"

// Aborted is the content of an ABORTED.json entry.
type Aborted struct {
	Reason  string `json:"reason"`
	At      string `json:"at"` // RFC3339
	Version string `json:"version,omitempty"`
}

// ErrArchiveAborted is an archive whose node abandoned it mid-stream; the
// wrapped message is the node's reason.
var ErrArchiveAborted = errors.New("the node aborted this backup")

// CreateLive archives a running node's state, as src reads it, into a tar
// stream written to w followed by the trailing MANIFEST.json — the offline
// archive's shape exactly, so Restore takes it as is. nodeID is recorded
// for provenance. Returns the manifest it wrote.
func CreateLive(w io.Writer, src LiveSource, nodeID uint64, now time.Time) (*Manifest, error) {
	tw := tar.NewWriter(w)
	manifest := &Manifest{
		FormatVersion: FormatVersion,
		CreatedAt:     now.UTC().Format(time.RFC3339),
		Version:       version.Version,
		Commit:        version.Commit,
		FeatureLevel:  version.FeatureLevel,
		NodeID:        nodeID,
		Live:          true,
	}
	// Once the first entry has begun, every failure — in the capture, in the
	// completeness check, in the manifest — ends the archive with the
	// ABORTED.json entry naming it, since nothing else can carry the reason
	// then. An entry cut off mid-stream (the store changed under the reader)
	// is first padded to the size its header declared, or the tar could not
	// take another entry; the padding is garbage in an archive Restore
	// refuses on sight of the marker.
	began := false
	var owed int64 // bytes the in-flight entry's header declared but the stream did not deliver
	abort := func(err error) (*Manifest, error) {
		if !began {
			return nil, err
		}
		if owed > 0 {
			_, _ = io.CopyN(tw, zeroReader{}, owed)
		}
		reason, _ := cluster.RedactedMessage(err)
		if marker, merr := json.MarshalIndent(Aborted{Reason: reason, At: now.UTC().Format(time.RFC3339), Version: version.Version}, "", "  "); merr == nil {
			_ = writeTarFile(tw, AbortedName, 0o600, marker)
			_ = tw.Close()
		}
		return nil, err
	}

	info, err := src.CaptureBackup(func(name string, size int64, write func(io.Writer) error) error {
		hdr := &tar.Header{Name: name, Mode: 0o600, Size: size, Typeflag: tar.TypeReg, ModTime: now}
		began = true
		if err := tw.WriteHeader(hdr); err != nil {
			return fmt.Errorf("backup: write header %q: %w", name, err)
		}
		owed = size
		h := sha256.New()
		cw := &countingWriter{w: io.MultiWriter(tw, h)}
		err := write(cw)
		owed = size - cw.n
		if err != nil {
			return fmt.Errorf("backup: stream %q: %w", name, err)
		}
		if cw.n != size {
			return fmt.Errorf("%w: %q declared %d bytes, wrote %d", ErrLiveShortEntry, name, size, cw.n)
		}
		manifest.Files = append(manifest.Files, FileEntry{Path: name, Size: size, SHA256: hex.EncodeToString(h.Sum(nil))})
		return nil
	})
	if err != nil {
		return abort(err)
	}
	manifest.Source = info.DataDir
	manifest.AppliedIndex = info.AppliedIndex
	manifest.EventLogGeneration = info.EventLogGeneration

	paths := make([]string, len(manifest.Files))
	for i, f := range manifest.Files {
		paths[i] = f.Path
	}
	if err := datadir.RequireCompleteNodeDir(paths); err != nil {
		return abort(fmt.Errorf("backup: %w", err))
	}

	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return abort(fmt.Errorf("backup: marshal manifest: %w", err))
	}
	if err := writeTarFile(tw, ManifestName, 0o600, manifestBytes); err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("backup: close archive: %w", err)
	}
	return manifest, nil
}

// zeroReader pads a cut-off entry to its declared size.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// countingWriter counts what passes through it, so a live entry's declared
// size is checked against what the store actually produced.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
