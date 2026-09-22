package tidwall

import (
	"bytes"
	"errors"
	"testing"

	native "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestLegacyRewritePreservesSurvivorBytes(t *testing.T) {
	path := t.TempDir()
	writer, err := CreateLegacyRewrite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Close() }()
	frames := [][]byte{[]byte("first framed survivor"), []byte("second framed survivor"), []byte("locked delta")}
	if writer.Count() != 0 {
		t.Fatal(writer.Count())
	}
	for _, frame := range frames[:2] {
		if err := writer.Append(frame); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := writer.CompressSealed(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(frames[2]); err != nil {
		t.Fatal(err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if writer.Count() != 3 {
		t.Fatal(writer.Count())
	}
	if err := writer.Append([]byte("after close")); err == nil || writer.Count() != 3 {
		t.Fatal("failed append advanced survivor count", err)
	}
	reopened, err := native.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	first, err := reopened.FirstIndex()
	if err != nil || first != 1 {
		t.Fatal(first, err)
	}
	last, err := reopened.LastIndex()
	if err != nil || last != 3 {
		t.Fatal(last, err)
	}
	for i, want := range frames {
		got, err := reopened.Read(uint64(i + 1))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatal("rewrite changed survivor bytes", err)
		}
	}
}

func TestLegacyRewriteRejectsExistingHistory(t *testing.T) {
	path := t.TempDir()
	log, err := native.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Write(1, []byte("existing")); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if writer, err := CreateLegacyRewrite(path); !errors.Is(err, eventlog.ErrInvalid) || writer != nil {
		t.Fatal(writer, err)
	}
	log, err = native.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	if got, err := log.Read(1); err != nil || string(got) != "existing" {
		t.Fatal("rejected creation changed existing history", err)
	}
}
