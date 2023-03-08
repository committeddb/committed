package raft_test

import (
	"bytes"
	"io"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/lni/dragonboat/v4/raftio"
	"github.com/lni/dragonboat/v4/raftpb"
	"github.com/philborlin/committed/internal/controlplane/raft"
	"github.com/stretchr/testify/assert"
)

var defaultFS = fstest.MapFS{
	"replicas/1":            {Mode: fs.ModeDir},
	"replicas/1/2":          {Mode: fs.ModeDir},
	"replicas/1/3":          {Mode: fs.ModeDir},
	"replicas/1/4/replicas": {Mode: fs.ModeDir},
}

func TestName(t *testing.T) {
	w, _ := raft.NewWALLogDB(defaultFS, nil)
	assert.Equal(t, "wal", w.Name())
}

func TestBinaryFormat(t *testing.T) {
	w, _ := raft.NewWALLogDB(defaultFS, nil)
	assert.Equal(t, uint32(0), w.BinaryFormat())
}

// TODO ListNodeInfo test create
func TestListNodeInfo(t *testing.T) {
	w, walError := raft.NewWALLogDB(defaultFS, nil)
	is, nodeErr := w.ListNodeInfo()

	assert.Nil(t, walError)
	assert.Nil(t, nodeErr)
	assert.Equal(t, raftio.NodeInfo{ReplicaID: 1, ShardID: 2}, is[0])
	assert.Equal(t, raftio.NodeInfo{ReplicaID: 1, ShardID: 3}, is[1])
	assert.Equal(t, raftio.NodeInfo{ReplicaID: 1, ShardID: 4}, is[2])
}

func TestListNodeInfoErrors(t *testing.T) {
	tests := map[string]struct {
		input string
		err   string
	}{
		"no replica": {input: "a/b/c", err: "open replicas"},
		"bad rid":    {input: "replicas/b/c", err: "parsing \"b\""},
		"bad sid":    {input: "replicas/1/c", err: "parsing \"c\""},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			fsys := fstest.MapFS{tc.input: {Mode: fs.ModeDir}}

			w, _ := raft.NewWALLogDB(fsys, nil)
			_, err := w.ListNodeInfo()

			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.err)
		})
	}
}

func TestBootstrapInfo(t *testing.T) {
	fsys := fstest.MapFS{"replicas/1/1": {Mode: fs.ModeDir}}
	w, _ := raft.NewWALLogDB(fsys, &MapFSFileWriter{fsys})

	bs := raftpb.Bootstrap{
		Addresses: map[uint64]string{uint64(1): "123", uint64(2): "456"},
		Join:      true,
		Type:      raftpb.StateMachineType(1),
	}

	err := w.SaveBootstrapInfo(1, 1, bs)
	assert.Nil(t, err)

	bs2, err := w.GetBootstrapInfo(1, 1)
	assert.Nil(t, err)
	assert.Equal(t, bs, bs2)
}

type MapFSFileWriter struct {
	fs fstest.MapFS
}

func (w *MapFSFileWriter) Open(name string) (io.WriteCloser, error) {
	return &MapFSWriteCloser{fs: w.fs, name: name, bb: &bytes.Buffer{}}, nil
}

type MapFSWriteCloser struct {
	fs   fstest.MapFS
	name string
	bb   *bytes.Buffer
}

func (wc *MapFSWriteCloser) Write(p []byte) (n int, err error) {
	return wc.bb.Write(p)
}

func (wc *MapFSWriteCloser) Close() error {
	wc.fs[wc.name] = &fstest.MapFile{Data: wc.bb.Bytes()}
	return nil
}
