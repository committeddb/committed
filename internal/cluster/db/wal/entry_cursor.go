package wal

import (
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// entryCursor is the application-side traversal boundary. SeekGE replaces the
// position; Current returns a read-only entry retained until Advance, another
// seek, invalidation, or Close. EOF is temporary. Failed reads do not advance.
// The owner serializes calls and excludes rewrite publication while using entries.
type entryCursor interface {
	SeekGE(uint64) error
	Current() (*pb.Entry, error)
	Advance()
	Close() error
}

// decodedEntryCursor retains decoding work independently of physical positioning.
// Seeking is lazy: storage/decode errors surface from Current.
type decodedEntryCursor struct {
	seek              func(uint64) (*pb.Entry, error)
	close             func() error
	target            uint64
	current           *pb.Entry
	closed, exhausted bool
}

func (c *decodedEntryCursor) SeekGE(index uint64) error {
	if c.closed {
		return eventlog.ErrClosed
	}
	c.target, c.current, c.exhausted = index, nil, false
	return nil
}

func (c *decodedEntryCursor) Current() (*pb.Entry, error) {
	if c.closed {
		return nil, eventlog.ErrClosed
	}
	if c.exhausted {
		return nil, eventlog.ErrNotFound
	}
	if c.current == nil {
		entry, err := c.seek(c.target)
		if err != nil {
			return nil, err
		}
		c.current = entry
	}
	return c.current, nil
}

func (c *decodedEntryCursor) Advance() {
	if c.closed || c.current == nil {
		return
	}
	index := c.current.GetIndex()
	c.current = nil
	c.exhausted = index == ^uint64(0)
	if !c.exhausted {
		c.target = index + 1
	}
}

func (c *decodedEntryCursor) Close() error {
	if c.closed {
		return nil
	}
	c.closed, c.current = true, nil
	return c.close()
}

// invalidate preserves the explicit seek target or next unconsumed index.
// The underlying raw cursor invalidates its own physical hints on publication.
func (c *decodedEntryCursor) invalidate() { c.current = nil }

func newEventEntryCursor(raw eventlog.Cursor, index uint64) *decodedEntryCursor {
	return &decodedEntryCursor{
		target: index,
		seek: func(id uint64) (*pb.Entry, error) {
			record, err := raw.Seek(id)
			return decodeEventEntry(record, err)
		},
		close: raw.Close,
	}
}
