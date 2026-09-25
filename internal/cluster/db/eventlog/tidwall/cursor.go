package tidwall

import "github.com/committeddb/committed/internal/cluster/db/eventlog"

// cursor keeps a dense sequence only within its current selected directory.
// The caller still supplies logical IDs, including retries and backwards seeks.
type cursor struct {
	log                        *Log
	directory                  string
	sequence, requested, found uint64
	valid, closed              bool
}

func (l *Log) NewCursor() eventlog.Cursor { return &cursor{log: l} }
func (c *cursor) Seek(id uint64) (eventlog.Record, error) {
	l := c.log
	l.mu.Lock()
	defer l.mu.Unlock()
	if c.closed {
		return eventlog.Record{}, eventlog.ErrClosed
	}
	if err := l.usable(); err != nil {
		return eventlog.Record{}, err
	}
	seq := c.sequence
	direct := false
	if c.valid && c.directory == l.state.Directory {
		if id >= c.requested && id <= c.found {
			direct = true
		}
		if c.found != ^uint64(0) && id == c.found+1 {
			seq++
			direct = true
		}
	}
	var r eventlog.Record
	var err error
	if direct {
		var last uint64
		last, err = l.data.LastIndex()
		if err == nil {
			if seq > last {
				return r, eventlog.ErrNotFound
			}
			r, err = l.at(seq)
		}
	} else {
		seq, r, err = l.seek(id)
	}
	if err != nil {
		c.valid = false
		return r, err
	}
	c.sequence, c.requested, c.found, c.directory = seq, id, r.ID, l.state.Directory
	c.valid = true
	return r, nil
}

func (c *cursor) Close() error {
	c.log.mu.Lock()
	defer c.log.mu.Unlock()
	c.closed = true
	c.valid = false
	return nil
}
