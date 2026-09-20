package segmented

import (
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/pkg/segmentlog"
)

type cursor struct{ raw *segmentlog.Cursor }

func (l *Log) NewCursor() eventlog.Cursor                 { return &cursor{raw: l.log.NewCursor()} }
func (c *cursor) Seek(id uint64) (eventlog.Record, error) { return converted(c.raw.Seek(id)) }
func (c *cursor) Close() error                            { return translate(c.raw.Close()) }
