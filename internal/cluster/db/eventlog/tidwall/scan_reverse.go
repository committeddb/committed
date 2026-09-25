package tidwall

import (
	"context"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func (l *Log) ScanReverse(ctx context.Context, limit int, visit func(eventlog.Record) (bool, error)) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return 0, err
	}
	if ctx == nil || limit < 0 || visit == nil {
		return 0, eventlog.ErrInvalid
	}
	if err := ctx.Err(); err != nil || limit == 0 {
		return 0, err
	}
	first, err := l.data.FirstIndex()
	if err != nil {
		return 0, mapError(err)
	}
	last, err := l.data.LastIndex()
	if err != nil {
		return 0, mapError(err)
	}
	count := 0
	for seq := last; first != 0 && seq >= first; seq-- {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		r, err := l.at(seq)
		if err != nil {
			return count, err
		}
		count++
		more, err := visit(r)
		if err != nil {
			return count, err
		}
		if err := ctx.Err(); err != nil {
			return count, err
		}
		if !more || count == limit || seq == first {
			break
		}
	}
	return count, ctx.Err()
}
