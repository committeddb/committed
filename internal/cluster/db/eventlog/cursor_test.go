package eventlog_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestEventLogCursor(t *testing.T) {
	for _, backend := range backends() {
		t.Run(backend.name, func(t *testing.T) {
			log, err := backend.create(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Close() })
			cursor, other := log.NewCursor(), log.NewCursor()
			defer func() { _ = cursor.Close(); _ = other.Close() }()
			check := func(c eventlog.Cursor, request, want uint64, first byte) {
				t.Helper()
				r, err := c.Seek(request)
				if err != nil || r.ID != want || len(r.Payload) != 100 || r.Payload[0] != first {
					t.Fatal(request, r.ID, err)
				}
				r.Payload[0] = 'X'
			}
			if _, err := cursor.Seek(0); !errors.Is(err, eventlog.ErrNotFound) {
				t.Fatal(err)
			}
			if err := log.Append([]eventlog.Record{{ID: 1, Payload: bytes.Repeat([]byte("a"), 100)}, {ID: 10, Payload: bytes.Repeat([]byte("b"), 100)}, {ID: 20, Payload: bytes.Repeat([]byte("c"), 100)}}); err != nil {
				t.Fatal(err)
			}
			check(cursor, 0, 1, 'a')
			check(cursor, 2, 10, 'b')
			check(cursor, 2, 10, 'b') // retries do not commit caller progress
			check(other, 0, 1, 'a')
			check(cursor, 0, 1, 'a') // backwards seek
			check(cursor, 11, 20, 'c')
			if _, err := cursor.Seek(21); !errors.Is(err, eventlog.ErrNotFound) {
				t.Fatal(err)
			}
			if err := log.Append([]eventlog.Record{{ID: 30, Payload: bytes.Repeat([]byte("d"), 100)}}); err != nil {
				t.Fatal(err)
			}
			check(cursor, 21, 30, 'd') // EOF resumes after append/rollover
			check(cursor, 2, 10, 'b')
			_, err = log.Rewrite(t.Context(), 1, func(r eventlog.Record) ([]byte, bool, error) {
				if r.ID >= 20 {
					r.Payload[0] = 'z'
				}
				return r.Payload, r.ID != 10, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := log.Reclaim(t.Context()); err != nil {
				t.Fatal(err)
			}
			check(cursor, 2, 20, 'z') // must discard the old generation, including on retry
			check(cursor, 21, 30, 'z')
			check(other, 0, 1, 'a')
			if _, err := cursor.Seek(^uint64(0)); !errors.Is(err, eventlog.ErrNotFound) {
				t.Fatal(err)
			}
			if err := cursor.Close(); err != nil {
				t.Fatal(err)
			}
			if err := cursor.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := cursor.Seek(0); !errors.Is(err, eventlog.ErrClosed) {
				t.Fatal(err)
			}
			if err := log.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := other.Seek(0); !errors.Is(err, eventlog.ErrClosed) {
				t.Fatal(err)
			}
		})
	}
}
