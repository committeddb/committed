package segmentlog

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRewritePreparationAllowsReads(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, pauseAt := range []uint64{1, 20} {
			for _, operation := range []string{"append", "rewrite", "reclaim", "orphans", "close"} {
				t.Run(fmt.Sprintf("cache=%t/pause=%d/%s", cached, pauseAt, operation), func(t *testing.T) {
					var l *Log
					if cached {
						l = cachedLog(t, 42)
					} else {
						l = newLog(t, 42)
					}
					if err := l.Append([]Record{{1, []byte("value")}, {10, []byte("value")}, {20, []byte("value")}}); err != nil {
						t.Fatal(err)
					}
					cursor := l.NewCursor()
					defer func() { _ = cursor.Close() }()
					if _, err := cursor.Seek(1); err != nil {
						t.Fatal(err)
					}
					entered, release := make(chan struct{}), make(chan struct{})
					unblock := sync.OnceFunc(func() { close(release) })
					defer unblock()
					done := make(chan error, 1)
					go func() {
						_, err := l.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) {
							r.Payload[0] = 'X'
							if r.ID == pauseAt {
								close(entered)
								<-release
							}
							return r.Payload, true, nil
						})
						done <- err
					}()
					select {
					case <-entered:
					case <-time.After(10 * time.Second):
						t.Fatal("rewrite did not pause")
					}
					if l.mutationMu.TryLock() {
						l.mutationMu.Unlock()
						t.Fatal("rewrite lost mutation ownership")
					}
					reads := make(chan error, 1)
					go func() {
						for _, id := range []uint64{1, 10, 20} {
							r, err := cursor.Seek(id)
							if err != nil {
								reads <- err
								return
							}
							if r.ID != id || string(r.Payload) != "value" {
								reads <- errors.New("unpublished bytes visible")
								return
							}
						}
						count := 0
						err := l.Scan(t.Context(), Coverage{0, 21}, func(r Record) error {
							count++
							if string(r.Payload) != "value" {
								return errors.New("mixed generation")
							}
							r.Payload[0] = 'Y'
							return nil
						})
						if err == nil && count != 3 {
							err = errors.New("incomplete scan")
						}
						reads <- err
					}()
					if err := awaitOperation(t, reads); err != nil {
						t.Fatal(err)
					}
					started, mutation := make(chan struct{}), make(chan error, 1)
					go func() {
						close(started)
						var err error
						switch operation {
						case "append":
							err = l.Append([]Record{{30, []byte("later")}})
						case "rewrite":
							_, err = l.Rewrite(context.Background(), 2, func(r Record) ([]byte, bool, error) { return r.Payload, true, nil })
						case "reclaim":
							_, err = l.Reclaim(context.Background())
						case "orphans":
							_, err = l.ReclaimOrphans(context.Background())
						case "close":
							err = l.Close()
						}
						mutation <- err
					}()
					<-started
					select {
					case err := <-mutation:
						t.Fatalf("mutation escaped preparation: %v", err)
					case <-time.After(10 * time.Millisecond):
					}
					unblock()
					if err := awaitOperation(t, done); err != nil {
						t.Fatal(err)
					}
					if err := awaitOperation(t, mutation); err != nil {
						t.Fatal(err)
					}
					if operation == "close" {
						if _, err := cursor.Seek(1); !errors.Is(err, ErrClosed) {
							t.Fatal(err)
						}
					} else {
						for _, id := range []uint64{1, 10, 20} {
							r, err := cursor.Seek(id)
							if err != nil || string(r.Payload) != "Xalue" {
								t.Fatal(r, err)
							}
						}
					}
				})
			}
		}
	}
}
