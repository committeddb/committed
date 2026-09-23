package eventlog_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// historyModel contains only logical survivors and original append progress.
// It deliberately has no segments, catalogs, physical sequences, or tail state.
type historyModel struct {
	records    []eventlog.Record
	head       uint64
	has        bool
	generation uint64
}

func modelPayload(rng *rand.Rand) []byte {
	sizes := [...]int{0, 1, 31, 160, 512}
	data := make([]byte, sizes[rng.IntN(len(sizes))])
	for i := range data {
		data[i] = byte(rng.Uint32())
	}
	return data
}

func checkHistory(t *testing.T, log eventlog.EventLog, m historyModel, rng *rand.Rand) {
	t.Helper()
	generation, err := log.Generation()
	if err != nil || generation != m.generation {
		t.Fatalf("generation: got %d, want %d: %v", generation, m.generation, err)
	}
	head, has, err := log.LastAppended()
	if err != nil || head != m.head || has != m.has {
		t.Fatalf("append progress: got %d/%t, want %d/%t: %v", head, has, m.head, m.has, err)
	}
	var got []eventlog.Record
	if err := log.Scan(t.Context(), eventlog.Coverage{End: ^uint64(0)}, func(r eventlog.Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(m.records) {
		t.Fatalf("survivor count: got %d, want %d", len(got), len(m.records))
	}
	for i, want := range m.records {
		if got[i].ID != want.ID || !bytes.Equal(got[i].Payload, want.Payload) {
			t.Fatalf("scan differs at record %d, id %d", i, want.ID)
		}
		exact, err := log.Read(want.ID)
		if err != nil || exact.ID != want.ID || !bytes.Equal(exact.Payload, want.Payload) {
			t.Fatalf("exact lookup differs at %d: %v", want.ID, err)
		}
	}
	// Include gaps, erased/frontier IDs, zero, and the exclusive maximum bound.
	probes := []uint64{0, m.head, ^uint64(0), uint64(rng.IntN(int(m.head + 2)))}
	if m.has {
		probes = append(probes, m.head+1)
	}
	for _, id := range probes {
		next := 0
		for next < len(m.records) && m.records[next].ID < id {
			next++
		}
		record, err := log.Seek(id)
		if next == len(m.records) {
			if !errors.Is(err, eventlog.ErrNotFound) {
				t.Fatalf("seek %d: expected absence, got %v", id, err)
			}
		} else if err != nil || record.ID != m.records[next].ID || !bytes.Equal(record.Payload, m.records[next].Payload) {
			t.Fatalf("seek %d differs: %v", id, err)
		}
		record, err = log.Read(id)
		if next == len(m.records) || m.records[next].ID != id {
			if !errors.Is(err, eventlog.ErrNotFound) {
				t.Fatalf("read %d: expected absence, got %v", id, err)
			}
		} else if err != nil || record.ID != id || !bytes.Equal(record.Payload, m.records[next].Payload) {
			t.Fatalf("read %d differs: %v", id, err)
		}
	}
	start, end := uint64(rng.IntN(int(m.head+2))), uint64(rng.IntN(int(m.head+2)))
	if start > end {
		start, end = end, start
	}
	first, last := 0, 0
	for first < len(m.records) && m.records[first].ID < start {
		first++
	}
	last = first
	for last < len(m.records) && m.records[last].ID < end {
		last++
	}
	expected := m.records[first:last]
	scanned := 0
	if err := log.Scan(t.Context(), eventlog.Coverage{Start: start, End: end}, func(r eventlog.Record) error {
		if scanned >= len(expected) || r.ID != expected[scanned].ID || !bytes.Equal(r.Payload, expected[scanned].Payload) {
			return fmt.Errorf("scan differs in [%d,%d) at record %d", start, end, scanned)
		}
		scanned++
		return nil
	}); err != nil || scanned != len(expected) {
		t.Fatalf("bounded scan count %d, want %d: %v", scanned, len(expected), err)
	}
}

func appendHistory(t *testing.T, log eventlog.EventLog, m *historyModel, rng *rand.Rand) {
	t.Helper()
	records := make([]eventlog.Record, 1+rng.IntN(4))
	head, has := m.head, m.has
	for i := range records {
		if has {
			head += uint64(1 + rng.IntN(17))
		}
		records[i] = eventlog.Record{ID: head, Payload: modelPayload(rng)}
		has = true
	}
	if err := log.Append(records); err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		m.records = append(m.records, eventlog.Record{ID: r.ID, Payload: bytes.Clone(r.Payload)})
	}
	m.head, m.has = head, has
	// A bad suffix must reject the whole batch, including a valid new prefix.
	if err := log.Append([]eventlog.Record{{ID: head + 1}, {ID: head}}); !errors.Is(err, eventlog.ErrInvalid) {
		t.Fatalf("accepted invalid batch: %v", err)
	}
}

func rewriteHistory(t *testing.T, log eventlog.EventLog, m *historyModel, rng *rand.Rand, mode string) {
	t.Helper()
	next := make([]eventlog.Record, 0, len(m.records))
	replacements := make(map[uint64][]byte, len(m.records))
	changed := uint64(0)
	for _, r := range m.records {
		action := rng.IntN(4)
		if mode == "noop" {
			action = 3
		}
		if mode == "erase-all" || (mode == "erase-head" && r.ID == m.head) {
			action = 0
		}
		if action == 0 {
			changed++
			continue
		}
		payload := bytes.Clone(r.Payload)
		if action == 1 {
			payload = modelPayload(rng)
		}
		if action == 2 && len(payload) > 0 {
			payload[0] ^= 0xff
		}
		if !bytes.Equal(payload, r.Payload) {
			changed++
		}
		next = append(next, eventlog.Record{ID: r.ID, Payload: payload})
		replacements[r.ID] = payload
	}
	calls := make(map[uint64]int, len(m.records))
	result, err := log.Rewrite(t.Context(), m.generation+1, func(r eventlog.Record) ([]byte, bool, error) {
		calls[r.ID]++
		payload, keep := replacements[r.ID]
		// Exercise in-place callbacks as well as separately allocated replacements.
		if keep && len(payload) == len(r.Payload) {
			copy(r.Payload, payload)
			return r.Payload, true, nil
		}
		return bytes.Clone(payload), keep, nil
	})
	if err != nil || !result.Published || result.ChangedRecords != changed {
		t.Fatalf("rewrite %s: %+v, want %d changed: %v", mode, result, changed, err)
	}
	if len(calls) != len(m.records) {
		t.Fatal("rewrite visited wrong record set")
	}
	for _, r := range m.records {
		if calls[r.ID] != 1 {
			t.Fatalf("id %d transformed %d times", r.ID, calls[r.ID])
		}
	}
	m.records = next
	m.generation++
}

func TestEventLogRepeatedHistory(t *testing.T) {
	for _, backend := range backends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, seed := range []uint64{7, 19, 83} {
				t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
					rng := rand.New(rand.NewPCG(seed, seed+1))
					path := t.TempDir()
					log, err := backend.create(path)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = log.Close() })
					m := historyModel{}
					reopen := func() {
						t.Helper()
						if err := log.Close(); err != nil {
							t.Fatal(err)
						}
						log, err = backend.open(path)
						if err != nil {
							t.Fatal(err)
						}
					}
					checkHistory(t, log, m, rng)
					for cycle := range 8 {
						t.Logf("cycle %d, head %d, generation %d", cycle, m.head, m.generation)
						appendHistory(t, log, &m, rng)
						appendHistory(t, log, &m, rng)
						checkHistory(t, log, m, rng)
						mode := "mixed"
						if cycle%3 == 1 {
							mode = "erase-head"
						}
						if cycle%3 == 2 {
							mode = "erase-all"
						}
						rewriteHistory(t, log, &m, rng, mode)
						checkHistory(t, log, m, rng)
						reopen()
						checkHistory(t, log, m, rng)
						// The erased frontier remains unavailable for reuse after recovery.
						if err := log.Append([]eventlog.Record{{ID: m.head}}); !errors.Is(err, eventlog.ErrInvalid) {
							t.Fatal("reused original frontier", err)
						}
						rewriteHistory(t, log, &m, rng, "noop")
						if _, err := log.Reclaim(t.Context()); err != nil {
							t.Fatal(err)
						}
						reopen()
						checkHistory(t, log, m, rng)
						if _, err := log.Rewrite(t.Context(), m.generation, func(r eventlog.Record) ([]byte, bool, error) { return r.Payload, true, nil }); !errors.Is(err, eventlog.ErrInvalid) {
							t.Fatal("accepted stale generation", err)
						}
						// Fail after a changed record has prepared output, then recover the old
						// complete history. A later successful rewrite reuses this generation.
						if len(m.records) > 1 {
							injected := errors.New("injected transform failure")
							calls := 0
							_, err := log.Rewrite(t.Context(), m.generation+1, func(r eventlog.Record) ([]byte, bool, error) {
								calls++
								if calls == 2 {
									return nil, false, injected
								}
								return append(bytes.Clone(r.Payload), 1), true, nil
							})
							if !errors.Is(err, injected) {
								t.Fatal(err)
							}
							reopen()
							checkHistory(t, log, m, rng)
							if _, err := log.Reclaim(t.Context()); err != nil {
								t.Fatal(err)
							}
							rewriteHistory(t, log, &m, rng, "noop")
							checkHistory(t, log, m, rng)
						}
					}
				})
			}
		})
	}
}
