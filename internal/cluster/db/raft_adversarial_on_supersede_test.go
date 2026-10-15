//go:build adversarial

package db_test

import (
	"testing"
	"time"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// TestAdversarial_OnSupersedeScrubIsDeterministic: a scrub over a type declared
// onSupersede = "scrub" (switched from keep partway through the log) removes
// the same superseded versions on every replica, and the rewritten logs stay
// byte-identical.
func TestAdversarial_OnSupersedeScrubIsDeterministic(t *testing.T) {
	forLifecycleBackends(t, testOnSupersedeScrubIsDeterministic)
}

func testOnSupersedeScrubIsDeterministic(t *testing.T, storageOpts []wal.Option) {
	rafts, fc, dirs, _ := newSevereLagCluster(t, 3, catchUpNodeOpts(), storageOpts...)
	alive := []bool{true, true, true}
	defer func() {
		for i, r := range rafts {
			if alive[i] {
				_ = r.Close()
			}
		}
		for i, r := range rafts {
			if alive[i] {
				_ = r.storage.Close()
			}
		}
	}()
	rafts.WaitForLeader(t)

	propose := func(entities ...*cluster.Entity) {
		bs, err := (&cluster.Proposal{Entities: entities}).Marshal()
		if err != nil {
			t.Fatal(err)
		}
		proposeAndCheckBytes(t, rafts, bs)
		for _, r := range rafts {
			waitForUserEntry(t, r, bs)
		}
	}
	register := func(on cluster.OnSupersede) {
		e, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "events", Name: "events", Version: 1, EntityKind: cluster.EntityKindEvent, OnSupersede: on})
		if err != nil {
			t.Fatal(err)
		}
		propose(e)
	}
	event := func(key, data string) *cluster.Entity {
		return &cluster.Entity{Type: &cluster.Type{ID: "events"}, Key: []byte(key), Data: []byte(data)}
	}

	register(cluster.OnSupersedeKeep)
	propose(event("1", `{"customer":"ada"}`))
	propose(event("2", `{"qty":2}`))
	propose(event("1", `{"customer":"***"}`)) // a mask, before the switch
	register(cluster.OnSupersedeScrub)
	propose(event("3", `{"qty":1}`), event("2", `{"qty":5}`)) // a correction, after it
	var seq uint64
	proposeBurst(t, rafts, &seq, 5)
	waitForSurvivorConvergence(t, rafts, 10*time.Second)

	// Indices of the two superseded originals, read from one node's log.
	superseded := func(ws *wal.Storage) []uint64 {
		t.Helper()
		var out []uint64
		for i := uint64(1); i <= ws.EventIndex(); i++ {
			a, err := ws.ActualAt(i)
			if err != nil {
				continue
			}
			for _, e := range a.Entities {
				if e.Type.ID == "events" && (string(e.Data) == `{"customer":"ada"}` || string(e.Data) == `{"qty":2}`) {
					out = append(out, i)
				}
			}
		}
		return out
	}
	gone := superseded(rafts[0].storage.(*wal.Storage))
	if len(gone) != 2 {
		t.Fatalf("expected the two superseded originals in the log before the scrub, found %v", gone)
	}

	bound := rafts.LeaderRaft().storage.AppliedIndex()
	scrub, err := cluster.NewScrubEntity(bound, false)
	if err != nil {
		t.Fatal(err)
	}
	propose(scrub)
	for _, r := range rafts {
		ws := r.storage.(*wal.Storage)
		deadline := time.Now().Add(20 * time.Second)
		for {
			if _, c := ws.ScrubProgress(); c >= bound {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d never completed the scrub to %d", r.id, bound)
			}
			time.Sleep(20 * time.Millisecond)
		}
		if left := superseded(ws); len(left) != 0 {
			t.Fatalf("node %d still holds superseded versions at %v after the scrub", r.id, left)
		}
	}

	fc.Partition([]uint64{1}, []uint64{2, 3})
	fc.Partition([]uint64{2}, []uint64{3})
	for i, r := range rafts {
		if alive[i] {
			_ = r.Close()
		}
	}
	for i, r := range rafts {
		if alive[i] {
			_ = r.storage.Close()
			alive[i] = false
		}
	}
	assertEventLogPrefixMatches(t, rafts, dirs)
}
