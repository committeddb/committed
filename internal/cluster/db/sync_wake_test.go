package db_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// arrivalSyncable records when each user entry reaches it.
type arrivalSyncable struct {
	mu      sync.Mutex
	arrived map[string]time.Time
}

func (s *arrivalSyncable) Sync(_ context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range a.Entities {
		if !cluster.IsInternal(e.Type.ID) {
			s.arrived[string(e.Key)] = time.Now()
		}
	}
	return false, nil
}

func (s *arrivalSyncable) Close() error { return nil }

func (s *arrivalSyncable) at(key string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.arrived[key]
	return t, ok
}

// TestSyncWake_IdleWorkerWakesOnApply: an entry appended to a quiet node
// reaches a caught-up, idle sync worker as soon as it applies, not after its
// poll backoff (up to 500 ms). Five appends, each after the worker has idled
// to the top of its backoff; polling alone would land all five within 150 ms
// of the apply only rarely. Timed from the apply, so a slow commit on a
// loaded machine does not count against the wake.
func TestSyncWake_IdleWorkerWakesOnApply(t *testing.T) {
	d, _ := newWalDB(t)
	proposeTypeTOML(t, d, "evt", "evt", "", "")
	const id = "wake"
	seedSyncableConfig(t, d, id)
	s := &arrivalSyncable{arrived: map[string]time.Time{}}
	require.NoError(t, d.Sync(context.Background(), id, s))

	for i := range 5 {
		time.Sleep(1200 * time.Millisecond) // the worker idles to its max backoff
		key := string(rune('a' + i))
		proposeUserUpsert(t, d, "evt", key, `{}`)
		applied := time.Now() // Propose returns once the entry applied
		require.Eventually(t, func() bool { _, ok := s.at(key); return ok }, 5*time.Second, time.Millisecond)
		arrived, _ := s.at(key)
		require.Less(t, arrived.Sub(applied), 150*time.Millisecond,
			"append %d reached the idle worker %s after it applied", i, arrived.Sub(applied))
	}
}

// failingSyncable fails every user entry with a transient error.
type failingSyncable struct{ calls atomic.Int64 }

func (s *failingSyncable) Sync(_ context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	if allSystem(a) {
		return false, nil
	}
	s.calls.Add(1)
	return false, errors.New("destination unavailable")
}

func (s *failingSyncable) Close() error { return nil }

// TestSyncWake_FailingWorkerKeepsItsBackoff: a worker retrying a failed sync
// is not caught up, so applies elsewhere in the log must not wake it — its
// retries stay at the backoff rate, never the apply rate.
func TestSyncWake_FailingWorkerKeepsItsBackoff(t *testing.T) {
	d, _ := newWalDB(t)
	proposeTypeTOML(t, d, "evt", "evt", "", "")
	proposeTypeTOML(t, d, "other", "other", "", "")
	const id = "failing"
	seedSyncableConfig(t, d, id)
	s := &failingSyncable{}
	require.NoError(t, d.Sync(context.Background(), id, s))
	proposeUserUpsert(t, d, "evt", "poison", `{}`)
	require.Eventually(t, func() bool { return s.calls.Load() > 0 }, 5*time.Second, time.Millisecond)

	// Keep entries applying for 3 s.
	stop := time.Now().Add(3 * time.Second)
	before := s.calls.Load()
	for i := 0; time.Now().Before(stop); i++ {
		proposeUserUpsert(t, d, "other", string(rune('A'+i%26)), `{}`)
	}
	retries := s.calls.Load() - before
	require.Less(t, retries, int64(20),
		"a failing worker retried %d times in 3 s of applies: it is being woken at the apply rate", retries)
}
