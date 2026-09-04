package db

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The registry's model-based stress test. The lifecycle tests elsewhere in
// this package each pin one race (a supervisor resurrection, a concurrent
// replace, a Close vs. late install) between two actors; none drives the
// full mix — listener-style replace and remove, supervisor-style swap,
// HTTP-style keep-on-wedge stop, and closeAll — across goroutines at once.
// This test does, over fake workers that exit on cancel or wedge, and
// asserts the registry's invariants directly rather than any one scenario:
//
//   - no orphans: every handle ever spawned is cancelled by the time
//     closeAll has returned (it was either registered, so closeAll cancelled
//     it, or evicted, so the eviction did);
//   - a condemned handle is never swapped on: once an eviction has been
//     observed for a handle, swapIfCurrent on it must refuse;
//   - resources are released at most once per handle and only after its
//     drain completed (never on a wedged worker);
//   - nothing survives closeAll: both tables empty, every later install
//     refused with ErrClosed, every later swap refused;
//   - lookups never block behind a drain (its own deterministic test below).
//
// Run under -race; the interleavings are what it exercises.

// stressModel is the test's view of every handle the registry has been
// handed, tracked from outside the lock: the registry's own fields
// (condemned, the tables) are never read directly, only observed through
// what the operations return and the callbacks they invoke.
type stressModel struct {
	t       *testing.T
	release chan struct{} // closed at cleanup so wedged goroutines exit

	mu         sync.Mutex
	spawned    []*workerHandle
	kindOf     map[*workerHandle]workerKind
	idOf       map[*workerHandle]string
	wedged     map[*workerHandle]bool
	evicted    map[*workerHandle]bool // an eviction was observed (condemn precedes it)
	closes     map[*workerHandle]int
	violations []string
}

func newStressModel(t *testing.T) *stressModel {
	m := &stressModel{
		t: t, release: make(chan struct{}),
		kindOf: map[*workerHandle]workerKind{}, idOf: map[*workerHandle]string{},
		wedged: map[*workerHandle]bool{}, evicted: map[*workerHandle]bool{}, closes: map[*workerHandle]int{},
	}
	t.Cleanup(func() { close(m.release) })
	return m
}

func (m *stressModel) violate(format string, args ...any) {
	m.mu.Lock()
	m.violations = append(m.violations, fmt.Sprintf(format, args...))
	m.mu.Unlock()
}

// workerMode is how a fake worker behaves.
type workerMode int

const (
	exitsOnCancel workerMode = iota // closes done once its ctx is cancelled — the normal worker
	wedged                          // closes done only when the test releases it: past every drain timeout
	frozen                          // exited on its own with ctx still live — a frozen ingest worker, the supervisor's input
)

func randomMode(r *rand.Rand) workerMode {
	switch r.IntN(6) {
	case 0:
		return wedged
	case 1:
		return frozen
	default:
		return exitsOnCancel
	}
}

// spawn builds a fake worker in the given mode.
func (m *stressModel) spawn(kind workerKind, id string, mode workerMode) *workerHandle {
	ctx, cancel := context.WithCancel(context.Background())
	h := &workerHandle{cancel: cancel, ctx: ctx, done: make(chan struct{})}
	m.mu.Lock()
	m.spawned = append(m.spawned, h)
	m.kindOf[h], m.idOf[h], m.wedged[h] = kind, id, mode == wedged
	m.mu.Unlock()
	if mode == frozen {
		close(h.done)
		return h
	}
	go func() {
		<-ctx.Done()
		if mode == wedged {
			<-m.release
		}
		close(h.done)
	}()
	return h
}

func doneClosed(h *workerHandle) bool {
	select {
	case <-h.done:
		return true
	default:
		return false
	}
}

// onEvicted is what the callers' onDrained / post-remove cleanup does:
// record the eviction, and release resources only if the drain completed.
func (m *stressModel) onEvicted(h *workerHandle, drained bool) {
	m.mu.Lock()
	m.evicted[h] = true
	m.mu.Unlock()
	if !drained {
		return
	}
	select {
	case <-h.done:
	default:
		m.violate("drained reported for a worker whose done is still open")
	}
	h.closeResources.Do(func() {
		m.mu.Lock()
		m.closes[h]++
		m.mu.Unlock()
	})
}

func (m *stressModel) wasEvicted(h *workerHandle) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.evicted[h]
}

func (m *stressModel) randomSpawned(r *rand.Rand) *workerHandle {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.spawned) == 0 {
		return nil
	}
	return m.spawned[r.IntN(len(m.spawned))]
}

func TestWorkerRegistry_StressInvariants(t *testing.T) {
	const (
		goroutines = 8
		iterations = 150
		drain      = 3 * time.Millisecond
	)
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed=%d", seed)

	m := newStressModel(t)
	reg := newWorkerRegistry(drain)
	kinds := []workerKind{workerKindSync, workerKindIngest}
	ids := []string{"a", "b", "c"}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, uint64(g)))
			for i := 0; i < iterations; i++ {
				kind, id := kinds[r.IntN(len(kinds))], ids[r.IntN(len(ids))]
				switch r.IntN(6) {
				case 0, 1: // listener replace (the common case)
					err := reg.replace(kind, id,
						func(bool) *workerHandle { return m.spawn(kind, id, randomMode(r)) },
						func(prev *workerHandle, drained bool) { m.onEvicted(prev, drained) })
					if err != nil && !errors.Is(err, ErrClosed) {
						m.violate("replace: unexpected error %v", err)
					}
				case 2: // delete / reconcile-cancel
					if h, drained := reg.remove(kind, id, abandonOnWedge, nil); h != nil {
						m.onEvicted(h, drained)
					}
				case 3: // rebuild stop: a wedged worker stays registered
					if h, drained := reg.remove(kind, id, keepOnWedge, nil); h != nil {
						m.onEvicted(h, drained)
					}
				case 4: // supervisor restart on some handle it once held
					h := m.randomSpawned(r)
					if h == nil || !doneClosed(h) {
						continue // the supervisor only ever holds an exited worker
					}
					m.mu.Lock()
					hk, hid := m.kindOf[h], m.idOf[h]
					m.mu.Unlock()
					evictedBefore := m.wasEvicted(h)
					swapped := reg.swapIfCurrent(hk, hid, h, func() *workerHandle { return m.spawn(hk, hid, randomMode(r)) })
					if swapped && evictedBefore {
						m.violate("swapIfCurrent succeeded on a handle already observed evicted (condemned)")
					}
					if swapped {
						m.onEvicted(h, true) // swap drops it without a drain: done was closed (checked above)
					}
				case 5: // status reads
					_ = reg.lookup(kind, id)
					for _, got := range reg.ids(kind) {
						if got != "a" && got != "b" && got != "c" {
							m.violate("ids returned an unknown id %q", got)
						}
					}
				}
			}
		}(g)
	}

	// Shutdown lands somewhere inside the churn.
	time.Sleep(time.Duration(20+rand.New(rand.NewPCG(seed, 99)).IntN(100)) * time.Millisecond)
	handles, abandoned := reg.closeAll(500 * time.Millisecond)
	wg.Wait()

	// Nothing survives closeAll.
	require.Empty(t, reg.ids(workerKindSync))
	require.Empty(t, reg.ids(workerKindIngest))
	err := reg.replace(workerKindSync, "a", func(bool) *workerHandle { t.Fatal("spawn after close"); return nil }, nil)
	require.ErrorIs(t, err, ErrClosed)
	if h := m.randomSpawned(rand.New(rand.NewPCG(seed, 7))); h != nil {
		require.False(t, reg.swapIfCurrent(m.kindOf[h], m.idOf[h], h, func() *workerHandle { t.Fatal("spawn after close"); return nil }))
	}
	for _, h := range handles {
		m.onEvicted(h, doneClosed(h)) // the caller's post-close release checks done per handle
	}

	// No orphans: every handle ever spawned was cancelled by someone.
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, h := range m.spawned {
		if h.ctx.Err() == nil {
			m.violations = append(m.violations, "orphan: a spawned worker was never cancelled (running but unreferenced)")
		}
		if m.closes[h] > 1 {
			m.violations = append(m.violations, "resources released more than once for one handle")
		}
	}
	wedgedAtClose := 0
	for _, h := range handles {
		if m.wedged[h] {
			wedgedAtClose++
		}
	}
	require.LessOrEqual(t, abandoned, wedgedAtClose, "closeAll abandoned a worker that exits on cancel")
	require.Empty(t, m.violations)
	t.Logf("spawned=%d registered-at-close=%d abandoned=%d", len(m.spawned), len(handles), abandoned)
}

// TestWorkerRegistry_LookupNeverBlocksDuringDrain pins the one hazard no
// lifecycle test covers: the drain in remove/replace must run OUTSIDE the
// lock, so the status paths (lookup, ids) answer instantly while a wedged
// worker is being waited on — draining under the lock would not deadlock
// (workers never take it) but would stall every status read and Close for
// up to the drain timeout. Deterministic, not timing-based: the worker
// signals when remove has cancelled it (which remove does under the lock,
// so the signal proves remove is at or past its unlock, inside the drain),
// and the drain cannot end until the test releases the worker.
func TestWorkerRegistry_LookupNeverBlocksDuringDrain(t *testing.T) {
	reg := newWorkerRegistry(10 * time.Second) // never reached: the test releases the worker
	ctx, cancel := context.WithCancel(context.Background())
	cancelled, release := make(chan struct{}), make(chan struct{})
	h := &workerHandle{cancel: cancel, ctx: ctx, done: make(chan struct{})}
	go func() {
		<-ctx.Done()
		close(cancelled)
		<-release
		close(h.done)
	}()
	reg.put(workerKindSync, "s", h)

	removing := make(chan struct{})
	go func() {
		defer close(removing)
		reg.remove(workerKindSync, "s", abandonOnWedge, nil)
	}()
	<-cancelled // remove is inside its drain from here until we release the worker

	start := time.Now()
	got := reg.lookup(workerKindSync, "s")
	ids := reg.ids(workerKindSync)
	elapsed := time.Since(start)

	require.Less(t, elapsed, 100*time.Millisecond, "lookup/ids blocked behind the drain")
	require.Same(t, h, got, "the draining worker is still registered until the relock")
	require.Equal(t, []string{"s"}, ids)

	close(release)
	<-removing
	require.Nil(t, reg.lookup(workerKindSync, "s"), "abandonOnWedge deregisters after the drain")
}
