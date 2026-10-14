package main

import (
	"context"
	"crypto/rand"
	"fmt"
	mrand "math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// event is one row of the recipe's events table.
type event struct {
	stream  string
	version int
	typ     string
	data    string

	// What the payload says, for the expected row.
	customer   string
	qty, cents int
}

// expected is the read-model row the recipe's projection should produce for a
// stream: its emits computed from what was appended.
type expected struct {
	customer string
	status   string
	items    int
	cents    int64
	version  int
}

func (e expected) total() string { return fmt.Sprintf("%d.%02d", e.cents/100, e.cents%100) }

// stream is a stream a writer owns: what it has appended and what is left.
type stream struct {
	id    string
	want  expected
	hot   bool
	items int    // short streams: item_added events still to append
	end   string // short streams: the closing event type, or "" to stay open
}

var (
	customers = []string{"ada", "grace", "barbara", "edsger", "frances", "radia", "leslie", "margaret"}
	skus      = []string{"pen", "ink", "pad", "clip", "tape", "glue"}
)

// writer appends events for the streams it owns, so each stream's versions
// are appended in order by one connection.
type writer struct {
	rnd       *mrand.Rand
	open      []*stream
	hot       []*stream
	hotTarget int
	hotCredit float64
	hotShare  float64
	done      []*stream
	next      int
}

const openShortStreams = 8

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// nextEvent returns the writer's next event without applying it: apply it
// once the source commits it.
func (w *writer) nextEvent() (*stream, event) {
	w.hotCredit += w.hotShare
	if w.hotCredit >= 1 {
		for range w.hot {
			s := w.hot[w.next%len(w.hot)]
			w.next++
			if s.want.version < w.hotTarget {
				w.hotCredit--
				return s, w.eventFor(s)
			}
		}
		w.hotCredit = 0
	}
	if len(w.open) < openShortStreams {
		s := &stream{id: newUUID(), items: 1 + w.rnd.IntN(8)}
		switch r := w.rnd.IntN(100); {
		case r < 70:
			s.end = "order_shipped"
		case r < 85:
			s.end = "order_cancelled"
		}
		w.open = append(w.open, s)
	}
	s := w.open[w.next%len(w.open)]
	w.next++
	return s, w.eventFor(s)
}

func (w *writer) eventFor(s *stream) event {
	e := event{stream: s.id, version: s.want.version + 1}
	switch {
	case s.want.version == 0:
		e.typ = "order_placed"
		e.customer = customers[w.rnd.IntN(len(customers))]
		e.data = fmt.Sprintf(`{"customer": %q}`, e.customer)
	case s.hot || s.items > 0:
		e.typ = "item_added"
		e.qty, e.cents = 1+w.rnd.IntN(5), 100+w.rnd.IntN(9900)
		e.data = fmt.Sprintf(`{"sku": %q, "qty": %d, "price": %d.%02d}`,
			skus[w.rnd.IntN(len(skus))], e.qty, e.cents/100, e.cents%100)
	default:
		e.typ = s.end
		e.data = `{}`
	}
	return e
}

// apply records a committed event in the stream's expected row.
func (w *writer) apply(s *stream, e event) {
	s.want.version = e.version
	switch e.typ {
	case "order_placed":
		s.want.customer = e.customer
		s.want.status = e.typ
	case "item_added":
		s.want.items += e.qty
		s.want.cents += int64(e.qty) * int64(e.cents)
		if !s.hot {
			s.items--
		}
	default:
		s.want.status = e.typ
	}
	if s.hot {
		return
	}
	finished := s.items == 0 && (s.end == "" || e.typ == s.end)
	if finished && e.typ != "order_placed" {
		for i, o := range w.open {
			if o == s {
				w.open = append(w.open[:i], w.open[i+1:]...)
				break
			}
		}
		w.done = append(w.done, s)
	}
}

// streams returns every stream the writer touched.
func (w *writer) streams() []*stream {
	out := append([]*stream{}, w.done...)
	out = append(out, w.open...)
	return append(out, w.hot...)
}

// workload is the running append phase.
type workload struct {
	o        options
	src      *source
	tracker  *tracker
	writers  []*writer
	appended atomic.Int64
	errors   atomic.Int64
	lastErr  atomic.Value
	perSec   []int64 // appends committed in each second of the run
	perSecMu sync.Mutex
}

func newWorkload(o options, src *source, tr *tracker) *workload {
	wl := &workload{o: o, src: src, tracker: tr}
	perWriter := o.rate * o.duration.Seconds() / float64(o.writers)
	for i := range o.writers {
		w := &writer{rnd: mrand.New(mrand.NewPCG(uint64(i)+1, 0x5eed)), hotTarget: o.hotEvents} //nolint:gosec // G404: workload shape, not security
		wl.writers = append(wl.writers, w)
	}
	for i := range o.hotStreams {
		w := wl.writers[i%len(wl.writers)]
		w.hot = append(w.hot, &stream{id: newUUID(), hot: true})
	}
	for _, w := range wl.writers {
		if len(w.hot) > 0 && perWriter > 0 {
			w.hotShare = min(1, float64(len(w.hot)*o.hotEvents)/perWriter)
		}
	}
	return wl
}

// run appends for the duration, each writer paced to rate/writers. A writer
// that falls behind catches up for at most a second's worth, then drops the
// backlog: the report gives the rate actually committed.
func (wl *workload) run(ctx context.Context) (start, lastCommit time.Time) {
	start = time.Now()
	end := start.Add(wl.o.duration)
	wl.perSec = make([]int64, int(wl.o.duration.Seconds())+2)
	interval := time.Duration(float64(time.Second) * float64(wl.o.writers) / wl.o.rate)
	var last atomic.Int64
	var wg sync.WaitGroup
	for i, w := range wl.writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			next := start.Add(interval * time.Duration(i) / time.Duration(len(wl.writers)))
			var s *stream
			var e event
			for {
				now := time.Now()
				if now.After(end) || ctx.Err() != nil {
					return
				}
				if next.After(now) {
					select {
					case <-ctx.Done():
						return
					case <-time.After(next.Sub(now)):
					}
				} else if now.Sub(next) > time.Second {
					next = now
				}
				next = next.Add(interval)
				if s == nil {
					s, e = w.nextEvent()
				}
				if err := wl.src.append(ctx, e); err != nil {
					wl.errors.Add(1)
					wl.lastErr.Store(err.Error())
					continue // retry the same event on the next tick
				}
				at := time.Now()
				w.apply(s, e)
				n := wl.appended.Add(1)
				last.Store(at.UnixNano())
				if n%int64(wl.o.sampleEvery) == 0 {
					wl.tracker.add(e.stream, e.version, at)
				}
				if sec := int(at.Sub(start).Seconds()); sec < len(wl.perSec) {
					wl.perSecMu.Lock()
					wl.perSec[sec]++
					wl.perSecMu.Unlock()
				}
				s = nil
			}
		}()
	}
	wg.Wait()
	return start, time.Unix(0, last.Load())
}

// expectedRows is every stream's expected read-model row.
func (wl *workload) expectedRows() map[string]expected {
	out := make(map[string]expected)
	for _, w := range wl.writers {
		for _, s := range w.streams() {
			if s.want.version > 0 {
				out[s.id] = s.want
			}
		}
	}
	return out
}

func (wl *workload) hotSizes() []int {
	var out []int
	for _, w := range wl.writers {
		for _, s := range w.hot {
			out = append(out, s.want.version)
		}
	}
	return out
}
