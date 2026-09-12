package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

// Catch-up is a property of the node, not an operator procedure. A node that
// joins or rejoins with an event log behind the snapshot raft wants to
// install — an empty data directory, or one that was away while the cluster
// compacted past it — fetches the events it is missing from a peer, by
// default, with nothing to configure, and only then lets the snapshot in.
// Ordinary replication takes over from there.
//
// The seam is the Ready loop, just before Save: an InstallSnapshot whose
// index is past this node's event index cannot be persisted (saveWithSnapshot
// refuses it — persisting raft state ahead of the event log would leave a
// permanent gap), so the loop fills the event log first, from a peer over the
// peer transport, then saves and installs as if the node had never been
// behind. The loop is stalled while it fetches, which is fine for a learner
// or a node that was already down; the node reports catchingUp on its status
// and not-ready on /ready for the duration.
//
// One rule keeps a fetched log sound under right-to-be-forgotten scrubs: a
// log is one GENERATION (the scrub bound its bytes reflect — see
// EventServer.EventLogGeneration), and a fetch never mixes two. Every stream
// is pinned to the generation this node's log already has; a peer at a newer
// generation (a scrub completed while this node was away) makes this node
// discard its log and fetch it whole, and a peer at an older one is skipped
// for another. What the node then holds is one node's log at one
// generation, so a pending scrub beyond it re-runs exactly as it would on
// any follower.

// EventReceiver is the storage surface a catch-up fills — wal.Storage in
// production. A storage without it (the in-memory test doubles) cannot catch
// up and keeps the fail-fast behaviour instead.
type EventReceiver interface {
	EventIndex() uint64
	EventLogGeneration() uint64
	// SnapshotScrubCompleted reads the completed scrub bound a snapshot's
	// payload carries — the least generation a log installing it may be
	// at (see EventFetchRequest.MinGeneration).
	SnapshotScrubCompleted(snap *raftpb.Snapshot) (uint64, error)
	// SetEventLogGeneration records, durably, the generation of the content
	// this node is about to adopt into an empty log, so a restart mid-fetch
	// resumes at that generation instead of starting over.
	SetEventLogGeneration(gen uint64) error
	// ResetEventLog discards this node's event log so a fetch starts from
	// the peer's first sequence.
	ResetEventLog() error
	// AppendFetchedRecords appends a run of records in the log's on-disk
	// encoding; records at or below this node's event index are skipped.
	AppendFetchedRecords(data []byte) error
	// AdoptEventSegments takes staged whole segment files into the log, in
	// order; the files are consumed.
	AdoptEventSegments(paths []string) error
	// EventFetchDir is the staging directory for segment files in flight.
	EventFetchDir() string
}

// CatchUpStatus is a node's progress through a catch-up: the raft index its
// event log has reached, the one the pending snapshot needs, when it began,
// and the peer that last served it.
type CatchUpStatus struct {
	Have, Need uint64
	Since      time.Time
	Peer       uint64
}

// snapshotNeedsCatchUp reports whether installing snap would leave this
// node's event log behind it — in length (events past the log's end) or in
// generation (content the snapshot's bbolt can no longer bring forward).
func (n *Raft) snapshotNeedsCatchUp(snap *raftpb.Snapshot) (needIndex, needGen uint64, needed bool) {
	needIndex = snap.Metadata.GetIndex()
	if needIndex > n.storage.EventIndex() {
		needed = true
	}
	recv, ok := n.storage.(EventReceiver)
	if !ok {
		return needIndex, 0, needed
	}
	needGen, err := recv.SnapshotScrubCompleted(snap)
	if err != nil {
		n.logger.Warn("could not read the snapshot's completed scrub bound; assuming none", zap.Error(err))
		return needIndex, 0, needed
	}
	if needGen > recv.EventLogGeneration() {
		needed = true
	}
	return needIndex, needGen, needed
}

// catchUpProgress is the node-local, ephemeral record the status surface
// reads while a catch-up runs.
type catchUpProgress struct {
	mu     sync.Mutex
	active bool
	runs   int
	st     CatchUpStatus
}

func (p *catchUpProgress) begin(have, need uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active = true
	p.runs++
	p.st = CatchUpStatus{Have: have, Need: need, Since: time.Now()}
}

func (p *catchUpProgress) update(have, peer uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.st.Have = have
	if peer != 0 {
		p.st.Peer = peer
	}
}

func (p *catchUpProgress) end() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active = false
}

func (p *catchUpProgress) status() (CatchUpStatus, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.st, p.active
}

// CatchUp reports the catch-up in progress on this node, if any.
func (n *Raft) CatchUp() (CatchUpStatus, bool) { return n.catchUp.status() }

// CatchUp reports the catch-up in progress on this node, if any: its event
// log is being filled from a peer before a snapshot installs. Served on
// GET /node/status; /ready is not-ready for the duration.
func (db *DB) CatchUp() (CatchUpStatus, bool) {
	if db.raft == nil {
		return CatchUpStatus{}, false
	}
	return db.raft.CatchUp()
}

// CatchingUp is CatchUp as the /ready probe reads it.
func (db *DB) CatchingUp() bool {
	_, active := db.CatchUp()
	return active
}

const (
	catchUpInitialBackoff = 250 * time.Millisecond
	catchUpMaxBackoff     = 10 * time.Second
	// catchUpWarnEvery paces the "still waiting" log line while no peer
	// serves: loud enough to be noticed, not a flood.
	catchUpWarnEvery = 30 * time.Second
)

// catchUpEventLog fills this node's event log up to the snapshot's index
// from a peer, retrying until it is there or the node closes. It returns
// true when the log has reached the index and the snapshot can be saved,
// false when it could not run (a storage without an event log) or the node
// is closing — in which case the caller falls through to the fail-fast
// path, whose message says so.
func (n *Raft) catchUpEventLog(snap *raftpb.Snapshot, need, needGen uint64) bool {
	recv, ok := n.storage.(EventReceiver)
	if !ok {
		return false
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-n.closeC:
			cancel()
		case <-ctx.Done():
		}
	}()

	have := recv.EventIndex()
	n.catchUp.begin(have, need)
	defer n.catchUp.end()
	n.logger.Info("event log is behind the snapshot to install; catching up from a peer before installing it",
		zap.Uint64("eventIndex", have), zap.Uint64("snapshotIndex", need),
		zap.Uint64("generation", recv.EventLogGeneration()), zap.Uint64("snapshotCompletedScrubBound", needGen))
	if have > 0 && recv.EventLogGeneration() < needGen {
		// This log predates a scrub the snapshot's bbolt has already pruned
		// the tombstones of: nothing can bring it forward. Fetch it whole.
		n.logger.Warn("this node's event log predates a scrub the cluster completed while it was away; discarding it and fetching it whole",
			zap.Uint64("generation", recv.EventLogGeneration()), zap.Uint64("snapshotCompletedScrubBound", needGen))
		if err := recv.ResetEventLog(); err != nil {
			n.logger.Error("reset event log for catch-up", zap.Error(err))
		}
	}

	sink := &fetchSink{recv: recv, dir: recv.EventFetchDir(), logger: n.logger}
	backoff := catchUpInitialBackoff
	lastWarn := time.Now()
	for {
		have = recv.EventIndex()
		if have >= need {
			n.logger.Info("caught up from a peer; installing the snapshot",
				zap.Uint64("eventIndex", have), zap.Uint64("snapshotIndex", need), zap.Uint64("generation", recv.EventLogGeneration()))
			return true
		}
		req := EventFetchRequest{After: have, To: need, MinGeneration: needGen}
		if have > 0 {
			req.Generation = recv.EventLogGeneration()
		}
		res, err := n.transport.FetchEvents(ctx, req, sink)
		if ctx.Err() != nil {
			return false
		}
		var mismatch *EventGenerationMismatch
		switch {
		case err == nil:
			n.catchUp.update(recv.EventIndex(), res.Peer)
			if recv.EventIndex() > have {
				backoff = catchUpInitialBackoff
				continue
			}
			// The peer answered but had nothing past our index (it is behind
			// the snapshot too); ask again after a pause.
		case errors.As(err, &mismatch):
			if have > 0 && mismatch.Have > recv.EventLogGeneration() {
				n.logger.Warn("a peer's event log is at a newer generation than this node's (a scrub completed while this node was away); "+
					"discarding this node's event log and fetching it whole",
					zap.Uint64("peer", mismatch.Peer), zap.Uint64("peerGeneration", mismatch.Have), zap.Uint64("generation", mismatch.Want))
				if rerr := recv.ResetEventLog(); rerr != nil {
					n.logger.Error("reset event log for catch-up", zap.Error(rerr))
				} else {
					backoff = catchUpInitialBackoff
					continue
				}
			} else {
				n.logger.Debug("skipping a peer whose event log is at an older generation than this node needs",
					zap.Uint64("peer", mismatch.Peer), zap.Uint64("peerGeneration", mismatch.Have), zap.Uint64("wanted", mismatch.Want))
			}
		default:
			if time.Since(lastWarn) >= catchUpWarnEvery {
				lastWarn = time.Now()
				n.logger.Warn("catch-up is waiting: no peer is serving the events this node is missing",
					zap.Uint64("eventIndex", recv.EventIndex()), zap.Uint64("snapshotIndex", need), zap.Error(err))
			} else {
				n.logger.Debug("catch-up fetch failed; retrying", zap.Error(err))
			}
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > catchUpMaxBackoff {
			backoff = catchUpMaxBackoff
		}
	}
}

// fetchSink is the receiving side of a fetch: it pins the generation,
// stages segment files under the events directory and adopts them in
// batches — at the end of the stream, or before a run of records that
// must follow them — and appends record runs. Everything adopted is
// durable before the next exchange is asked for, and a stream cut short
// leaves its staged-but-unadopted files behind for the next exchange to
// overwrite, so an interrupted fetch resumes from wherever it got to.
type fetchSink struct {
	recv   EventReceiver
	dir    string
	logger *zap.Logger
	staged []string
}

func (f *fetchSink) Begin(gen, _ uint64) error {
	// A previous stream that was cut short left its staged files behind;
	// they belong to that stream, not this one.
	f.discardStaged()
	if f.recv.EventIndex() == 0 {
		// An empty log takes the peer's generation, durably, before any
		// content — a restart mid-fetch must resume at it, not start over.
		return f.recv.SetEventLogGeneration(gen)
	}
	if mine := f.recv.EventLogGeneration(); mine != gen {
		return &EventGenerationMismatch{Have: gen, Want: mine}
	}
	return nil
}

func (f *fetchSink) Segment(name string, size int64, r io.Reader) error {
	if err := os.MkdirAll(f.dir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(f.dir, name)
	_ = os.Remove(path) // a previous attempt's partial file
	if err := writeStagedSegment(path, size, r); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("stage %s: %w", name, err)
	}
	f.staged = append(f.staged, path)
	return nil
}

func (f *fetchSink) Records(data []byte) error {
	if err := f.adoptStaged(); err != nil {
		return err
	}
	return f.recv.AppendFetchedRecords(data)
}

func (f *fetchSink) End(EventServeResult) error { return f.adoptStaged() }

// adoptStaged takes every staged segment into the log in one reopen. The
// files are consumed either way: adoption moves them, and a refused batch
// is removed so a retry stages afresh.
func (f *fetchSink) adoptStaged() error {
	if len(f.staged) == 0 {
		return nil
	}
	paths := f.staged
	f.staged = nil
	if err := f.recv.AdoptEventSegments(paths); err != nil {
		for _, p := range paths {
			_ = os.Remove(p)
		}
		return err
	}
	return nil
}

func (f *fetchSink) discardStaged() {
	for _, p := range f.staged {
		_ = os.Remove(p)
	}
	f.staged = nil
}

func writeStagedSegment(path string, size int64, r io.Reader) error {
	out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // G304: under this node's own data directory
	if err != nil {
		return err
	}
	n, err := io.Copy(out, io.LimitReader(r, size))
	if err == nil && n != size {
		err = fmt.Errorf("short segment: %d of %d bytes", n, size)
	}
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	return err
}
