// Package stagestore is the node-local state a syncable's internal stages
// fold into: one bbolt file per syncable under <dataDir>/projections/.
//
// Everything in a store is DERIVED data — a pure function of a log prefix
// (the pipeline design's log-prefix determinism) — so the store is a cache,
// never a source of truth: it is not raft-replicated, it is rebuildable
// from the log at any time, and every recovery posture below leans on that.
// A corrupt or mismatched store is deleted and rebuilt loudly; an ownership
// move re-derives (or resumes a stale store by its recorded frontier); the
// fold runs without fsync and hardens only at checkpoint boundaries,
// because the invariant that matters is
//
//	store frontier >= replicated checkpoint
//
// with idempotent re-apply covering the window between them.
//
// Layout (buckets):
//
//	meta                     format version, config fingerprint, frontier
//	stage:<name>:out         output key -> the stage's current output bytes
//	stage:<name>:in          retained input set: frame(outKey)+inKey -> input
//	stage:<name>:rev:<join>  reverse index: frame(dimKey)+outKey -> nil
//
// The in/rev keys are length-framed (varint prefix) rather than
// separator-joined so an output key containing any byte — including a
// separator — scans correctly.
package stagestore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// formatVersion is bumped when the layout changes incompatibly; an old
// store is then reset and rebuilt from the log rather than migrated.
const formatVersion uint64 = 1

var (
	metaBucket      = []byte("meta")
	metaFormat      = []byte("format")
	metaFingerprint = []byte("fingerprint")
	metaFrontier    = []byte("frontier")
)

// FilePath is the store file for one syncable under a store dir — the
// naming Open uses, exported so teardown paths can remove a store they
// never opened.
func FilePath(dir, name string) string { return filepath.Join(dir, name+".db") }

// Store is one syncable's stage state. Not safe for concurrent use by
// multiple goroutines beyond what bbolt provides; the syncable worker is
// the single writer.
type Store struct {
	db   *bolt.DB
	path string
}

// Open opens (creating if needed) the store for one syncable and verifies
// its format version and config fingerprint. On any mismatch — a torn file
// bbolt cannot open, an older format, a changed config — the store is
// DELETED and recreated, loudly, and reset=true tells the caller to
// rebuild stage state from the log (the store is a cache; the log is
// truth). A fresh store also reports reset=true: either way the caller's
// next act is a backfill from index 0 or its checkpoint.
func Open(dir, name, fingerprint string) (_ *Store, reset bool, err error) {
	if dir == "" {
		return nil, false, errors.New("stage store dir is empty (the node's projections dir was not threaded to this syncable)")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, false, fmt.Errorf("create stage store dir: %w", err)
	}
	path := FilePath(dir, name)

	open := func() (*bolt.DB, error) {
		// NoSync: the fold's writes harden at checkpoint boundaries via
		// Sync(), not per-transaction — see the package invariant.
		// Timeout: bolt's flock otherwise waits FOREVER — a second opener
		// (a leaked validation parse, a misbehaving process) must fail
		// loudly, never wedge a worker goroutine silently.
		return bolt.Open(path, 0o600, &bolt.Options{NoSync: true, Timeout: 5 * time.Second})
	}

	db, err := open()
	if err != nil {
		// A file bbolt cannot open is a torn/corrupt store: delete and
		// rebuild. This is STORE corruption, not data corruption — the
		// loud-alive-repairable posture applies to the log; the store is
		// derived and self-heals by re-derivation.
		zap.L().Warn("stage store unreadable — deleting and rebuilding from the log (the store is derived state; no data is lost)",
			zap.String("path", path), zap.Error(err))
		if rmErr := os.Remove(path); rmErr != nil {
			return nil, false, fmt.Errorf("remove unreadable stage store %s: %w", path, rmErr)
		}
		if db, err = open(); err != nil {
			return nil, false, fmt.Errorf("recreate stage store %s: %w", path, err)
		}
		reset = true
	}

	s := &Store{db: db, path: path}
	fresh, mismatch, err := s.checkMeta(fingerprint)
	if err != nil {
		_ = db.Close()
		return nil, false, err
	}
	if mismatch != "" {
		zap.L().Warn("stage store does not match this config — deleting and rebuilding from the log (a changed pipeline must re-derive)",
			zap.String("path", path), zap.String("mismatch", mismatch))
		_ = db.Close()
		if rmErr := os.Remove(path); rmErr != nil {
			return nil, false, fmt.Errorf("remove mismatched stage store %s: %w", path, rmErr)
		}
		if db, err = open(); err != nil {
			return nil, false, fmt.Errorf("recreate stage store %s: %w", path, err)
		}
		s = &Store{db: db, path: path}
		if _, _, err := s.checkMeta(fingerprint); err != nil {
			_ = db.Close()
			return nil, false, err
		}
		reset = true
	}
	if fresh {
		reset = true
	}
	return s, reset, nil
}

// checkMeta initializes the meta bucket on a fresh store and otherwise
// verifies format and fingerprint, returning a non-empty mismatch reason
// when the store cannot serve this config.
func (s *Store) checkMeta(fingerprint string) (fresh bool, mismatch string, err error) {
	err = s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(metaBucket)
		if err != nil {
			return err
		}
		storedFormat := b.Get(metaFormat)
		if storedFormat == nil {
			fresh = true
			var fv [8]byte
			binary.BigEndian.PutUint64(fv[:], formatVersion)
			if err := b.Put(metaFormat, fv[:]); err != nil {
				return err
			}
			return b.Put(metaFingerprint, []byte(fingerprint))
		}
		if binary.BigEndian.Uint64(storedFormat) != formatVersion {
			mismatch = fmt.Sprintf("format %d (this binary writes %d)", binary.BigEndian.Uint64(storedFormat), formatVersion)
			return nil
		}
		if stored := string(b.Get(metaFingerprint)); stored != fingerprint {
			mismatch = "config fingerprint changed"
			return nil
		}
		return nil
	})
	return fresh, mismatch, err
}

// Close closes the store, syncing first so a clean shutdown never loses
// the NoSync window.
func (s *Store) Close() error {
	if err := s.db.Sync(); err != nil {
		_ = s.db.Close()
		return err
	}
	return s.db.Close()
}

// Path returns the store's file path (for logs and ops).
func (s *Store) Path() string { return s.path }

// Sync forces the store durable — called at checkpoint boundaries, the
// only point durability matters (below the checkpoint the log re-derives).
func (s *Store) Sync() error { return s.db.Sync() }

// Frontier reads the input frontier: the highest log index folded into
// this store, 0 for a fresh one.
func (s *Store) Frontier() (uint64, error) {
	var f uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(metaBucket).Get(metaFrontier); v != nil {
			f = binary.BigEndian.Uint64(v)
		}
		return nil
	})
	return f, err
}

// Update runs one write transaction — the unit of one Actual's fold across
// every stage it touches.
func (s *Store) Update(fn func(tx *Tx) error) error {
	return s.db.Update(func(btx *bolt.Tx) error { return fn(&Tx{btx: btx}) })
}

// View runs one read transaction.
func (s *Store) View(fn func(tx *Tx) error) error {
	return s.db.View(func(btx *bolt.Tx) error { return fn(&Tx{btx: btx}) })
}

// Tx wraps one bbolt transaction with the stage-store vocabulary.
type Tx struct{ btx *bolt.Tx }

// SetFrontier records the input frontier inside the transaction, so state
// and frontier move atomically.
func (tx *Tx) SetFrontier(index uint64) error {
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], index)
	return tx.btx.Bucket(metaBucket).Put(metaFrontier, v[:])
}

func outBucket(stage string) []byte { return []byte("stage:" + stage + ":out") }

func srcBucket(stage string) []byte { return []byte("stage:" + stage + ":src") }

func dimBucket(stage, join string) []byte {
	return []byte("stage:" + stage + ":dim:" + join)
}
func inBucket(stage string) []byte { return []byte("stage:" + stage + ":in") }
func revBucket(stage, join string) []byte {
	return []byte("stage:" + stage + ":rev:" + join)
}

func (tx *Tx) bucket(name []byte) (*bolt.Bucket, error) {
	if tx.btx.Writable() {
		return tx.btx.CreateBucketIfNotExists(name)
	}
	return tx.btx.Bucket(name), nil
}

// PutOut records a stage's current output for one key.
func (tx *Tx) PutOut(stage string, key, val []byte) error {
	b, err := tx.bucket(outBucket(stage))
	if err != nil {
		return err
	}
	return b.Put(key, val)
}

// GetOut reads a stage's current output for one key (nil if absent). The
// returned bytes are only valid within the transaction.
func (tx *Tx) GetOut(stage string, key []byte) ([]byte, error) {
	b, err := tx.bucket(outBucket(stage))
	if err != nil || b == nil {
		return nil, err
	}
	return b.Get(key), nil
}

// DeleteOut removes a stage's output for one key.
func (tx *Tx) DeleteOut(stage string, key []byte) error {
	b, err := tx.bucket(outBucket(stage))
	if err != nil || b == nil {
		return err
	}
	return b.Delete(key)
}

// PutSrc records which output key an input identity currently feeds — the
// reverse question deletes and rekeys need: a topic delete carries only
// the input's key, and a re-emitted input may move to a NEW output key
// (the old one must refold without it).
func (tx *Tx) PutSrc(stage string, inKey, outKey []byte) error {
	b, err := tx.bucket(srcBucket(stage))
	if err != nil {
		return err
	}
	return b.Put(inKey, outKey)
}

// GetSrc reads the output key an input identity feeds (nil if unknown).
func (tx *Tx) GetSrc(stage string, inKey []byte) ([]byte, error) {
	b, err := tx.bucket(srcBucket(stage))
	if err != nil || b == nil {
		return nil, err
	}
	return b.Get(inKey), nil
}

// DeleteSrc removes an input identity's mapping.
func (tx *Tx) DeleteSrc(stage string, inKey []byte) error {
	b, err := tx.bucket(srcBucket(stage))
	if err != nil || b == nil {
		return err
	}
	return b.Delete(inKey)
}

// PutDim stores one dimension row for a stage's join — the joined topic's
// current payload by its entity key, read back at refold to decide
// participation.
func (tx *Tx) PutDim(stage, join string, dimKey, payload []byte) error {
	b, err := tx.bucket(dimBucket(stage, join))
	if err != nil {
		return err
	}
	return b.Put(dimKey, payload)
}

// GetDim reads a join's dimension row (nil if the dimension has not
// arrived — participation fails until it does, and heals when it does).
func (tx *Tx) GetDim(stage, join string, dimKey []byte) ([]byte, error) {
	b, err := tx.bucket(dimBucket(stage, join))
	if err != nil || b == nil {
		return nil, err
	}
	return b.Get(dimKey), nil
}

// DeleteDim removes a dimension row (its dependents stop participating).
func (tx *Tx) DeleteDim(stage, join string, dimKey []byte) error {
	b, err := tx.bucket(dimBucket(stage, join))
	if err != nil || b == nil {
		return err
	}
	return b.Delete(dimKey)
}

// PutIn retains one input for a stage's output key (the refold working
// set: every stateful reduce retains its inputs — no O(1) winner mode).
func (tx *Tx) PutIn(stage string, outKey, inKey, val []byte) error {
	b, err := tx.bucket(inBucket(stage))
	if err != nil {
		return err
	}
	return b.Put(frameKey(outKey, inKey), val)
}

// DeleteIn removes one retained input.
func (tx *Tx) DeleteIn(stage string, outKey, inKey []byte) error {
	b, err := tx.bucket(inBucket(stage))
	if err != nil || b == nil {
		return err
	}
	return b.Delete(frameKey(outKey, inKey))
}

// InputsFor iterates a stage's retained inputs for one output key, in
// stable (bytewise inKey) order — the deterministic-refold order.
func (tx *Tx) InputsFor(stage string, outKey []byte, fn func(inKey, val []byte) error) error {
	b, err := tx.bucket(inBucket(stage))
	if err != nil || b == nil {
		return err
	}
	prefix := framePrefix(outKey)
	c := b.Cursor()
	for k, v := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, v = c.Next() {
		if err := fn(k[len(prefix):], v); err != nil {
			return err
		}
	}
	return nil
}

// PutRev records that dimKey (in one of the stage's joins) is referenced
// by outKey — the fan-out address for dimension changes.
func (tx *Tx) PutRev(stage, join string, dimKey, outKey []byte) error {
	b, err := tx.bucket(revBucket(stage, join))
	if err != nil {
		return err
	}
	return b.Put(frameKey(dimKey, outKey), nil)
}

// DeleteRev removes one reverse-index entry.
func (tx *Tx) DeleteRev(stage, join string, dimKey, outKey []byte) error {
	b, err := tx.bucket(revBucket(stage, join))
	if err != nil || b == nil {
		return err
	}
	return b.Delete(frameKey(dimKey, outKey))
}

// DependentsOf iterates the output keys referencing dimKey through one of
// the stage's joins, in stable order.
func (tx *Tx) DependentsOf(stage, join string, dimKey []byte, fn func(outKey []byte) error) error {
	b, err := tx.bucket(revBucket(stage, join))
	if err != nil || b == nil {
		return err
	}
	prefix := framePrefix(dimKey)
	c := b.Cursor()
	for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
		if err := fn(k[len(prefix):]); err != nil {
			return err
		}
	}
	return nil
}

// InputsAll iterates EVERY retained input of a stage — the epoch sweep's
// set-at-a-time scan (rare: once per refresh marker).
func (tx *Tx) InputsAll(stage string, fn func(outKey, inKey, val []byte) error) error {
	b, err := tx.bucket(inBucket(stage))
	if err != nil || b == nil {
		return err
	}
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		outKey, inKey, ok := splitFrame(k)
		if !ok {
			continue
		}
		if err := fn(outKey, inKey, v); err != nil {
			return err
		}
	}
	return nil
}

// DimsAll iterates every dimension row of one stage join — the epoch
// sweep's dimension scan.
func (tx *Tx) DimsAll(stage, join string, fn func(dimKey, val []byte) error) error {
	b, err := tx.bucket(dimBucket(stage, join))
	if err != nil || b == nil {
		return err
	}
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

// splitFrame undoes frameKey: varint length, first key, second key.
func splitFrame(k []byte) (first, second []byte, ok bool) {
	n64, sz := binary.Uvarint(k)
	if sz <= 0 {
		return nil, nil, false
	}
	rest := k[sz:]
	n := int(n64) //nolint:gosec // G115: bounds-checked against len(rest) next line; a forged length fails there
	if n < 0 || n > len(rest) {
		return nil, nil, false
	}
	return rest[:n], rest[n:], true
}

// frameKey length-frames the first key so composite keys scan correctly
// whatever bytes the keys contain (an output key may legitimately contain
// any byte; a separator could be forged, a varint length cannot).
func frameKey(first, second []byte) []byte {
	out := framePrefix(first)
	return append(out, second...)
}

func framePrefix(first []byte) []byte {
	var lead [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lead[:], uint64(len(first)))
	out := make([]byte, 0, n+len(first))
	out = append(out, lead[:n]...)
	return append(out, first...)
}

func hasPrefix(k, prefix []byte) bool {
	if len(k) < len(prefix) {
		return false
	}
	for i := range prefix {
		if k[i] != prefix[i] {
			return false
		}
	}
	return true
}
