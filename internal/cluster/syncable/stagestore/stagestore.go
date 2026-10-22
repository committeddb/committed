// Package stagestore is the node-local state a syncable's internal stages
// fold into: one Pebble database per store directory (the node's
// <dataDir>/projections/store), shared by every syncable on the node, each
// syncable's state under its own key prefix.
//
// Everything in a store is DERIVED data — a pure function of a log prefix
// (the pipeline design's log-prefix determinism) — so the store is a cache,
// never a source of truth: it is not raft-replicated, it is rebuildable
// from the log at any time, and every recovery posture below leans on that.
// A corrupt or mismatched store is deleted and rebuilt loudly; an ownership
// move re-derives (or resumes a stale store by its recorded frontier); the
// fold commits without fsync, because the invariant that matters is
//
//	store frontier >= replicated checkpoint
//
// with idempotent re-apply covering the window between them.
//
// Layout: every key is frame(syncable name) + frame(bucket) + key, so one
// syncable's state is one contiguous range (reset and teardown delete it
// whole) and each bucket within it is a contiguous range too. The buckets:
//
//	meta                     format version, config fingerprint, frontier
//	stage:<name>:out         output key -> the stage's current output bytes
//	stage:<name>:in          retained input set: frame(outKey)+inKey -> input
//	stage:<name>:rev:<join>  reverse index: frame(dimKey)+outKey -> nil
//	stage:<name>:ord         window stages: frame(partition)+sortKey+inKey -> order length
//	stage:<name>:pos         window stages: inKey -> its current ord key
//	stage:<name>:row         window stages: inKey -> the row's running state
//	stage:<name>:mdim:<join> fan joins: frame(onKey)+producerKey -> the matched row
//	stage:<name>:mpos:<join> fan joins: producerKey -> its current onKey
//	stage:<name>:parent      fan joins: inKey -> the input, for re-fanning
//	stage:<name>:odim:<join> ordered joins: frame(onKey)+sortKey+producerKey -> order end + row
//	stage:<name>:opos:<join> ordered joins: producerKey -> its current odim key
//	stage:<name>:arev:<join> as-of joins: frame(onKey)+asOf sort key+outKey -> asOf end
//	stage:<name>:acc         aggregate stages without joins: output key -> its fold state
//
// The odim/opos buckets exist only for joins declaring orderBy: every
// matching row (version) of a reference, in business order, so the join
// resolves by one seek — the greatest version, or for asOf the greatest at
// or below the input's bound. arev indexes an as-of join's dependents by
// their bound, so a version at T re-resolves only the inputs in [T, next
// version).
//
// The mdim/mpos/parent buckets exist only for stages with a fan join: a
// fan join resolves EVERY row matching a reference (mdim, scanned by the
// onKey prefix; mpos finds a producer row's entry when it moves or
// leaves), and a dimension change re-fans the inputs referencing it from
// their retained copies (parent).
//
// The acc bucket holds an aggregate key's fold state (counts, exact sums,
// extremes, ordered item lists), kept in step with its retained inputs so a
// changed input updates the state instead of refolding every input. It is
// derived state: a key without one refolds from its inputs and writes it.
//
// The ord/pos/row buckets exist only for window stages (reduce =
// "window"): ord walks a partition in business order (the sort key is an
// order-preserving encoding, so bytes compare as values do), pos finds an
// input's entry when it moves or leaves, and row holds the running state
// a recompute continues from instead of re-reading the prefix.
//
// The in/rev keys are length-framed (varint prefix) rather than
// separator-joined so an output key containing any byte — including a
// separator — scans correctly.
package stagestore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"go.uber.org/zap"
)

// formatVersion is bumped when the layout changes incompatibly; an old
// store is then reset and rebuilt from the log rather than migrated.
const formatVersion uint64 = 4 // 2: numeric key parts render canonically (5.0000 ≡ 5); 3: aggregate fold state (acc); 4: one shared Pebble database per node

var (
	metaBucket      = []byte("meta")
	metaFormat      = []byte("format")
	metaFingerprint = []byte("fingerprint")
	metaFrontier    = []byte("frontier")
)

// dbDir is the shared database's directory under a store dir.
func dbDir(dir string) string { return filepath.Join(dir, "store") }

// openWait bounds how long Open waits for another holder of the same
// syncable's store to close it (a worker being replaced) before failing
// loudly — never wedging a worker goroutine silently.
const openWait = 5 * time.Second

// shared is one store dir's database and the syncables holding it open.
type shared struct {
	db    *pebble.DB
	refs  int
	names map[string]bool
}

var (
	sharedMu sync.Mutex
	shareds  = map[string]*shared{}
)

// acquire opens (or references) a store dir's shared database. A database
// Pebble cannot open is deleted and recreated, loudly: every syncable's
// state in it re-derives (the store is a cache; the log is truth).
func acquire(dir string) (*shared, error) {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if sh := shareds[dir]; sh != nil {
		sh.refs++
		return sh, nil
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create stage store dir: %w", err)
	}
	removeLegacyStores(dir)
	path := dbDir(dir)
	db, err := openPebble(path)
	if err != nil {
		zap.L().Warn("stage store unreadable — deleting and rebuilding from the log (the store is derived state; no data is lost)",
			zap.String("path", path), zap.Error(err))
		if rmErr := os.RemoveAll(path); rmErr != nil {
			return nil, fmt.Errorf("remove unreadable stage store %s: %w", path, rmErr)
		}
		if db, err = openPebble(path); err != nil {
			return nil, fmt.Errorf("recreate stage store %s: %w", path, err)
		}
	}
	sh := &shared{db: db, refs: 1, names: map[string]bool{}}
	shareds[dir] = sh
	return sh, nil
}

// release drops one reference, closing the database with the last.
func release(dir string, sh *shared) error {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if sh.refs--; sh.refs > 0 {
		return nil
	}
	delete(shareds, dir)
	// Flush first so a clean shutdown keeps everything the fold wrote.
	err := sh.db.Flush()
	return errors.Join(err, sh.db.Close())
}

func openPebble(path string) (*pebble.DB, error) {
	opts := &pebble.Options{
		CacheSize:          256 << 20,
		MemTableSize:       32 << 20,
		FormatMajorVersion: pebble.FormatNewest,
		Logger:             pebbleLogger{},
	}
	opts.ApplyCompressionSettings(func() pebble.DBCompressionSettings { return pebble.DBCompressionBalanced })
	for i := range opts.Levels {
		// Most of a fold's reads look up keys that may not exist yet (a
		// new input's source, a key's first fold state): a filter answers
		// those without reading the table.
		opts.Levels[i].FilterPolicy = bloom.FilterPolicy(10)
	}
	return pebble.Open(path, opts)
}

// removeLegacyStores deletes the one-file-per-syncable bbolt stores of
// format 3 and earlier: this binary cannot read them, and every syncable
// they served re-derives into the shared database.
func removeLegacyStores(dir string) {
	legacy, _ := filepath.Glob(filepath.Join(dir, "*.db"))
	for _, path := range legacy {
		if err := os.Remove(path); err != nil {
			zap.L().Warn("could not remove a legacy stage store file", zap.String("path", path), zap.Error(err))
			continue
		}
		zap.L().Info("removed a legacy stage store file (its state re-derives into the shared store)", zap.String("path", path))
	}
}

// pebbleLogger routes Pebble's log to the node log: its routine
// information at debug, its errors as warnings.
type pebbleLogger struct{}

func (pebbleLogger) Infof(format string, args ...any) {
	zap.L().Debug("stage store: " + fmt.Sprintf(format, args...))
}

func (pebbleLogger) Errorf(format string, args ...any) {
	zap.L().Warn("stage store: " + fmt.Sprintf(format, args...))
}

func (pebbleLogger) Fatalf(format string, args ...any) {
	zap.L().Fatal("stage store: " + fmt.Sprintf(format, args...))
}

// Store is one syncable's stage state. The syncable worker is its single
// writer.
type Store struct {
	sh     *shared
	dir    string
	name   string
	prefix []byte // frame(name): every key of this store starts with it
}

// Open opens (creating if needed) the store for one syncable and verifies
// its format version and config fingerprint. On any mismatch — an older
// format, a changed config — the syncable's state is DELETED, loudly, and
// reset=true tells the caller to rebuild stage state from the log (the
// store is a cache; the log is truth). A fresh store also reports
// reset=true: either way the caller's next act is a backfill from index 0
// or its checkpoint.
func Open(dir, name, fingerprint string) (_ *Store, reset bool, err error) {
	if dir == "" {
		return nil, false, errors.New("stage store dir is empty (the node's projections dir was not threaded to this syncable)")
	}
	sh, err := acquire(dir)
	if err != nil {
		return nil, false, err
	}
	if err := claim(sh, name); err != nil {
		_ = release(dir, sh)
		return nil, false, err
	}
	s := &Store{sh: sh, dir: dir, name: name, prefix: framePrefix([]byte(name))}
	fresh, mismatch, err := s.checkMeta(fingerprint)
	if err == nil && mismatch != "" {
		zap.L().Warn("stage store does not match this config — deleting and rebuilding from the log (a changed pipeline must re-derive)",
			zap.String("syncable", name), zap.String("path", s.Path()), zap.String("mismatch", mismatch))
		if err = s.clear(); err == nil {
			_, _, err = s.checkMeta(fingerprint)
		}
		reset = true
	}
	if err != nil {
		_ = s.Close()
		return nil, false, err
	}
	return s, reset || fresh, nil
}

// claim reserves one syncable's state for one holder, waiting up to
// openWait for a previous holder to close.
func claim(sh *shared, name string) error {
	deadline := time.Now().Add(openWait)
	for {
		sharedMu.Lock()
		if !sh.names[name] {
			sh.names[name] = true
			sharedMu.Unlock()
			return nil
		}
		sharedMu.Unlock()
		if time.Now().After(deadline) {
			return fmt.Errorf("stage store for %q is already open (a second holder waited %s for the first to close)", name, openWait)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// checkMeta initializes the meta bucket on a fresh store and otherwise
// verifies format and fingerprint, returning a non-empty mismatch reason
// when the store cannot serve this config.
func (s *Store) checkMeta(fingerprint string) (fresh bool, mismatch string, err error) {
	err = s.Update(func(tx *Tx) error {
		storedFormat, err := tx.get(metaBucket, metaFormat)
		if err != nil {
			return err
		}
		if storedFormat == nil {
			fresh = true
			var fv [8]byte
			binary.BigEndian.PutUint64(fv[:], formatVersion)
			if err := tx.put(metaBucket, metaFormat, fv[:]); err != nil {
				return err
			}
			return tx.put(metaBucket, metaFingerprint, []byte(fingerprint))
		}
		if len(storedFormat) != 8 || binary.BigEndian.Uint64(storedFormat) != formatVersion {
			mismatch = fmt.Sprintf("format %x (this binary writes %d)", storedFormat, formatVersion)
			return nil
		}
		stored, err := tx.get(metaBucket, metaFingerprint)
		if err != nil {
			return err
		}
		if string(stored) != fingerprint {
			mismatch = "config fingerprint changed"
		}
		return nil
	})
	return fresh, mismatch, err
}

// clear deletes every key of this store.
func (s *Store) clear() error {
	return s.sh.db.DeleteRange(s.prefix, PrefixEnd(s.prefix), pebble.NoSync)
}

// Remove deletes one syncable's stage state from a store dir — teardown's
// erase, for a store the caller does not hold open.
func Remove(dir, name string) error {
	s, err := openNamed(dir, name)
	if err != nil {
		return err
	}
	return errors.Join(s.clear(), s.Close())
}

// HasState reports whether a store dir holds any state for one syncable.
func HasState(dir, name string) (bool, error) {
	s, err := openNamed(dir, name)
	if err != nil {
		return false, err
	}
	defer func() { _ = s.Close() }()
	found := false
	err = s.View(func(tx *Tx) error {
		it, err := tx.iter(s.prefix)
		if err != nil {
			return err
		}
		found = it.First()
		return nil
	})
	return found, err
}

// openNamed holds one syncable's store without checking its meta.
func openNamed(dir, name string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("stage store dir is empty")
	}
	sh, err := acquire(dir)
	if err != nil {
		return nil, err
	}
	if err := claim(sh, name); err != nil {
		_ = release(dir, sh)
		return nil, err
	}
	return &Store{sh: sh, dir: dir, name: name, prefix: framePrefix([]byte(name))}, nil
}

// Close releases the store; the shared database closes, flushed, with its
// last holder.
func (s *Store) Close() error {
	sharedMu.Lock()
	delete(s.sh.names, s.name)
	sharedMu.Unlock()
	return release(s.dir, s.sh)
}

// Path returns the shared database's directory (for logs and ops).
func (s *Store) Path() string { return dbDir(s.dir) }

// Sync forces the store durable — the point below which a crash loses
// nothing the fold has committed.
func (s *Store) Sync() error { return s.sh.db.LogData(nil, pebble.Sync) }

// DiskUsage estimates the bytes this syncable's state occupies on disk,
// after flushing what is still in memory.
func (s *Store) DiskUsage() (uint64, error) {
	if err := s.sh.db.Flush(); err != nil {
		return 0, err
	}
	return s.sh.db.EstimateDiskUsage(s.prefix, PrefixEnd(s.prefix))
}

// Metrics reports the shared database's engine metrics, for diagnostics.
func (s *Store) Metrics() string { return s.sh.db.Metrics().String() }

// Frontier reads the input frontier: the highest log index folded into
// this store, 0 for a fresh one.
func (s *Store) Frontier() (uint64, error) {
	var f uint64
	err := s.View(func(tx *Tx) error {
		v, err := tx.get(metaBucket, metaFrontier)
		if len(v) == 8 {
			f = binary.BigEndian.Uint64(v)
		}
		return err
	})
	return f, err
}

// Update runs one write transaction — the unit of one Actual's fold across
// every stage it touches. It commits atomically, without fsync.
func (s *Store) Update(fn func(tx *Tx) error) error {
	b := s.sh.db.NewIndexedBatch()
	tx := &Tx{prefix: s.prefix, r: b, w: b}
	err := fn(tx)
	err = errors.Join(err, tx.closeIters())
	if err == nil {
		err = b.Commit(pebble.NoSync)
	}
	return errors.Join(err, b.Close())
}

// View runs one read transaction.
func (s *Store) View(fn func(tx *Tx) error) error {
	tx := &Tx{prefix: s.prefix, r: s.sh.db}
	err := fn(tx)
	return errors.Join(err, tx.closeIters())
}

// Tx is one transaction with the stage-store vocabulary. Bytes it returns
// are the caller's to keep.
type Tx struct {
	prefix []byte
	r      pebble.Reader
	w      *pebble.Batch // nil in a read transaction
	iters  []*pebble.Iterator
}

var errReadOnly = errors.New("stage store: a write in a read transaction")

// key composes a stored key: the store's prefix, the framed bucket, the key.
func (tx *Tx) key(bucket, k []byte) []byte {
	out := make([]byte, 0, len(tx.prefix)+binary.MaxVarintLen64+len(bucket)+len(k))
	out = append(out, tx.prefix...)
	out = binary.AppendUvarint(out, uint64(len(bucket)))
	out = append(out, bucket...)
	return append(out, k...)
}

func (tx *Tx) put(bucket, k, v []byte) error {
	if tx.w == nil {
		return errReadOnly
	}
	return tx.w.Set(tx.key(bucket, k), v, nil)
}

func (tx *Tx) del(bucket, k []byte) error {
	if tx.w == nil {
		return errReadOnly
	}
	return tx.w.Delete(tx.key(bucket, k), nil)
}

// get reads one value (nil if absent; an empty value is non-nil).
func (tx *Tx) get(bucket, k []byte) ([]byte, error) {
	v, closer, err := tx.r.Get(tx.key(bucket, k))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := append(make([]byte, 0, len(v)), v...)
	return out, closer.Close()
}

// iter opens an iterator over every stored key starting with prefix,
// closed when the transaction ends.
func (tx *Tx) iter(prefix []byte) (*pebble.Iterator, error) {
	it, err := tx.r.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: PrefixEnd(prefix)})
	if err != nil {
		return nil, err
	}
	tx.iters = append(tx.iters, it)
	return it, nil
}

func (tx *Tx) closeIters() error {
	var err error
	for _, it := range tx.iters {
		err = errors.Join(err, it.Close())
	}
	tx.iters = nil
	return err
}

// each walks one bucket's entries whose keys start with prefix, in key
// order, handing fn copies of each key (without the prefix) and value.
func (tx *Tx) each(bucket, prefix []byte, fn func(k, v []byte) error) error {
	full := tx.key(bucket, prefix)
	it, err := tx.iter(full)
	if err != nil {
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		if err := fn(clone(it.Key()[len(full):]), clone(it.Value())); err != nil {
			return err
		}
	}
	return it.Error()
}

func clone(b []byte) []byte { return append(make([]byte, 0, len(b)), b...) }

// SetFrontier records the input frontier inside the transaction, so state
// and frontier move atomically.
func (tx *Tx) SetFrontier(index uint64) error {
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], index)
	return tx.put(metaBucket, metaFrontier, v[:])
}

// OutKeyCount reports how many output keys a stage currently holds —
// the introspection read that distinguishes "the fan produced nothing"
// from "the join never matched" (the field triage that had no way to
// ask). 0 for a stage that has never folded.
func (tx *Tx) OutKeyCount(stage string) (int, error) {
	it, err := tx.iter(tx.key(outBucket(stage), nil))
	if err != nil {
		return 0, err
	}
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	return n, it.Error()
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

// PutOut records a stage's current output for one key.
func (tx *Tx) PutOut(stage string, key, val []byte) error {
	return tx.put(outBucket(stage), key, val)
}

// GetOut reads a stage's current output for one key (nil if absent).
func (tx *Tx) GetOut(stage string, key []byte) ([]byte, error) {
	return tx.get(outBucket(stage), key)
}

// DeleteOut removes a stage's output for one key.
func (tx *Tx) DeleteOut(stage string, key []byte) error {
	return tx.del(outBucket(stage), key)
}

// PutSrc records which output key an input identity currently feeds — the
// reverse question deletes and rekeys need: a topic delete carries only
// the input's key, and a re-emitted input may move to a NEW output key
// (the old one must refold without it).
func (tx *Tx) PutSrc(stage string, inKey, outKey []byte) error {
	return tx.put(srcBucket(stage), inKey, outKey)
}

// GetSrc reads the output key an input identity feeds (nil if unknown).
func (tx *Tx) GetSrc(stage string, inKey []byte) ([]byte, error) {
	return tx.get(srcBucket(stage), inKey)
}

// DeleteSrc removes an input identity's mapping.
func (tx *Tx) DeleteSrc(stage string, inKey []byte) error {
	return tx.del(srcBucket(stage), inKey)
}

// PutDim stores one dimension row for a stage's join — the joined topic's
// current payload by its entity key, read back at refold to decide
// participation.
func (tx *Tx) PutDim(stage, join string, dimKey, payload []byte) error {
	return tx.put(dimBucket(stage, join), dimKey, payload)
}

// GetDim reads a join's dimension row (nil if the dimension has not
// arrived — participation fails until it does, and heals when it does).
func (tx *Tx) GetDim(stage, join string, dimKey []byte) ([]byte, error) {
	return tx.get(dimBucket(stage, join), dimKey)
}

// DeleteDim removes a dimension row (its dependents stop participating).
func (tx *Tx) DeleteDim(stage, join string, dimKey []byte) error {
	return tx.del(dimBucket(stage, join), dimKey)
}

// PutIn retains one input for a stage's output key (the refold working
// set: every stateful reduce retains its inputs — no O(1) winner mode).
func (tx *Tx) PutIn(stage string, outKey, inKey, val []byte) error {
	return tx.put(inBucket(stage), frameKey(outKey, inKey), val)
}

// DeleteIn removes one retained input.
func (tx *Tx) DeleteIn(stage string, outKey, inKey []byte) error {
	return tx.del(inBucket(stage), frameKey(outKey, inKey))
}

// InputsFor iterates a stage's retained inputs for one output key, in
// stable (bytewise inKey) order — the deterministic-refold order.
func (tx *Tx) InputsFor(stage string, outKey []byte, fn func(inKey, val []byte) error) error {
	return tx.each(inBucket(stage), framePrefix(outKey), fn)
}

// GetIn reads one retained input (nil if absent).
func (tx *Tx) GetIn(stage string, outKey, inKey []byte) ([]byte, error) {
	return tx.get(inBucket(stage), frameKey(outKey, inKey))
}

func accBucket(stage string) []byte { return []byte("stage:" + stage + ":acc") }

// PutAcc records an aggregate key's fold state.
func (tx *Tx) PutAcc(stage string, outKey, val []byte) error {
	return tx.put(accBucket(stage), outKey, val)
}

// GetAcc reads an aggregate key's fold state (nil if absent).
func (tx *Tx) GetAcc(stage string, outKey []byte) ([]byte, error) {
	return tx.get(accBucket(stage), outKey)
}

// DeleteAcc removes an aggregate key's fold state.
func (tx *Tx) DeleteAcc(stage string, outKey []byte) error {
	return tx.del(accBucket(stage), outKey)
}

func ordBucket(stage string) []byte { return []byte("stage:" + stage + ":ord") }
func posBucket(stage string) []byte { return []byte("stage:" + stage + ":pos") }
func rowBucket(stage string) []byte { return []byte("stage:" + stage + ":row") }

// OrdPrefix is the ord-key prefix of one window partition: every entry of
// the partition, and nothing else, starts with it.
func OrdPrefix(partition []byte) []byte { return framePrefix(partition) }

// OrdKey composes a window row's ord key: the partition prefix, the sort
// key (order values, then tie values — order-preserving encodings), and
// the input key as the final tiebreak.
func OrdKey(partition, sortKey, inKey []byte) []byte {
	out := framePrefix(partition)
	out = append(out, sortKey...)
	return append(out, inKey...)
}

// OrdEntry is one decoded ord entry. OrderEnd and SortEnd are absolute
// offsets into Key: Key[:OrderEnd] is the shared prefix of the row's peer
// group (partition + order values), Key[SortEnd:] is the input key.
type OrdEntry struct {
	Key       []byte
	Partition []byte
	OrderEnd  int
	SortEnd   int
}

// InKey is the entry's input key.
func (e OrdEntry) InKey() []byte { return e.Key[e.SortEnd:] }

// PeerPrefix is the prefix every peer of this row (equal order values in
// the same partition) shares, and that no other row has.
func (e OrdEntry) PeerPrefix() []byte { return e.Key[:e.OrderEnd] }

// decodeOrdEntry resolves an ord key and its value (the order and sort
// lengths) into an entry; ok=false at a cursor's end or on a malformed
// entry.
func decodeOrdEntry(k, v []byte) (OrdEntry, bool) {
	if k == nil {
		return OrdEntry{}, false
	}
	part, rest, ok := splitFrame(k)
	if !ok {
		return OrdEntry{}, false
	}
	orderLen, n1 := binary.Uvarint(v)
	if n1 <= 0 {
		return OrdEntry{}, false
	}
	sortLen, n2 := binary.Uvarint(v[n1:])
	if n2 <= 0 || sortLen > uint64(len(rest)) || orderLen > sortLen {
		return OrdEntry{}, false
	}
	base := len(k) - len(rest)
	return OrdEntry{Key: k, Partition: part, OrderEnd: base + int(orderLen), SortEnd: base + int(sortLen)}, true //nolint:gosec // G115: bounded by len(rest) above
}

// PutOrd records a window row's position: its ord key with the byte
// lengths of the order part and the whole sort key (order + tie values).
func (tx *Tx) PutOrd(stage string, ordKey []byte, orderLen, sortLen int) error {
	v := binary.AppendUvarint(nil, uint64(orderLen)) //nolint:gosec // G115: a length, never negative
	v = binary.AppendUvarint(v, uint64(sortLen))     //nolint:gosec // G115: as above
	return tx.put(ordBucket(stage), ordKey, v)
}

// DeleteOrd removes a window row's position.
func (tx *Tx) DeleteOrd(stage string, ordKey []byte) error {
	return tx.del(ordBucket(stage), ordKey)
}

// GetOrd reads one ord entry (ok=false when absent).
func (tx *Tx) GetOrd(stage string, ordKey []byte) (OrdEntry, bool, error) {
	v, err := tx.get(ordBucket(stage), ordKey)
	if err != nil || v == nil {
		return OrdEntry{}, false, err
	}
	e, ok := decodeOrdEntry(clone(ordKey), v)
	return e, ok, nil
}

// OrdCursor walks one window stage's ord bucket in key (business) order.
// It sees the bucket as it was when opened: after writing to the ord
// bucket, open a new one. Returned entries are the caller's to keep.
type OrdCursor struct {
	it     *pebble.Iterator
	prefix []byte // the bucket's stored-key prefix
}

// OrdCursor opens a cursor over a window stage's ord bucket; on a stage
// that has never folded it yields nothing.
func (tx *Tx) OrdCursor(stage string) (*OrdCursor, error) {
	prefix := tx.key(ordBucket(stage), nil)
	it, err := tx.iter(prefix)
	if err != nil {
		return nil, err
	}
	return &OrdCursor{it: it, prefix: prefix}, nil
}

func (c *OrdCursor) entry(ok bool) (OrdEntry, bool) {
	if !ok {
		return OrdEntry{}, false
	}
	return decodeOrdEntry(clone(c.it.Key()[len(c.prefix):]), clone(c.it.Value()))
}

// Seek positions at the first entry >= k; Next and Prev step. ok=false at
// either end.
func (c *OrdCursor) Seek(k []byte) (OrdEntry, bool) {
	return c.entry(c.it.SeekGE(append(clone(c.prefix), k...)))
}

func (c *OrdCursor) Next() (OrdEntry, bool) { return c.entry(c.it.Next()) }

func (c *OrdCursor) Prev() (OrdEntry, bool) { return c.entry(c.it.Prev()) }

// Last positions at the bucket's last entry.
func (c *OrdCursor) Last() (OrdEntry, bool) { return c.entry(c.it.Last()) }

// PutPos / GetPos / DeletePos map a window input to its current ord key.
func (tx *Tx) PutPos(stage string, inKey, ordKey []byte) error {
	return tx.put(posBucket(stage), inKey, ordKey)
}

func (tx *Tx) GetPos(stage string, inKey []byte) ([]byte, error) {
	return tx.get(posBucket(stage), inKey)
}

func (tx *Tx) DeletePos(stage string, inKey []byte) error {
	return tx.del(posBucket(stage), inKey)
}

// PutRow / GetRow / DeleteRow hold a window row's running state.
func (tx *Tx) PutRow(stage string, inKey, state []byte) error {
	return tx.put(rowBucket(stage), inKey, state)
}

func (tx *Tx) GetRow(stage string, inKey []byte) ([]byte, error) {
	return tx.get(rowBucket(stage), inKey)
}

func (tx *Tx) DeleteRow(stage string, inKey []byte) error {
	return tx.del(rowBucket(stage), inKey)
}

// PutRev records that dimKey (in one of the stage's joins) is referenced
// by outKey — the fan-out address for dimension changes.
func (tx *Tx) PutRev(stage, join string, dimKey, outKey []byte) error {
	return tx.put(revBucket(stage, join), frameKey(dimKey, outKey), nil)
}

// DeleteRev removes one reverse-index entry.
func (tx *Tx) DeleteRev(stage, join string, dimKey, outKey []byte) error {
	return tx.del(revBucket(stage, join), frameKey(dimKey, outKey))
}

// DependentsOf iterates the output keys referencing dimKey through one of
// the stage's joins, in stable order.
func (tx *Tx) DependentsOf(stage, join string, dimKey []byte, fn func(outKey []byte) error) error {
	return tx.each(revBucket(stage, join), framePrefix(dimKey), func(k, _ []byte) error { return fn(k) })
}

// InputsAll iterates EVERY retained input of a stage — the epoch sweep's
// set-at-a-time scan (rare: once per refresh marker).
func (tx *Tx) InputsAll(stage string, fn func(outKey, inKey, val []byte) error) error {
	return tx.each(inBucket(stage), nil, func(k, v []byte) error {
		outKey, inKey, ok := splitFrame(k)
		if !ok {
			return nil
		}
		return fn(outKey, inKey, v)
	})
}

// DimsAll iterates every dimension row of one stage join — the epoch
// sweep's dimension scan.
func (tx *Tx) DimsAll(stage, join string, fn func(dimKey, val []byte) error) error {
	return tx.each(dimBucket(stage, join), nil, fn)
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

func mdimBucket(stage, join string) []byte { return []byte("stage:" + stage + ":mdim:" + join) }
func mposBucket(stage, join string) []byte { return []byte("stage:" + stage + ":mpos:" + join) }
func parentBucket(stage string) []byte     { return []byte("stage:" + stage + ":parent") }

// PutMDim stores one matched row of a fan join under the reference value
// it answers (onKey) and its producer key.
func (tx *Tx) PutMDim(stage, join string, onKey, producerKey, val []byte) error {
	return tx.put(mdimBucket(stage, join), frameKey(onKey, producerKey), val)
}

// GetMDim reads one filed row of a fan join (nil if absent).
func (tx *Tx) GetMDim(stage, join string, onKey, producerKey []byte) ([]byte, error) {
	return tx.get(mdimBucket(stage, join), frameKey(onKey, producerKey))
}

// DeleteMDim removes one matched row of a fan join.
func (tx *Tx) DeleteMDim(stage, join string, onKey, producerKey []byte) error {
	return tx.del(mdimBucket(stage, join), frameKey(onKey, producerKey))
}

// MDimsFor iterates every row a fan join matches for one reference value,
// in producer-key order.
func (tx *Tx) MDimsFor(stage, join string, onKey []byte, fn func(producerKey, val []byte) error) error {
	return tx.each(mdimBucket(stage, join), framePrefix(onKey), fn)
}

// MDimsAll iterates every matched row of a fan join — the epoch sweep's
// scan.
func (tx *Tx) MDimsAll(stage, join string, fn func(onKey, producerKey, val []byte) error) error {
	return tx.each(mdimBucket(stage, join), nil, func(k, v []byte) error {
		onKey, producerKey, ok := splitFrame(k)
		if !ok {
			return nil
		}
		return fn(onKey, producerKey, v)
	})
}

// PutMPos / GetMPos / DeleteMPos map a fan join's producer row to the
// reference value it is filed under.
func (tx *Tx) PutMPos(stage, join string, producerKey, onKey []byte) error {
	return tx.put(mposBucket(stage, join), producerKey, onKey)
}

func (tx *Tx) GetMPos(stage, join string, producerKey []byte) ([]byte, error) {
	return tx.get(mposBucket(stage, join), producerKey)
}

func (tx *Tx) DeleteMPos(stage, join string, producerKey []byte) error {
	return tx.del(mposBucket(stage, join), producerKey)
}

// PutParent / GetParent / DeleteParent retain a fan-join stage's inputs,
// which a dimension change re-fans.
func (tx *Tx) PutParent(stage string, inKey, val []byte) error {
	return tx.put(parentBucket(stage), inKey, val)
}

func (tx *Tx) GetParent(stage string, inKey []byte) ([]byte, error) {
	return tx.get(parentBucket(stage), inKey)
}

func (tx *Tx) DeleteParent(stage string, inKey []byte) error {
	return tx.del(parentBucket(stage), inKey)
}

// ParentsAll iterates a fan-join stage's retained inputs — the epoch
// sweep's scan.
func (tx *Tx) ParentsAll(stage string, fn func(inKey, val []byte) error) error {
	return tx.each(parentBucket(stage), nil, fn)
}

// Join index kinds: an ordered join's versions (odim), each producer row's
// current version key (opos), and an as-of join's dependents (arev).
const (
	JoinIndexVersions   = "odim"
	JoinIndexPositions  = "opos"
	JoinIndexDependents = "arev"
)

func joinIndexBucket(stage, kind, join string) []byte {
	return []byte("stage:" + stage + ":" + kind + ":" + join)
}

// PutJoinIndex / GetJoinIndex / DeleteJoinIndex address one entry of an
// ordered join's index of the given kind.
func (tx *Tx) PutJoinIndex(stage, kind, join string, key, val []byte) error {
	return tx.put(joinIndexBucket(stage, kind, join), key, val)
}

func (tx *Tx) GetJoinIndex(stage, kind, join string, key []byte) ([]byte, error) {
	return tx.get(joinIndexBucket(stage, kind, join), key)
}

func (tx *Tx) DeleteJoinIndex(stage, kind, join string, key []byte) error {
	return tx.del(joinIndexBucket(stage, kind, join), key)
}

// ScanJoinIndex walks the entries starting with prefix: forward from the
// first key >= from, or backward from the last key < from (from nil = the
// prefix's last entry). fn returns false to stop.
func (tx *Tx) ScanJoinIndex(stage, kind, join string, prefix, from []byte, backward bool, fn func(key, val []byte) (bool, error)) error {
	bucketPrefix := tx.key(joinIndexBucket(stage, kind, join), nil)
	it, err := tx.iter(append(clone(bucketPrefix), prefix...))
	if err != nil {
		return err
	}
	var ok bool
	switch {
	case !backward && from != nil && string(from) > string(prefix):
		ok = it.SeekGE(append(clone(bucketPrefix), from...))
	case !backward:
		ok = it.First()
	case from != nil:
		ok = it.SeekLT(append(clone(bucketPrefix), from...))
	default:
		ok = it.Last()
	}
	for ; ok; ok = step(it, backward) {
		more, err := fn(clone(it.Key()[len(bucketPrefix):]), clone(it.Value()))
		if err != nil || !more {
			return err
		}
	}
	return it.Error()
}

func step(it *pebble.Iterator, backward bool) bool {
	if backward {
		return it.Prev()
	}
	return it.Next()
}

// AllJoinIndex iterates every entry of one join index — the epoch sweep.
func (tx *Tx) AllJoinIndex(stage, kind, join string, fn func(key, val []byte) error) error {
	return tx.each(joinIndexBucket(stage, kind, join), nil, fn)
}

// PrefixEnd is the least key greater than every key starting with prefix
// (nil: no such key — the prefix is all 0xFF, so its range runs to the
// end).
func PrefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// SplitFrame undoes a framed key: the first (length-prefixed) key and the
// rest.
func SplitFrame(k []byte) (first, rest []byte, ok bool) { return splitFrame(k) }

// Inspect walks one syncable's stored entries (ForEachEntry) without
// checking its meta — for tests and diagnostics over a store no one holds.
func Inspect(dir, name string, fn func(bucket, key, val []byte) error) error {
	s, err := openNamed(dir, name)
	if err != nil {
		return err
	}
	return errors.Join(s.ForEachEntry(fn), s.Close())
}

// ForEachEntry walks every entry of the store in (bucket, key) order — the
// order the store's references are written in — for tests and diagnostics.
func (s *Store) ForEachEntry(fn func(bucket, key, val []byte) error) error {
	type entry struct{ bucket, key, val []byte }
	var entries []entry
	err := s.View(func(tx *Tx) error {
		it, err := tx.iter(s.prefix)
		if err != nil {
			return err
		}
		for ok := it.First(); ok; ok = it.Next() {
			bucket, key, ok := splitFrame(it.Key()[len(s.prefix):])
			if !ok {
				return fmt.Errorf("stage store: malformed key %q", it.Key())
			}
			entries = append(entries, entry{clone(bucket), clone(key), clone(it.Value())})
		}
		return it.Error()
	})
	if err != nil {
		return err
	}
	// Stored keys order by bucket LENGTH first (the frame); the walk is by
	// bucket name, then key.
	sort.Slice(entries, func(i, j int) bool {
		if c := bytes.Compare(entries[i].bucket, entries[j].bucket); c != 0 {
			return c < 0
		}
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
	for _, e := range entries {
		if err := fn(e.bucket, e.key, e.val); err != nil {
			return err
		}
	}
	return nil
}
