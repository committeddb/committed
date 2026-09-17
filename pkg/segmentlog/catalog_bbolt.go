package segmentlog

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"iter"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/durablefs"
	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

const boltCatalogName = "metadata.db"

var (
	boltStateBucket   = []byte("state")
	boltRangesBucket  = []byte("ranges")
	boltRetiredBucket = []byte("retired")
	boltHeaderKey     = []byte("header")
)

type boltHeader struct {
	Version uint64
	Catalog Catalog
	Ranges  uint64
}

// boltCatalog is private to a managed Log and serialized by its mutex. bbolt
// owns metadata durability; segment preparation owns payload durability. commit
// is the transaction boundary, replaceable in tests to exercise uncertain I/O.
type boltCatalog struct {
	path   string
	db     *bolt.DB
	commit func(func(*bolt.Tx) error) error
	poison error
}

// CreateBoltLog creates an experimental log with a dedicated bbolt catalog.
// It requires an empty durable directory. It does not change CreateLog's format.
func CreateBoltLog(path string, start uint64, opts LogOptions) (*Log, error) {
	return createLog(path, start, opts, true)
}

// OpenBoltLog recovers the bbolt header and active tail. Closed ranges are read
// and checked on demand; it does not verify all historical payloads at startup.
// Missing metadata is an error, never an instruction to initialize a new log.
func OpenBoltLog(path string, encoding Options) (*Log, error) { return openLog(path, encoding, true) }

func boltEncode(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return format.LE.AppendUint32(b, format.CRC(b)), nil
}

func boltDecode(b []byte, v any) error {
	if len(b) < 4 || format.CRC(b[:len(b)-4]) != format.LE.Uint32(b[len(b)-4:]) {
		return ErrCorrupt
	}
	if err := json.Unmarshal(b[:len(b)-4], v); err != nil {
		return errors.Join(ErrCorrupt, err)
	}
	canonical, err := boltEncode(v)
	if err != nil || !bytes.Equal(b, canonical) {
		return ErrCorrupt
	}
	return nil
}

func rangeKey(start uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, start)
	return b
}

func validateBoltHeader(h boltHeader) error {
	c := h.Catalog
	if h.Version != 1 {
		return ErrUnsupported
	}
	if c.Active == nil || len(c.Segments) != 0 || c.SegmentBytes == 0 || c.Start > c.Active.Start {
		return ErrCorrupt
	}
	if (h.Ranges == 0) != (c.Start == c.Active.Start) || h.Ranges > c.Active.Start-c.Start {
		return ErrCorrupt
	}
	// Validate the standalone header with an empty historical prefix. Original
	// Start remains in the stored header; range continuity is checked by iteration.
	c.Start = c.Active.Start
	if err := validateCatalog(c); err != nil {
		return errors.Join(ErrCorrupt, err)
	}
	return nil
}

func readBoltHeader(tx *bolt.Tx) (h boltHeader, err error) {
	b := tx.Bucket(boltStateBucket)
	if b == nil || tx.Bucket(boltRangesBucket) == nil || tx.Bucket(boltRetiredBucket) == nil {
		return h, ErrCorrupt
	}
	if err = boltDecode(b.Get(boltHeaderKey), &h); err != nil {
		return h, err
	}
	return h, validateBoltHeader(h)
}

func putBoltHeader(tx *bolt.Tx, h boltHeader) error {
	if err := validateBoltHeader(h); err != nil {
		return err
	}
	b, err := boltEncode(h)
	if err != nil {
		return err
	}
	return tx.Bucket(boltStateBucket).Put(boltHeaderKey, b)
}

func decodeBoltRef(k, v []byte) (ref SegmentRef, err error) {
	if len(k) != 8 {
		return ref, ErrCorrupt
	}
	if err = boltDecode(v, &ref); err != nil {
		return ref, err
	}
	if binary.BigEndian.Uint64(k) != ref.Coverage.Start {
		return ref, ErrCorrupt
	}
	c := Catalog{History: [16]byte{1}, Revision: 1, Start: ref.Coverage.Start, Segments: []SegmentRef{ref}}
	if err = validateCatalog(c); err != nil {
		return ref, errors.Join(ErrCorrupt, err)
	}
	return ref, nil
}

func putBoltRef(tx *bolt.Tx, ref SegmentRef) error {
	b, err := boltEncode(ref)
	if err != nil {
		return err
	}
	if _, err = decodeBoltRef(rangeKey(ref.Coverage.Start), b); err != nil {
		return err
	}
	return tx.Bucket(boltRangesBucket).Put(rangeKey(ref.Coverage.Start), b)
}

func openBoltDB(path string) (*boltCatalog, error) {
	db, err := bolt.Open(filepath.Join(path, boltCatalogName), 0o600, &bolt.Options{Timeout: time.Second, FreelistType: bolt.FreelistMapType})
	if err != nil {
		return nil, err
	}
	return &boltCatalog{path: path, db: db, commit: db.Update}, nil
}

func createBoltCatalog(path string, c Catalog) (*boltCatalog, error) {
	if c.Revision != 1 || len(c.Segments) != 0 {
		return nil, ErrInvalid
	}
	if err := validateCatalog(c); err != nil {
		return nil, err
	}
	if err := verifyCatalogFiles(path, c); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(path, boltCatalogName), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600) // #nosec G304 -- Fixed metadata basename inside the exclusively owned log directory.
	if err != nil {
		return nil, err
	}
	if err = f.Close(); err != nil {
		return nil, err
	}
	s, err := openBoltDB(path)
	if err != nil {
		return nil, err
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{boltStateBucket, boltRangesBucket, boltRetiredBucket} {
			if _, e := tx.CreateBucket(name); e != nil {
				return e
			}
		}
		return putBoltHeader(tx, boltHeader{Version: 1, Catalog: c})
	})
	if err == nil {
		var d *durablefs.Dir
		d, err = durablefs.Open(path)
		if err == nil {
			err = d.Sync()
		}
	}
	if err != nil {
		return nil, errors.Join(err, s.Close())
	}
	return s, nil
}

func openBoltCatalog(path string) (*boltCatalog, error) {
	info, err := os.Lstat(filepath.Join(path, boltCatalogName))
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return nil, ErrCorrupt
	}
	s, err := openBoltDB(path)
	if err != nil {
		return nil, err
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		h, e := readBoltHeader(tx)
		if e != nil {
			return e
		}
		c := tx.Bucket(boltRangesBucket).Cursor()
		k, v := c.First()
		if h.Ranges == 0 {
			if k != nil {
				return ErrCorrupt
			}
			return nil
		}
		ref, e := decodeBoltRef(k, v)
		if e != nil {
			return e
		}
		if ref.Coverage.Start != h.Catalog.Start {
			return ErrCorrupt
		}
		k, v = c.Last()
		ref, e = decodeBoltRef(k, v)
		if e != nil {
			return e
		}
		if ref.Coverage.End != h.Catalog.Active.Start {
			return ErrCorrupt
		}
		return nil
	})
	if err != nil {
		return nil, errors.Join(err, s.Close())
	}
	return s, nil
}

func (s *boltCatalog) Close() error { return s.db.Close() }

func (s *boltCatalog) head() (c Catalog, err error) {
	if s.poison != nil {
		return c, s.poison
	}
	err = s.db.View(func(tx *bolt.Tx) error { h, e := readBoltHeader(tx); c = h.Catalog; return e })
	return c, err
}

func (s *boltCatalog) Current() (c Catalog, err error) {
	c, err = s.head()
	if err != nil {
		return c, err
	}
	for ref, e := range s.ranges(Coverage{c.Start, c.Active.Start}) {
		if e != nil {
			return c, e
		}
		c.Segments = append(c.Segments, ref)
	}
	return c, nil
}

func (s *boltCatalog) ranges(bounds Coverage) iter.Seq2[SegmentRef, error] {
	return func(yield func(SegmentRef, error) bool) {
		if s.poison != nil {
			yield(SegmentRef{}, s.poison)
			return
		}
		err := s.db.View(func(tx *bolt.Tx) error {
			h, e := readBoltHeader(tx)
			if e != nil {
				return e
			}
			head := h.Catalog
			if bounds.Start >= bounds.End || bounds.Start >= head.Active.Start || bounds.End <= head.Start {
				return nil
			}
			cursor := tx.Bucket(boltRangesBucket).Cursor()
			var k, v []byte
			start := max(bounds.Start, head.Start)
			if start == head.Start {
				k, v = cursor.First()
			} else {
				k, v = cursor.Seek(rangeKey(start))
				if k == nil {
					k, v = cursor.Last()
				} else if !bytes.Equal(k, rangeKey(start)) {
					k, v = cursor.Prev()
				}
			}
			next := start
			for next < min(bounds.End, head.Active.Start) {
				ref, e := decodeBoltRef(k, v)
				if e != nil {
					return e
				}
				if ref.Coverage.Start > next || ref.Coverage.End <= next || ref.Coverage.End > head.Active.Start {
					return ErrCorrupt
				}
				if !yield(ref, nil) {
					return nil
				}
				next = ref.Coverage.End
				k, v = cursor.Next()
				if len(k) == 8 && binary.BigEndian.Uint64(k) != next {
					return ErrCorrupt
				}
			}
			return nil
		})
		if err != nil {
			yield(SegmentRef{}, err)
		}
	}
}

// Rewrites validate their selected files while preparing replacements. bbolt
// metadata publication does not require an unrelated full-history preflight.
func (s *boltCatalog) preflight() error { return s.poison }

func (s *boltCatalog) update(fn func(*bolt.Tx) error) error {
	if s.poison != nil {
		return s.poison
	}
	if err := s.commit(fn); err != nil {
		s.poison = errors.Join(ErrCatalogPoisoned, err)
		return s.poison
	}
	return nil
}
