package wal

import (
	"hash/maphash"
	"math"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
)

// metadataSelection is shared by the legacy and experimental log scans. Callers
// feed entries in index order, capped at their scrub bound. It learns user kinds
// from registrations in that prefix, never from mutable live type state. Memory
// scales with registered types and selected keys; it is not an engine index.
//
// scrubTypes names the user types whose setting as of the bound is onSupersede
// = "scrub" (Storage.scrubTypesAt). Their whole prefix is selected, including
// entries written before the declaration. Those types are usually keyed one
// entity per key (an events table keyed by event id), so most keys appear once
// and remembering every key would grow with the log. Their keys therefore go
// through a membership filter first, and only a key the filter may already
// have seen enters latest. The filter never misses a key it has seen, so every
// key written twice is in latest at its last index; a false positive only adds
// a key whose one entry is its own latest, which removes nothing.
type metadataSelection struct {
	latest     map[string]uint64
	userKind   map[string]cluster.EntityKind
	scrubTypes map[string]bool
	seen       *seenFilter
}

func newMetadataSelection(scrubTypes map[string]bool) *metadataSelection {
	return &metadataSelection{
		latest:     make(map[string]uint64),
		userKind:   make(map[string]cluster.EntityKind),
		scrubTypes: scrubTypes,
		seen:       newSeenFilter(),
	}
}

func (s *metadataSelection) observe(entry *pb.Entry) error {
	if entry.GetType() != pb.EntryNormal || entry.Data == nil {
		return nil
	}
	index := entry.GetIndex()
	return cluster.ForEachProposalEntity(entry.Data, func(typeID string, key, data []byte, isDelete bool) error {
		if cluster.IsType(typeID) && !isDelete {
			typ := new(cluster.Type)
			if err := typ.Unmarshal(data); err != nil {
				return err
			}
			s.userKind[typ.ID] = typ.EntityKind
			return nil
		}
		tk := string(tombstoneKey(typeID, key))
		if cluster.IsSystemTombstonable(typeID) || s.userKind[typeID] == cluster.EntityKindSnapshot {
			if index > s.latest[tk] {
				s.latest[tk] = index
			}
			return nil
		}
		if !s.scrubTypes[typeID] {
			return nil
		}
		if _, ok := s.latest[tk]; ok || s.seen.testAndAdd(tk) {
			if index > s.latest[tk] {
				s.latest[tk] = index
			}
		}
		return nil
	})
}

// seenFilter is a scalable Bloom filter: a chain of bit arrays, each twice the
// previous one's capacity and added once the newest is full, so memory follows
// the number of keys observed. Each segment halves the previous one's
// false-positive rate (about 1.44 more bits per key), so the chain's rate stays
// under twice the first segment's (~1%), however many segments it grows. It is
// rebuilt for every scrub and never persisted. Its hash seed is random per
// process: false positives never change the selection (see metadataSelection),
// so replicas need not agree on them.
type seenFilter struct {
	seed  maphash.Seed
	parts []*bloomPart
}

type bloomPart struct {
	bits     []uint64
	k        uint64 // probes per key
	capacity int
	count    int
}

const (
	seenFilterInitialKeys = 1 << 16
	// The first segment: 10 bits and 7 probes per key, ~0.8% false positives.
	seenFilterBitsPerKey = 10.0
	// Halving a segment's false-positive rate costs log2(e) bits per key.
	seenFilterBitsPerHalving = 1.4427
)

func newSeenFilter() *seenFilter {
	return &seenFilter{seed: maphash.MakeSeed()}
}

// testAndAdd reports whether key may have been added before, and adds it.
func (f *seenFilter) testAndAdd(key string) bool {
	h1 := maphash.String(f.seed, key)
	h2 := h1>>32 | h1<<32 | 1 // odd, so probes cycle every bit position
	for _, p := range f.parts {
		if p.has(h1, h2) {
			return true
		}
	}
	if len(f.parts) == 0 || f.parts[len(f.parts)-1].count >= f.parts[len(f.parts)-1].capacity {
		n := len(f.parts)
		capacity := seenFilterInitialKeys << n
		bitsPerKey := seenFilterBitsPerKey + float64(n)*seenFilterBitsPerHalving
		words := int(math.Ceil(float64(capacity) * bitsPerKey / 64))
		k := uint64(math.Ceil(bitsPerKey * math.Ln2))
		f.parts = append(f.parts, &bloomPart{bits: make([]uint64, words), k: k, capacity: capacity})
	}
	f.parts[len(f.parts)-1].add(h1, h2)
	return false
}

func (p *bloomPart) has(h1, h2 uint64) bool {
	n := uint64(len(p.bits)) * 64
	for i := uint64(0); i < p.k; i++ {
		bit := (h1 + i*h2) % n
		if p.bits[bit/64]&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

func (p *bloomPart) add(h1, h2 uint64) {
	n := uint64(len(p.bits)) * 64
	for i := uint64(0); i < p.k; i++ {
		bit := (h1 + i*h2) % n
		p.bits[bit/64] |= 1 << (bit % 64)
	}
	p.count++
}
