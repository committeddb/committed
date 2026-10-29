package stagestore

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
)

// The output-key dictionary. Every retained input is stored under its
// output key and recorded against it in src, so a key that gathers many
// inputs would repeat its bytes in each of them (a 36-byte UUID stream key
// for every event of the stream). Each stage instead stores an output key
// once, in its keys bucket, under an 8-byte id, and the in, src and acc
// buckets refer to the id.
//
// The id is the key's hash; a collision takes the next free id (linear
// probing), so finding a key's id reads ids from its hash on until the key
// or an empty id. A key's id is released once no input is retained under
// it, at the end of the transaction that removed the last one, and only
// when the next id is empty — then no probe passes through it.
//
// The hash is part of the store format: changing it moves every id.

// keyHash is the dictionary's hash (a variable so tests can force
// collisions).
var keyHash = func(k []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(k)
	return h.Sum64()
}

func keysBucket(stage string) []byte { return []byte("stage:" + stage + ":keys") }

// inEntryKey is a retained input's key in the in bucket: its output key's
// id, then its input key.
func inEntryKey(id, inKey []byte) []byte {
	out := make([]byte, 0, len(id)+len(inKey))
	return append(append(out, id...), inKey...)
}

// keyID returns outKey's id in a stage's dictionary, adding it when assign
// is set; found is false when it is absent and not added.
func (tx *Tx) keyID(stage string, outKey []byte, assign bool) (id []byte, found bool, err error) {
	ck := stage + "\x00" + string(outKey)
	if id, ok := tx.ids[ck]; ok {
		return id, true, nil
	}
	b := keysBucket(stage)
	for h := keyHash(outKey); ; h++ {
		cand := binary.BigEndian.AppendUint64(nil, h)
		v, err := tx.get(b, cand)
		if err != nil {
			return nil, false, err
		}
		if v == nil {
			if !assign {
				return nil, false, nil
			}
			if err := tx.put(b, cand, outKey); err != nil {
				return nil, false, err
			}
			tx.remember(stage, ck, cand, outKey)
			if tx.added == nil {
				tx.added = map[string]bool{}
			}
			tx.added[ck] = true
			return cand, true, nil
		}
		if bytes.Equal(v, outKey) {
			tx.remember(stage, ck, cand, outKey)
			return cand, true, nil
		}
	}
}

// KeyAdded reports whether a stage's output key was given its id since
// the transaction began or last called ForgetAddedKeys: an id is held
// while any input is retained under the key, so every input the key
// retains arrived since then.
func (tx *Tx) KeyAdded(stage string, outKey []byte) bool {
	return tx.added[stage+"\x00"+string(outKey)]
}

// ForgetAddedKeys starts KeyAdded afresh.
func (tx *Tx) ForgetAddedKeys() { tx.added = nil }

// keyOf resolves an id to its output key (nil if the id is free).
func (tx *Tx) keyOf(stage string, id []byte) ([]byte, error) {
	ck := stage + "\x00" + string(id)
	if k, ok := tx.keys[ck]; ok {
		return k, nil
	}
	k, err := tx.get(keysBucket(stage), id)
	if err != nil || k == nil {
		return nil, err
	}
	tx.remember(stage, stage+"\x00"+string(k), id, k)
	return k, nil
}

// peekKey is keyOf without remembering the key: for scans over every key.
func (tx *Tx) peekKey(stage string, id []byte) ([]byte, error) {
	if k, ok := tx.keys[stage+"\x00"+string(id)]; ok {
		return k, nil
	}
	return tx.get(keysBucket(stage), id)
}

func (tx *Tx) remember(stage, keyCK string, id, outKey []byte) {
	if tx.ids == nil {
		tx.ids, tx.keys = map[string][]byte{}, map[string][]byte{}
	}
	tx.ids[keyCK] = id
	tx.keys[stage+"\x00"+string(id)] = outKey
}

// mayRelease notes an id whose last input may have left.
func (tx *Tx) mayRelease(stage string, id []byte) {
	if tx.release == nil {
		tx.release = map[string]releaseCandidate{}
	}
	tx.release[stage+"\x00"+string(id)] = releaseCandidate{stage: stage, id: id}
}

type releaseCandidate struct {
	stage string
	id    []byte
}

// releaseKeys frees the noted ids no input is retained under any more —
// the end of a write transaction, so a rekey within it never frees an id
// src still names. Freeing an id can free the one before it in a probe
// run (held back, maybe in an earlier transaction, only because this one
// was taken), so that one is noted too, and it repeats until nothing more
// frees.
func (tx *Tx) releaseKeys() error {
	for len(tx.release) > 0 {
		freed := false
		for ck, c := range tx.release {
			done, err := tx.tryRelease(c)
			if err != nil {
				return err
			}
			if done {
				delete(tx.release, ck)
				freed = freed || done
			}
		}
		if !freed {
			break
		}
	}
	tx.release = nil
	return nil
}

// tryRelease frees one id if no input is retained under it and no probe
// passes through it; done is false only while the next id is taken.
func (tx *Tx) tryRelease(c releaseCandidate) (done bool, err error) {
	it, err := tx.iter(tx.key(inBucket(c.stage), c.id))
	if err != nil {
		return false, err
	}
	inUse := it.First()
	if err := it.Error(); err != nil {
		return false, err
	}
	if inUse {
		return true, nil // still in use: nothing to free
	}
	next := binary.BigEndian.AppendUint64(nil, binary.BigEndian.Uint64(c.id)+1)
	v, err := tx.get(keysBucket(c.stage), next)
	if err != nil || v != nil {
		return false, err // a probe may pass through this id
	}
	k, err := tx.keyOf(c.stage, c.id)
	if err != nil || k == nil {
		return k == nil, err
	}
	if err := tx.del(keysBucket(c.stage), c.id); err != nil {
		return false, err
	}
	// A fold state outliving its key's inputs is never handed to the next
	// key given this id.
	if err := tx.del(accBucket(c.stage), c.id); err != nil {
		return false, err
	}
	delete(tx.keys, c.stage+"\x00"+string(c.id))
	delete(tx.ids, c.stage+"\x00"+string(k))
	// The id before this one may have stayed only because this one was taken.
	prev := binary.BigEndian.AppendUint64(nil, binary.BigEndian.Uint64(c.id)-1)
	if pk, err := tx.keyOf(c.stage, prev); err != nil {
		return false, err
	} else if pk != nil {
		tx.mayRelease(c.stage, prev)
	}
	return true, nil
}
