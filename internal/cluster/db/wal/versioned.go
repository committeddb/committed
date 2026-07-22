package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/cluster"
)

var (
	versionsBucket = []byte("versions")
	currentKey     = []byte("current")
)

// versionKey encodes a version number as big-endian uint64 for bbolt key ordering.
func versionKey(version uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], version)
	return buf[:]
}

// parseVersionKey decodes a big-endian uint64 version key.
func parseVersionKey(k []byte) uint64 {
	return binary.BigEndian.Uint64(k)
}

// versionLastIndexKey stores, per versioned id, the raft index of its most
// recent version-write — a direct key in the id sub-bucket alongside
// currentKey (versions live in the versionsBucket sub-bucket, so there is no
// collision).
var versionLastIndexKey = []byte("lastIndex")

// versionedLastIndex returns the raft index of id's most recent version-write,
// or 0 if id has no versions yet. It is the replay guard for
// ApplyCommittedBatch's widened crash window: a versioned apply whose entry
// index is <= this value has already been applied and MUST be skipped —
// otherwise putVersioned's last+1 allocator appends a phantom version on
// replay, diverging version history across replicas (config-version-replay).
func versionedLastIndex(resourceBucket *bolt.Bucket, id []byte) uint64 {
	idBucket := resourceBucket.Bucket(id)
	if idBucket == nil {
		return 0
	}
	v := idBucket.Get(versionLastIndexKey)
	if len(v) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

// setVersionedLastIndex records raftIndex as id's most-recent version-write
// index, creating the id sub-bucket if needed. Called inside the same bbolt
// transaction as the version write, so a failure rolls back both.
func setVersionedLastIndex(resourceBucket *bolt.Bucket, id []byte, raftIndex uint64) error {
	idBucket, err := resourceBucket.CreateBucketIfNotExists(id)
	if err != nil {
		return fmt.Errorf("create resource bucket %q: %w", id, err)
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], raftIndex)
	return idBucket.Put(versionLastIndexKey, buf[:])
}

// putVersioned stores a new version of a resource within a resource bucket.
// It creates the resource sub-bucket and versions sub-bucket if they don't exist,
// determines the next version number, writes the data, and updates "current".
// Returns the new version number.
func putVersioned(resourceBucket *bolt.Bucket, id []byte, data []byte) (uint64, error) {
	idBucket, err := resourceBucket.CreateBucketIfNotExists(id)
	if err != nil {
		return 0, fmt.Errorf("create resource bucket %q: %w", id, err)
	}

	verBucket, err := idBucket.CreateBucketIfNotExists(versionsBucket)
	if err != nil {
		return 0, fmt.Errorf("create versions bucket: %w", err)
	}

	var nextVersion uint64 = 1
	cursor := verBucket.Cursor()
	k, _ := cursor.Last()
	if k != nil {
		nextVersion = parseVersionKey(k) + 1
	}

	key := versionKey(nextVersion)
	if err := verBucket.Put(key, data); err != nil {
		return 0, fmt.Errorf("put version %d: %w", nextVersion, err)
	}

	if err := idBucket.Put(currentKey, key); err != nil {
		return 0, fmt.Errorf("put current: %w", err)
	}

	return nextVersion, nil
}

// getVersioned retrieves the current version's data for a resource.
func getVersioned(resourceBucket *bolt.Bucket, id []byte) ([]byte, error) {
	idBucket := resourceBucket.Bucket(id)
	if idBucket == nil {
		return nil, cluster.ErrResourceNotFound
	}

	currentVer := idBucket.Get(currentKey)
	if currentVer == nil {
		return nil, cluster.ErrResourceNotFound
	}

	verBucket := idBucket.Bucket(versionsBucket)
	if verBucket == nil {
		return nil, cluster.ErrResourceNotFound
	}

	data := verBucket.Get(currentVer)
	if data == nil {
		return nil, cluster.ErrVersionNotFound
	}

	return data, nil
}

// getVersion retrieves a specific version's data.
func getVersion(resourceBucket *bolt.Bucket, id []byte, version uint64) ([]byte, error) {
	idBucket := resourceBucket.Bucket(id)
	if idBucket == nil {
		return nil, cluster.ErrResourceNotFound
	}

	verBucket := idBucket.Bucket(versionsBucket)
	if verBucket == nil {
		return nil, cluster.ErrResourceNotFound
	}

	data := verBucket.Get(versionKey(version))
	if data == nil {
		return nil, cluster.ErrVersionNotFound
	}

	return data, nil
}

// listVersions returns metadata about all versions of a resource.
func listVersions(resourceBucket *bolt.Bucket, id []byte) ([]cluster.VersionInfo, error) {
	idBucket := resourceBucket.Bucket(id)
	if idBucket == nil {
		return nil, cluster.ErrResourceNotFound
	}

	currentVer := idBucket.Get(currentKey)

	verBucket := idBucket.Bucket(versionsBucket)
	if verBucket == nil {
		return nil, cluster.ErrResourceNotFound
	}

	var versions []cluster.VersionInfo
	err := verBucket.ForEach(func(k, v []byte) error {
		ver := parseVersionKey(k)
		isCurrent := bytes.Equal(k, currentVer)
		versions = append(versions, cluster.VersionInfo{Version: ver, Current: isCurrent})
		return nil
	})

	return versions, err
}

// deleteVersioned deletes a resource (all its versions) by deleting the sub-bucket.
func deleteVersioned(resourceBucket *bolt.Bucket, id []byte) error {
	if resourceBucket.Bucket(id) == nil {
		return nil
	}
	return resourceBucket.DeleteBucket(id)
}

// configExists reports whether a versioned config of the given kind is currently
// present for id — i.e. the id's versioned sub-bucket exists under configBucket
// (deleteVersioned removes exactly that sub-bucket). The per-config-id sibling
// writers gate their Puts on this so a worker bump that commits AFTER the config
// was deleted reaps the orphan instead of re-establishing it; otherwise a same-id
// recreate resumes from stale sibling state and silently skips work. Config
// existence is replicated state applied in identical log order on every node, so
// the gate is deterministic.
func configExists(tx *bolt.Tx, configBucket, id []byte) bool {
	cfg := tx.Bucket(configBucket)
	return cfg != nil && cfg.Bucket(id) != nil
}

// forEachCurrent iterates over all resources in a bucket, calling fn with
// the resource ID and the current version's data.
// rawConfig is one config listed tolerantly for reconcile: an undecodable
// entry is degraded (decodeErr set, cfg nil) rather than aborting the whole
// list, so a single corrupt config cannot strand the entire data plane.
type rawConfig struct {
	id        string
	cfg       *cluster.Configuration
	decodeErr error
}

// listRawConfigs lists the current version of every config in the named bucket
// TOLERANTLY — an undecodable one is marked degraded, not fatal. It performs NO
// parsing (that runs outside the view lock in the caller: a parser may call
// back into view-backed storage methods, which would recursively lock kvMu).
// Returns the raw configs, the set of present ids, and only a genuine bbolt
// error (never a per-config decode failure).
func (s *Storage) listRawConfigs(bucketName []byte) ([]rawConfig, map[string]struct{}, error) {
	var raws []rawConfig
	present := make(map[string]struct{})
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return ErrBucketMissing
		}
		return forEachCurrent(b, func(id, data []byte) error {
			idStr := string(id)
			present[idStr] = struct{}{}
			cfg := &cluster.Configuration{}
			if err := cfg.Unmarshal(data); err != nil {
				raws = append(raws, rawConfig{id: idStr, decodeErr: err})
				return nil
			}
			raws = append(raws, rawConfig{id: idStr, cfg: cfg})
			return nil
		})
	})
	if err != nil {
		return nil, nil, err
	}
	return raws, present, nil
}

func forEachCurrent(resourceBucket *bolt.Bucket, fn func(id []byte, data []byte) error) error {
	return resourceBucket.ForEach(func(k, v []byte) error {
		if v != nil {
			// Flat key/value (pre-migration) — skip gracefully.
			return nil
		}
		idBucket := resourceBucket.Bucket(k)
		if idBucket == nil {
			return nil
		}
		currentVer := idBucket.Get(currentKey)
		if currentVer == nil {
			return nil
		}
		verBucket := idBucket.Bucket(versionsBucket)
		if verBucket == nil {
			return nil
		}
		data := verBucket.Get(currentVer)
		if data == nil {
			return nil
		}
		return fn(k, data)
	})
}
