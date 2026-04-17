package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/philborlin/committed/internal/cluster"
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

// forEachCurrent iterates over all resources in a bucket, calling fn with
// the resource ID and the current version's data.
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
