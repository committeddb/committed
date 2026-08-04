package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
)

// The versioned ResolveType path is the syncable-reader hot path — one
// lookup per decoded entity — and is served from an in-memory cache so it
// costs no bbolt read transaction (measured at ~24% of all reader wait time
// with 17 concurrent replaying syncables). These tests pin the cache's two
// obligations: hits must actually bypass bbolt, and every path that can
// change versioned type content (delete, in-place mutable-field edit,
// snapshot restore) must invalidate.

// A second versioned resolve is served from cache: deleting the bbolt
// record out-of-band (no epoch bump) and resolving again still succeeds.
func TestResolveType_VersionedServedFromCache(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	v1 := &cluster.Type{ID: "person", Name: "Person", Version: 1}
	insertTypes(t, s, []*cluster.Type{v1}, 1, 1)

	got, err := s.ResolveType(cluster.TypeRefAt("person", 1))
	require.NoError(t, err)
	require.Equal(t, v1, got)

	require.NoError(t, s.DeleteTypeRecordBypassingCacheForTest("person"))

	got, err = s.ResolveType(cluster.TypeRefAt("person", 1))
	require.NoError(t, err, "second versioned resolve must be served from cache, not bbolt")
	require.Equal(t, v1, got)
}

// A production delete (apply path) must invalidate: the cached version must
// not resurrect a deleted type — the loud lookup error on a residual
// reference is a consistency signal operators rely on.
func TestResolveType_DeleteInvalidates(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	v1 := &cluster.Type{ID: "person", Name: "Person", Version: 1}
	insertTypes(t, s, []*cluster.Type{v1}, 1, 1)

	_, err := s.ResolveType(cluster.TypeRefAt("person", 1))
	require.NoError(t, err)

	saveEntity(t, cluster.NewDeleteTypeEntity("person"), s, 1, 2)

	_, err = s.ResolveType(cluster.TypeRefAt("person", 1))
	require.Error(t, err, "cached version must not survive the type's deletion")
}

// saveType can overwrite the CURRENT version in place (mutable fields:
// migration/entity-kind/discriminator — the "fix a buggy migration"
// operator path). A resolve after the edit must see the new migration; a
// stale cache here would keep applying the exact program the operator just
// fixed.
func TestResolveType_InPlaceMigrationEditInvalidates(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	before := &cluster.Type{ID: "person", Name: "Person", Version: 1, Migration: []byte(".old")}
	insertTypes(t, s, []*cluster.Type{before}, 1, 1)

	got, err := s.ResolveType(cluster.TypeRefAt("person", 1))
	require.NoError(t, err)
	require.Equal(t, []byte(".old"), got.Migration)

	after := &cluster.Type{ID: "person", Name: "Person", Version: 1, Migration: []byte(".new")}
	insertTypes(t, s, []*cluster.Type{after}, 1, 2)

	got, err = s.ResolveType(cluster.TypeRefAt("person", 1))
	require.NoError(t, err)
	require.Equal(t, []byte(".new"), got.Migration, "in-place migration edit must invalidate the cached version")
}

// RestoreSnapshot swaps the whole bbolt file; cached versions from the old
// file must not survive into the restored state.
func TestResolveType_RestoreSnapshotInvalidates(t *testing.T) {
	src := NewStorage(t, nil)
	defer src.Cleanup()

	srcType := &cluster.Type{ID: "events", Name: "FromSnapshot", Version: 1}
	insertTypes(t, src, []*cluster.Type{srcType}, 1, 1)
	snap, err := src.CreateSnapshot(src.AppliedIndex(), &pb.ConfState{})
	require.NoError(t, err)

	dst := NewStorage(t, nil)
	defer dst.Cleanup()
	dstType := &cluster.Type{ID: "events", Name: "PreRestore", Version: 1}
	insertTypes(t, dst, []*cluster.Type{dstType}, 1, 1)

	got, err := dst.ResolveType(cluster.TypeRefAt("events", 1))
	require.NoError(t, err)
	require.Equal(t, "PreRestore", got.Name)

	require.NoError(t, dst.RestoreSnapshot(snap))

	got, err = dst.ResolveType(cluster.TypeRefAt("events", 1))
	require.NoError(t, err)
	require.Equal(t, "FromSnapshot", got.Name, "restore must invalidate versions cached from the replaced bbolt")
}
