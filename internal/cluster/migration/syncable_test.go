package migration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/migration"
	"github.com/stretchr/testify/require"
)

// recordingSyncable saves every Proposal it receives.
type recordingSyncable struct {
	received []*cluster.Proposal
}

func (r *recordingSyncable) Sync(ctx context.Context, p *cluster.Proposal) (cluster.ShouldSnapshot, error) {
	r.received = append(r.received, p)
	return cluster.ShouldSnapshot(true), nil
}

func (r *recordingSyncable) Close() error { return nil }

// recordingBatchSyncable is a recordingSyncable that also implements
// BatchSyncable, to verify the wrapper preserves the batch optimization.
type recordingBatchSyncable struct {
	recordingSyncable
	batches [][]*cluster.Proposal
}

func (r *recordingBatchSyncable) SyncBatch(ctx context.Context, ps []*cluster.Proposal) (bool, error) {
	r.batches = append(r.batches, ps)
	return true, nil
}

func TestWrap_MigratesEntityBeforeSync(t *testing.T) {
	r := &stubResolver{types: map[string]*cluster.Type{
		"person@1": {ID: "person", Name: "Person", Version: 1},
		"person@2": {ID: "person", Name: "Person", Version: 2, Migration: []byte(`. + {email: "x@y"}`)},
	}}
	r.types["person"] = r.types["person@2"] // "latest" lookup

	inner := &recordingSyncable{}
	wrapped := migration.Wrap(inner, r)

	p := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: &cluster.Type{ID: "person", Version: 1},
		Key:  []byte("k"),
		Data: []byte(`{"name":"alice"}`),
	}}}
	_, err := wrapped.Sync(context.Background(), p)
	require.NoError(t, err)

	require.Len(t, inner.received, 1)
	require.Equal(t, 2, inner.received[0].Entities[0].Type.Version)

	var got map[string]any
	require.NoError(t, json.Unmarshal(inner.received[0].Entities[0].Data, &got))
	require.Equal(t, "x@y", got["email"])
}

func TestWrap_SkipsWhenAtLatest(t *testing.T) {
	r := &stubResolver{types: map[string]*cluster.Type{
		"person":   {ID: "person", Name: "Person", Version: 1},
		"person@1": {ID: "person", Name: "Person", Version: 1},
	}}

	inner := &recordingSyncable{}
	wrapped := migration.Wrap(inner, r)

	original := []byte(`{"name":"alice"}`)
	p := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: &cluster.Type{ID: "person", Version: 1},
		Key:  []byte("k"),
		Data: original,
	}}}
	_, err := wrapped.Sync(context.Background(), p)
	require.NoError(t, err)

	require.Len(t, inner.received, 1)
	require.Equal(t, original, inner.received[0].Entities[0].Data, "no migration should run when already at latest")
}

func TestWrap_PassesSystemEntitiesThrough(t *testing.T) {
	// IsSystem returns true for typeType.ID, so a Type-upsert proposal
	// should not trigger a resolver lookup.
	r := &stubResolver{types: map[string]*cluster.Type{}} // empty

	inner := &recordingSyncable{}
	wrapped := migration.Wrap(inner, r)

	// Construct a system-type entity directly: typeType lives in the
	// cluster package, so we reach it via NewUpsertTypeEntity.
	e, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "some-id", Name: "Name", Version: 1})
	require.NoError(t, err)

	p := &cluster.Proposal{Entities: []*cluster.Entity{e}}
	_, err = wrapped.Sync(context.Background(), p)
	require.NoError(t, err, "system entities should bypass migration, no resolver lookup")
	require.Len(t, inner.received, 1)
}

func TestWrap_MigrationFailureIsPermanent(t *testing.T) {
	r := &stubResolver{types: map[string]*cluster.Type{
		"person":   {ID: "person", Version: 2},
		"person@2": {ID: "person", Version: 2, Migration: []byte(`this is not jq`)},
	}}

	inner := &recordingSyncable{}
	wrapped := migration.Wrap(inner, r)

	p := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: &cluster.Type{ID: "person", Version: 1},
		Key:  []byte("k"),
		Data: []byte(`{"name":"alice"}`),
	}}}
	_, err := wrapped.Sync(context.Background(), p)
	require.Error(t, err)
	require.True(t, errors.Is(err, cluster.ErrPermanent), "migration failures must be permanent so the worker skips rather than retries")
}

func TestWrap_PreservesBatchInterface(t *testing.T) {
	r := &stubResolver{types: map[string]*cluster.Type{
		"person":   {ID: "person", Version: 2},
		"person@2": {ID: "person", Version: 2, Migration: []byte(`. + {email: "x@y"}`)},
	}}

	inner := &recordingBatchSyncable{}
	wrapped := migration.Wrap(inner, r)

	_, isBatch := wrapped.(cluster.BatchSyncable)
	require.True(t, isBatch, "Wrap of a BatchSyncable must satisfy cluster.BatchSyncable")

	bs := wrapped.(cluster.BatchSyncable)
	ps := []*cluster.Proposal{
		{Entities: []*cluster.Entity{{
			Type: &cluster.Type{ID: "person", Version: 1},
			Key:  []byte("k1"),
			Data: []byte(`{"name":"a"}`),
		}}},
		{Entities: []*cluster.Entity{{
			Type: &cluster.Type{ID: "person", Version: 1},
			Key:  []byte("k2"),
			Data: []byte(`{"name":"b"}`),
		}}},
	}
	_, err := bs.SyncBatch(context.Background(), ps)
	require.NoError(t, err)

	require.Len(t, inner.batches, 1)
	require.Len(t, inner.batches[0], 2)
	for i, p := range inner.batches[0] {
		require.Equal(t, 2, p.Entities[0].Type.Version)
		var got map[string]any
		require.NoError(t, json.Unmarshal(p.Entities[0].Data, &got))
		require.Equal(t, "x@y", got["email"], "batch entry %d", i)
	}
}

// Ensure fmt stays imported for future debug use; tests above don't
// reach it directly but the package keeps it around.
var _ = fmt.Sprintf
