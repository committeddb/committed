package db_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/migration"
)

// proposeTypeWithMigration is a variant of proposeTypeTOML that also
// embeds a [migration] section. Used by the always-current end-to-end
// tests so type versions come with an upgrade program.
func proposeTypeWithMigration(t *testing.T, d *db.DB, id, name, schema, jq string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := fmt.Sprintf(
		"[type]\nname = \"%s\"\nschemaType = \"JSONSchema\"\nschema = '%s'\n\n[migration]\ntransform = '%s'\n",
		name, schema, jq,
	)
	cfg := &cluster.Configuration{ID: id, MimeType: "text/toml", Data: []byte(data)}
	require.NoError(t, d.ProposeType(ctx, cfg))
}

// captureSyncable records the proposals it receives. Safe for
// concurrent Sync calls because the DB's sync worker always delivers on
// one goroutine.
type captureSyncable struct {
	mu        sync.Mutex
	received  []*cluster.Proposal
	doneAfter int
	cancel    func()
}

func (c *captureSyncable) Sync(ctx context.Context, p *cluster.Proposal) (cluster.ShouldSnapshot, error) {
	// Ignore proposals that only contain system config entries (types,
	// syncables, etc.). Tests care about user data; the sync worker
	// delivers everything on the log.
	allSystem := true
	for _, e := range p.Entities {
		if !cluster.IsSystem(e.ID) {
			allSystem = false
			break
		}
	}
	if allSystem {
		return cluster.ShouldSnapshot(true), nil
	}

	c.mu.Lock()
	c.received = append(c.received, p)
	done := c.doneAfter > 0 && len(c.received) >= c.doneAfter
	c.mu.Unlock()
	if done {
		c.cancel()
	}
	return cluster.ShouldSnapshot(true), nil
}

func (c *captureSyncable) Close() error { return nil }

func (c *captureSyncable) proposals() []*cluster.Proposal {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*cluster.Proposal, len(c.received))
	copy(out, c.received)
	return out
}

// TestAlwaysCurrentSyncable_MigratesOldProposal is the Phase 2 key
// success criterion: an entity proposed under type v1 is delivered to
// an always-current syncable with its data transformed by the v2
// migration, not as the raw v1 bytes.
func TestAlwaysCurrentSyncable_MigratesOldProposal(t *testing.T) {
	d, s := newWalDB(t)

	// Register "person" v1. No migration on the first version.
	proposeTypeTOML(t, d, "person", "Person", `{"type":"object"}`, "")
	v1, err := s.ResolveType(cluster.LatestTypeRef("person"))
	require.NoError(t, err)
	require.Equal(t, 1, v1.Version)

	// Propose an entity at v1.
	pOld := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type:      v1,
		Key:       []byte("alice"),
		Data:      []byte(`{"name":"alice"}`),
		Timestamp: time.Now().UnixMilli(),
	}}}
	require.NoError(t, d.Propose(testCtx(t), pOld))

	// Evolve to v2: add default "email". The jq program takes the v1
	// shape and produces the v2 shape.
	proposeTypeWithMigration(t, d, "person", "Person",
		`{"type":"object","required":["email"]}`,
		`. + {email: "unknown@example.com"}`)

	// Register an always-current syncable and wait for it to drain. In
	// production this decoration happens in wal.saveSyncable driven by
	// syncable.mode = "always-current"; here we call the wrapper
	// directly so the test doesn't depend on going through the wal
	// config round-trip.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cap := &captureSyncable{doneAfter: 1, cancel: cancel}
	wrapped := migration.Wrap(cap, s)
	require.NoError(t, d.Sync(ctx, "always-current-sync", wrapped))
	<-ctx.Done()

	ps := cap.proposals()
	require.Len(t, ps, 1)
	require.Len(t, ps[0].Entities, 1)
	// Entity type should be upgraded to v2.
	require.Equal(t, 2, ps[0].Entities[0].Type.Version)
	// Data should have been transformed by the jq program.
	var got map[string]any
	require.NoError(t, json.Unmarshal(ps[0].Entities[0].Data, &got))
	require.Equal(t, "alice", got["name"])
	require.Equal(t, "unknown@example.com", got["email"])
}

// TestAsStoredSyncable_DeliversRawBytes is the counterpart: an
// as-stored syncable sees the entity exactly as it was written, even
// when a later type version with a migration is registered.
func TestAsStoredSyncable_DeliversRawBytes(t *testing.T) {
	d, s := newWalDB(t)

	proposeTypeTOML(t, d, "person", "Person", `{"type":"object"}`, "")
	v1, err := s.ResolveType(cluster.LatestTypeRef("person"))
	require.NoError(t, err)

	pOld := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type:      v1,
		Key:       []byte("alice"),
		Data:      []byte(`{"name":"alice"}`),
		Timestamp: time.Now().UnixMilli(),
	}}}
	require.NoError(t, d.Propose(testCtx(t), pOld))

	proposeTypeWithMigration(t, d, "person", "Person",
		`{"type":"object","required":["email"]}`,
		`. + {email: "unknown@example.com"}`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cap := &captureSyncable{doneAfter: 1, cancel: cancel}
	require.NoError(t, d.Sync(ctx, "as-stored-sync", cap))
	<-ctx.Done()

	ps := cap.proposals()
	require.Len(t, ps, 1)
	// As-stored: version is v1, data is the raw bytes, no email field.
	require.Equal(t, 1, ps[0].Entities[0].Type.Version)
	require.Equal(t, `{"name":"alice"}`, string(ps[0].Entities[0].Data))
}
