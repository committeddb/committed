package db_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/internal/cluster/migration"
	synchttp "github.com/committeddb/committed/internal/cluster/syncable/http"
)

// newWalDBWithHTTPSyncables is newWalDB with the webhook syncable parser
// registered, so tests can POST real always-current syncables without a
// destination database.
func newWalDBWithHTTPSyncables(t *testing.T) (*db.DB, *wal.Storage) {
	t.Helper()
	dir := t.TempDir()
	p := parser.New()
	p.AddSyncableParser("http", &synchttp.SyncableParser{})
	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, nil, nil, db.WithTickInterval(testTickInterval))
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })
	return d, s
}

const ncSchemaV1 = `{"type":"object","properties":{"a":{"type":"string"}}}`

const ncSchemaV2 = `{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"string"}},"required":["b"]}`

func ncTypeTOML(schema, migrationSection string) []byte {
	return fmt.Appendf(nil, "[type]\nname = \"orders\"\nschemaType = \"JSONSchema\"\nschema = '%s'\n%s", schema, migrationSection)
}

func ncSyncableTOML(mode string) string {
	return fmt.Sprintf("[syncable]\nname = \"orders-hook\"\ntype = \"http\"%s\n\n[http]\ntopic = \"orders\"\nurl = \"http://sink.internal/hook\"\n", mode)
}

// TestNonConvertible_IntentDeclaration pins the [migration] vocabulary rules:
// exactly one intent; nonConvertible is meaningless on a first version or
// without a schema change; once declared it is immutable and must be restated
// on in-place edits; and a declared break persists on the version record.
func TestNonConvertible_IntentDeclaration(t *testing.T) {
	d, s := newWalDB(t)

	// Two intents at once → refused.
	err := d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "orders", MimeType: "text/toml",
		Data: ncTypeTOML(ncSchemaV1, "\n[migration]\nnone = true\nnonConvertible = true\n"),
	})
	require.ErrorContains(t, err, "exactly one intent")

	// A first version cannot declare a break.
	err = d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "orders", MimeType: "text/toml",
		Data: ncTypeTOML(ncSchemaV1, "\n[migration]\nnonConvertible = true\n"),
	})
	require.ErrorContains(t, err, "first version")

	// v1 plain, then a nonConvertible v2 admits (no always-current consumers)
	// and the flag lands on the v2 record only.
	require.NoError(t, d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "orders", MimeType: "text/toml", Data: ncTypeTOML(ncSchemaV1, ""),
	}))
	require.NoError(t, d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "orders", MimeType: "text/toml",
		Data: ncTypeTOML(ncSchemaV2, "\n[migration]\nnonConvertible = true\n"),
	}))
	v2, err := s.ResolveType(cluster.TypeRefAt("orders", 2))
	require.NoError(t, err)
	require.True(t, v2.NonConvertible)
	v1, err := s.ResolveType(cluster.TypeRefAt("orders", 1))
	require.NoError(t, err)
	require.False(t, v1.NonConvertible)

	// An identical restatement is a no-op; an in-place edit that DROPS the
	// declaration is refused (the intent is immutable per version).
	require.NoError(t, d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "orders", MimeType: "text/toml",
		Data: ncTypeTOML(ncSchemaV2, "\n[migration]\nnonConvertible = true\n"),
	}))
	err = d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "orders", MimeType: "text/toml", Data: ncTypeTOML(ncSchemaV2, ""),
	})
	require.ErrorContains(t, err, "restate nonConvertible")

	// Declaring a break retroactively (no schema change) is refused.
	require.NoError(t, d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "invoices", MimeType: "text/toml",
		Data: []byte("[type]\nname = \"invoices\"\nschemaType = \"JSONSchema\"\nschema = '" + ncSchemaV1 + "'"),
	}))
	err = d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "invoices", MimeType: "text/toml",
		Data: []byte("[type]\nname = \"invoices\"\nschemaType = \"JSONSchema\"\nschema = '" + ncSchemaV1 + "'\n[migration]\nnonConvertible = true\n"),
	})
	require.ErrorContains(t, err, "retroactively")
}

// TestNonConvertible_StrandingRequiresForce pins the sign-off decision: a
// nonConvertible bump that strands a standing always-current syncable is
// refused with the syncable NAMED, and only the explicit acknowledgment
// (?force=true → AcknowledgeStrandedSyncables) admits it.
func TestNonConvertible_StrandingRequiresForce(t *testing.T) {
	d, _ := newWalDBWithHTTPSyncables(t)

	require.NoError(t, d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "orders", MimeType: "text/toml", Data: ncTypeTOML(ncSchemaV1, ""),
	}))
	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "orders-hook", MimeType: "text/toml",
		Data: []byte(ncSyncableTOML("\nmode = \"always-current\"")),
	}))

	bump := &cluster.Configuration{
		ID: "orders", MimeType: "text/toml",
		Data: ncTypeTOML(ncSchemaV2, "\n[migration]\nnonConvertible = true\n"),
	}
	err := d.ProposeType(testCtx(t), bump)
	var stranded *cluster.StrandedSyncablesError
	require.ErrorAs(t, err, &stranded)
	require.Equal(t, []string{"orders-hook"}, stranded.Syncables, "the refusal names each stranded syncable")
	require.Equal(t, 2, stranded.Version)

	require.NoError(t, d.ProposeType(testCtx(t), bump, cluster.AcknowledgeStrandedSyncables()),
		"the acknowledgment admits the same bump")
}

// TestNonConvertible_AlwaysCurrentAdmissionRefused pins the other direction:
// once a break is declared, a NEW always-current syncable over the topic is
// refused at POST with the gap named, while as-stored admits fine.
func TestNonConvertible_AlwaysCurrentAdmissionRefused(t *testing.T) {
	d, _ := newWalDBWithHTTPSyncables(t)

	require.NoError(t, d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "orders", MimeType: "text/toml", Data: ncTypeTOML(ncSchemaV1, ""),
	}))
	require.NoError(t, d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "orders", MimeType: "text/toml",
		Data: ncTypeTOML(ncSchemaV2, "\n[migration]\nnonConvertible = true\n"),
	}))

	err := d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "orders-hook", MimeType: "text/toml",
		Data: []byte(ncSyncableTOML("\nmode = \"always-current\"")),
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "nonConvertible")
	require.ErrorContains(t, err, "version 2", "the refusal names the gap")

	// as-stored over the same topic admits — version handling is the
	// consumer's, which a break doesn't invalidate.
	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "orders-hook", MimeType: "text/toml",
		Data: []byte(ncSyncableTOML("")),
	}))
}

// TestNonConvertible_ForceStrandingIsNotSilent double-checks the runtime
// floor referenced by the force warning: the migration chain refuses to cross
// a declared break (see migration.Chain) rather than silently delivering
// unconverted data. Covered structurally here via the chain's own unit test;
// this asserts the error's classification survives the db seam.
func TestNonConvertible_ForceStrandingIsNotSilent(t *testing.T) {
	d, s := newWalDBWithHTTPSyncables(t)
	require.NoError(t, d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "orders", MimeType: "text/toml", Data: ncTypeTOML(ncSchemaV1, ""),
	}))
	require.NoError(t, d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "orders", MimeType: "text/toml",
		Data: ncTypeTOML(ncSchemaV2, "\n[migration]\nnonConvertible = true\n"),
	}))

	_, err := migration.Chain(context.Background(), s, "orders", 1, 2, []byte(`{"a":"x"}`))
	require.Error(t, err)
	require.ErrorContains(t, err, "nonConvertible")
	var merr *migration.Error
	require.ErrorAs(t, err, &merr, "the break reports as a migration step error → dead-letter machinery, replayable after an erratum heals the reading")
	require.Equal(t, 2, merr.ToVersion)
}
