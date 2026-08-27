package migration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/migration"
)

// okSync accepts everything — the wiring tests below only exercise the
// classification of Chain failures, which short-circuit before inner.
type okSync struct{}

func (okSync) Sync(context.Context, *cluster.Actual) (cluster.ShouldSnapshot, error) {
	return true, nil
}
func (okSync) Close() error { return nil }

// downcaseResolver serves a person type whose v1→v2 program compiles (so it
// passes compileMigration's registration-time validation) but fails at
// runtime on any row whose contact is not a string — the
// compiles-but-fails-every-row shape registration-time validation cannot
// catch.
func downcaseResolver() *stubResolver {
	return &stubResolver{types: map[string]*cluster.Type{
		"person":   {ID: "person", Name: "Person", Version: 2},
		"person@2": {ID: "person", Version: 2, Migration: []byte(`. + {email: (.contact | ascii_downcase)}`)},
	}}
}

// TestSyncable_ChainRunWedgesWhenConfigShaped pins the site-level ambiguity
// wiring for the migration chain: a program that fails a run of consecutive
// distinct rows with no success is config-shaped — the chain's tracker flips
// the classification from Permanent (dead-letter) to transient (wedge), and
// a retry of the wedged row stays wedged.
func TestSyncable_ChainRunWedgesWhenConfigShaped(t *testing.T) {
	tp1 := &cluster.Type{ID: "person", Name: "Person", Version: 1}
	s := migration.Wrap(okSync{}, downcaseResolver(), nil)
	ctx := context.Background()

	bad := func(i int) *cluster.Actual {
		return &cluster.Actual{Index: uint64(i), Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(tp1, fmt.Appendf(nil, "k%02d", i), []byte(`{"contact": 5}`)),
		}}
	}
	for i := 1; i < cluster.AmbiguityEvidenceThreshold; i++ {
		_, err := s.Sync(ctx, bad(i))
		require.ErrorIs(t, err, cluster.ErrPermanent, "failure %d may still be entry-specific → dead-letter", i)
		require.NotErrorIs(t, err, cluster.ErrConfigShaped)
	}
	_, err := s.Sync(ctx, bad(cluster.AmbiguityEvidenceThreshold))
	require.ErrorIs(t, err, cluster.ErrConfigShaped, "the threshold-th distinct row establishes the program config-shaped")
	require.NotErrorIs(t, err, cluster.ErrPermanent, "config-shaped must wedge, not dead-letter")

	_, err = s.Sync(ctx, bad(cluster.AmbiguityEvidenceThreshold))
	require.ErrorIs(t, err, cluster.ErrConfigShaped, "retrying the wedged row must stay wedged")
}

// TestSyncable_ChainSuccessKeepsEntrySpecific: rows the program transforms
// fine reset the chain's evidence, so scattered genuine per-row failures
// keep dead-lettering — they never wedge the worker.
func TestSyncable_ChainSuccessKeepsEntrySpecific(t *testing.T) {
	tp1 := &cluster.Type{ID: "person", Name: "Person", Version: 1}
	s := migration.Wrap(okSync{}, downcaseResolver(), nil)
	ctx := context.Background()

	for i := 1; i <= 3*cluster.AmbiguityEvidenceThreshold; i++ {
		data := `{"contact": "ALICE@EXAMPLE.COM"}`
		if i%2 == 0 {
			data = `{"contact": 5}` // fails ascii_downcase — genuinely bad row
		}
		_, err := s.Sync(ctx, &cluster.Actual{Index: uint64(i), Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(tp1, fmt.Appendf(nil, "k%02d", i), []byte(data)),
		}})
		if i%2 == 0 {
			require.ErrorIs(t, err, cluster.ErrPermanent, "isolated bad rows keep dead-lettering (row %d)", i)
			require.NotErrorIs(t, err, cluster.ErrConfigShaped)
		} else {
			require.NoError(t, err)
		}
	}
}
