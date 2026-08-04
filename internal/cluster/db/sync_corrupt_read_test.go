package db

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/committeddb/committed/internal/cluster"
)

// panicOnFatal makes logger.Fatal panic instead of calling os.Exit, so a test
// can assert whether a fatal fired.
type panicOnFatal struct{}

func (panicOnFatal) OnWrite(*zapcore.CheckedEntry, []zapcore.Field) {
	panic("logger.Fatal called")
}

// observedDB returns a DB whose logger records entries for assertion and
// panics (instead of os.Exit) if anything logs at Fatal.
func observedDB() (*DB, *observer.ObservedLogs) {
	core, logs := observer.New(zapcore.DebugLevel)
	return &DB{logger: zap.New(core, zap.WithFatalHook(panicOnFatal{}))}, logs
}

// TestCorruptEntryReadWedgesInsteadOfFatal pins the corruption posture — loud,
// alive, repairable — on the CRC flavor: a corrupt (checksum-failed) committed
// event on a sync read must NOT fatal-exit the node. Before 0.7.6 this branch
// fataled, which shares the crashloop shape of the post-scrub hollow-segment
// incident when the corrupt entry sits near a checkpoint: the node dies on the
// worker's first read after every boot, with no API window for the operator.
// Instead the syncable wedges (Reader.Read never advances past the corrupt
// entry, so nothing is skipped and the checkpoint holds) and the node stays up
// as the operator's debugging instrument. The log line must carry the repair
// guidance at Error level — that guidance is the operator's runbook.
func TestCorruptEntryReadWedgesInsteadOfFatal(t *testing.T) {
	d, logs := observedDB()

	// Wrapped exactly as Reader.Read wraps it; the policy must hold through
	// the wrap.
	corrupt := fmt.Errorf("event log read seq 5: %w", cluster.ErrCorruptEntry)
	require.NotPanics(t, func() { d.logSyncReadError("s1", corrupt) },
		"a corrupt read must wedge loudly, never fatal-exit the node")

	entries := logs.FilterLevelExact(zapcore.ErrorLevel).All()
	require.Len(t, entries, 1, "the CRC flavor must log at Error level")
	require.Contains(t, entries[0].Message, "wal repair",
		"the Error log must carry the repair guidance")
	require.Contains(t, entries[0].Message, "wedged",
		"the Error log must say the syncable is wedged, not skipped")
	require.Equal(t, "s1", entries[0].ContextMap()["id"],
		"the wedged syncable must be identified")
}

// A transient read error stays a Warn — no fatal, no Error-level escalation.
func TestTransientReadErrorWarns(t *testing.T) {
	d, logs := observedDB()

	require.NotPanics(t, func() { d.logSyncReadError("s1", errors.New("transient read glitch")) })

	require.Empty(t, logs.FilterLevelExact(zapcore.ErrorLevel).All(),
		"a transient error must not take the corruption branch")
	warns := logs.FilterLevelExact(zapcore.WarnLevel).All()
	require.Len(t, warns, 1)
	require.Equal(t, "sync read error", warns[0].Message)
}
