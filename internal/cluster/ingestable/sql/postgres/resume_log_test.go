package postgres

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// The resume-time positioning line, postgres side: every (re)connect states
// what the stream resumed FROM — the first datum a re-delivery incident
// needs, previously visible only by decoding the bbolt checkpoint out of a
// backup. Mirrors the MySQL dialect's resume log.
func TestLogResumePositioning_CheckpointedLSN(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	t.Cleanup(restore)

	logResumePositioning("committed_slot_orders", pglogrepl.LSN(0x1573178))

	entries := logs.FilterMessageSnippet("started from checkpointed position").All()
	require.Len(t, entries, 1)
	require.Equal(t, "committed_slot_orders", entries[0].ContextMap()["slot"])
	require.Equal(t, "0/1573178", entries[0].ContextMap()["resumeLSN"],
		"the line must carry the exact resume LSN")
}

// LSN 0 delegates the start point to the slot's confirmed position — the log
// must say that instead of printing a misleading 0/0.
func TestLogResumePositioning_SlotConfirmedPosition(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	t.Cleanup(restore)

	logResumePositioning("committed_slot_orders", 0)

	entries := logs.FilterMessageSnippet("slot's confirmed position").All()
	require.Len(t, entries, 1)
	require.Equal(t, "committed_slot_orders", entries[0].ContextMap()["slot"])
	require.NotContains(t, entries[0].ContextMap(), "resumeLSN")
}
