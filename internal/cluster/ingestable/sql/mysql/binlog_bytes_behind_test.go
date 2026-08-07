package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// binlogBytesBehind is the file:pos lag fallback's arithmetic: consumed
// file's remaining bytes plus every later file in full (the last file's size
// is the write head). Pure, so every shape pins without a server.
func TestBinlogBytesBehind(t *testing.T) {
	inv := []binlogFile{
		{name: "binlog.000001", size: 1000},
		{name: "binlog.000002", size: 800},
		{name: "binlog.000003", size: 600},
	}

	t.Run("same file", func(t *testing.T) {
		got, purged, ok := binlogBytesBehind(inv, "binlog.000003", 200)
		require.True(t, ok)
		require.False(t, purged)
		require.EqualValues(t, 400, got)
	})

	t.Run("multi file", func(t *testing.T) {
		got, purged, ok := binlogBytesBehind(inv, "binlog.000001", 900)
		require.True(t, ok)
		require.False(t, purged)
		require.EqualValues(t, 100+800+600, got)
	})

	t.Run("caught up exactly", func(t *testing.T) {
		got, purged, ok := binlogBytesBehind(inv, "binlog.000003", 600)
		require.True(t, ok)
		require.False(t, purged)
		require.Zero(t, got)
	})

	t.Run("stale inventory clamps to zero", func(t *testing.T) {
		// A commit can land between the checkpoint read and the inventory
		// read, putting the consumed pos past the listed size — noise, not a
		// negative lag.
		got, purged, ok := binlogBytesBehind(inv, "binlog.000003", 700)
		require.True(t, ok)
		require.False(t, purged)
		require.Zero(t, got)
	})

	t.Run("consumed file purged", func(t *testing.T) {
		later := []binlogFile{{name: "binlog.000003", size: 600}}
		_, purged, ok := binlogBytesBehind(later, "binlog.000001", 900)
		require.True(t, purged, "a consumed file missing from the inventory is the purge hole")
		require.False(t, ok)
	})

	t.Run("lineage reset reads as purged", func(t *testing.T) {
		// RESET MASTER / a rebuilt server restarts numbering below the
		// checkpoint: the consumed file is gone and re-snapshot is the answer,
		// same as a purge.
		early := []binlogFile{{name: "binlog.000001", size: 100}}
		_, purged, _ := binlogBytesBehind(early, "binlog.000014", 4)
		require.True(t, purged)
	})

	t.Run("unparsable consumed name", func(t *testing.T) {
		_, _, ok := binlogBytesBehind(inv, "weird-name", 0)
		require.False(t, ok)
	})

	t.Run("unparsable inventory name", func(t *testing.T) {
		bad := []binlogFile{{name: "no-suffix", size: 100}}
		_, _, ok := binlogBytesBehind(bad, "binlog.000001", 0)
		require.False(t, ok)
	})

	t.Run("empty inventory", func(t *testing.T) {
		_, _, ok := binlogBytesBehind(nil, "binlog.000001", 0)
		require.False(t, ok)
	})

	t.Run("encrypted file in range poisons the arithmetic", func(t *testing.T) {
		// binlog_encryption=ON: on-disk sizes include the encryption header,
		// so they no longer equal logical stream offsets — a nil lag is
		// honest, a small constant lie that pins caughtUp false is not.
		enc := []binlogFile{
			{name: "binlog.000001", size: 1000},
			{name: "binlog.000002", size: 800, encrypted: true},
		}
		_, purged, ok := binlogBytesBehind(enc, "binlog.000001", 900)
		require.False(t, ok)
		require.False(t, purged)
	})

	t.Run("encrypted file below the consumed range is irrelevant", func(t *testing.T) {
		enc := []binlogFile{
			{name: "binlog.000001", size: 1000, encrypted: true},
			{name: "binlog.000002", size: 800},
		}
		got, purged, ok := binlogBytesBehind(enc, "binlog.000002", 300)
		require.True(t, ok)
		require.False(t, purged)
		require.EqualValues(t, 500, got)
	})
}
