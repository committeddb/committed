package tidwall

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	native "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestLegacyLogRetiresCapabilitiesTogether(t *testing.T) {
	path := t.TempDir()
	owner, err := OpenLegacy(path, LegacyOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = owner.Close() })
	codec := LegacyCodec{
		Encode: func(r eventlog.Record) ([]byte, error) {
			return binary.BigEndian.AppendUint64(nil, r.ID), nil
		},
		Decode: func(raw []byte) (eventlog.Record, error) {
			if len(raw) != 8 {
				return eventlog.Record{}, eventlog.ErrCorrupt
			}
			return eventlog.Record{ID: binary.BigEndian.Uint64(raw)}, nil
		},
	}
	appender := owner.NewAppender(codec)
	require.NoError(t, appender.Append([]eventlog.Record{{ID: 10}, {ID: 30}}))
	decodes := 0
	var decoded *eventlog.Record
	cursor := NewLegacyLogCursor(owner, func(raw []byte) (uint64, *eventlog.Record, error) {
		decodes++
		r, err := codec.Decode(raw)
		decoded = &r
		return r.ID, decoded, err
	})
	defer func() { _ = cursor.Close() }()
	got, err := cursor.Seek(10)
	require.NoError(t, err)
	require.Same(t, decoded, got, "positioning must preserve the decoded object")
	require.Equal(t, 1, decodes)
	transfer := owner.Transfer(func(raw []byte) ([]byte, error) { return raw, nil })
	layout, err := transfer.Layout()
	require.NoError(t, err)
	require.Equal(t, uint64(2), layout.LastSeq)

	require.NoError(t, owner.Close())
	replacement, err := OpenLegacy(path, LegacyOptions{})
	require.NoError(t, err)
	defer func() { _ = replacement.Close() }()
	require.NoError(t, replacement.NewAppender(codec).Append([]eventlog.Record{{ID: 50}}))

	// Existing capabilities stay attached to the retired handle, even after
	// the same directory is reopened and appended to by its replacement.
	require.ErrorIs(t, appender.Append([]eventlog.Record{{ID: 70}}), native.ErrClosed)
	_, err = cursor.Seek(30)
	require.ErrorIs(t, err, native.ErrClosed)
	_, err = transfer.Read(1)
	require.ErrorIs(t, err, native.ErrClosed)
	_, err = transfer.Layout()
	require.ErrorIs(t, err, native.ErrClosed)
	_, err = owner.CompressNextSealed()
	require.ErrorIs(t, err, eventlog.ErrClosed)
	last, ok, err := replacement.NewAppender(codec).LastAppended()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(50), last)
}
