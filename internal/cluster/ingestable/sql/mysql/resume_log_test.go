package mysql

import (
	"context"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// observeGlobalLogs routes zap's global logger into an observer for the test's
// duration — the resume/rotation logs go through zap.L(), matching the rest of
// the mysql dialect's logging.
func observeGlobalLogs(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zap.InfoLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	t.Cleanup(restore)
	return logs
}

// The resume-time positioning log is the capture-time log's twin: every binlog
// (re)connect must state HOW the stream positioned and FROM WHERE. The GTID
// saga's incident was undiagnosable for lack of exactly this line — the resume
// coordinates lived only as protobuf in bbolt.
func TestLogResumePositioning_GTID(t *testing.T) {
	logs := observeGlobalLogs(t)

	set, err := mysql.ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-4")
	require.NoError(t, err)
	logResumePositioning(true, set, &mysql.Position{Name: "binlog.000014", Pos: 5475})

	entries := logs.FilterMessageSnippet("started by GTID positioning").All()
	require.Len(t, entries, 1, "GTID resume must log the positioning line")
	require.Equal(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-4",
		entries[0].ContextMap()["resumeGtidSet"],
		"the line must carry the exact resume set")
}

func TestLogResumePositioning_FilePos(t *testing.T) {
	logs := observeGlobalLogs(t)

	logResumePositioning(false, nil, &mysql.Position{Name: "binlog.000014", Pos: 5475})

	entries := logs.FilterMessageSnippet("started by binlog file:pos positioning").All()
	require.Len(t, entries, 1, "file:pos resume must log the positioning line")
	require.Equal(t, "binlog.000014", entries[0].ContextMap()["resumeFile"])
	require.EqualValues(t, 5475, entries[0].ContextMap()["resumePos"])
}

// A real rotation logs from→to — and at connect, a fake rotate naming a
// DIFFERENT file than the resume seed takes this same branch, so a server
// starting the dump at an old binlog (the rewind signature the restart
// re-delivery incident needed) is one visible line, not an unexplained
// replay. The same-file fake rotate every stream opens with stays silent.
func TestDispatchEvent_LogsRealRotation(t *testing.T) {
	logs := observeGlobalLogs(t)
	h := &MySQLEventHandler{curFile: "binlog.000014"}

	// Start-of-stream fake rotate restating the current file: skipped, silent.
	fake := &replication.RotateEvent{NextLogName: []byte("binlog.000014")}
	require.NoError(t, h.dispatchEvent(context.Background(),
		&replication.EventHeader{Timestamp: 0}, fake))
	require.Empty(t, logs.FilterMessageSnippet("rotated").All(),
		"a same-file fake rotate must stay silent")
	require.Equal(t, "binlog.000014", h.curFile)

	// Fake rotate naming an OLDER file — the rewind signature at connect.
	rewind := &replication.RotateEvent{NextLogName: []byte("binlog.000001")}
	require.NoError(t, h.dispatchEvent(context.Background(),
		&replication.EventHeader{Timestamp: 0}, rewind))

	entries := logs.FilterMessageSnippet("rotated").All()
	require.Len(t, entries, 1, "a file-changing rotate must log")
	require.Equal(t, "binlog.000014", entries[0].ContextMap()["from"],
		"from must be the pre-rotate file so the rewind is readable")
	require.Equal(t, "binlog.000001", entries[0].ContextMap()["to"])
	require.Equal(t, "binlog.000001", h.curFile, "curFile must still advance")
}
