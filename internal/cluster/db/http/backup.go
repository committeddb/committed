package http

import (
	"errors"
	"fmt"
	"io"
	httpgo "net/http"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster/db"
)

// backupWriteStall bounds each write of a streaming backup: a receiver that
// stops reading would otherwise hold the node's log layouts frozen — and
// with them compression and compaction — for as long as it liked. A bound
// on progress, not on size: a slow link still completes a large archive.
const backupWriteStall = 60 * time.Second

// NodeBackup serves GET /node/backup: a backup archive of THIS node's state,
// taken live, as a tar stream — the same archive `committed backup` takes
// from a stopped node, restored by the same `committed restore`. The
// manifest travels last, as in the offline archive; a stream that ends
// without it was cut short (a receiver stall, or the node's own maintenance
// overtaking the read, which it detects and reports instead of finishing an
// inconsistent archive) and must be taken again. One backup streams at a
// time per node (409 otherwise). `committed backup --live` is the client.
func (h *HTTP) NodeBackup(w httpgo.ResponseWriter, r *httpgo.Request) {
	rc := httpgo.NewResponseController(w)
	arm := func() error {
		err := rc.SetWriteDeadline(time.Now().Add(backupWriteStall))
		if errors.Is(err, httpgo.ErrNotSupported) {
			return nil
		}
		return err
	}
	if err := arm(); err != nil {
		writeInternalError(w, "backup: write deadline", err)
		return
	}
	body := &backupStream{w: w, arm: arm, headers: func() {
		w.Header().Set("Content-Type", "application/x-tar")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="committed-node%d-%s.tar"`, h.db.ID(), time.Now().UTC().Format("20060102T150405Z")))
	}}
	m, err := h.db.LiveBackup(body, time.Now())
	if err != nil {
		switch {
		case body.began:
			// The status is out. The archive carries an ABORTED.json entry
			// naming the reason (backup.CreateLive), so the client can say
			// why; a stream the write itself cut ends without one.
			zap.L().Warn("live backup aborted after it began streaming", zap.Error(err))
		case errors.Is(err, db.ErrLiveBackupBusy):
			writeError(w, httpgo.StatusConflict, "backup_in_progress", "a live backup of this node is already in progress; one streams at a time")
		case errors.Is(err, db.ErrLiveBackupCatchingUp):
			w.Header().Set("Retry-After", "60")
			writeError(w, httpgo.StatusServiceUnavailable, "catching_up", "this node is catching up from a peer (see catchingUp on /v1/node/status); take the backup once it has caught up")
		case errors.Is(err, db.ErrLiveBackupUnsupported):
			writeError(w, httpgo.StatusNotImplemented, "backup_unsupported", "this node's storage cannot be backed up live")
		default:
			writeInternalError(w, "live backup failed", err)
		}
		return
	}
	zap.L().Info("live backup served", zap.Int("files", len(m.Files)), zap.Uint64("appliedIndex", m.AppliedIndex), zap.Uint64("eventLogGeneration", m.EventLogGeneration))
}

// backupStream is the archive's writer: the archive's headers and the
// status go out with the first byte (an error before it answers as JSON),
// and every write re-arms the stall deadline.
type backupStream struct {
	w       io.Writer
	arm     func() error
	headers func()
	began   bool
}

func (b *backupStream) Write(p []byte) (int, error) {
	if err := b.arm(); err != nil {
		return 0, err
	}
	if !b.began {
		b.headers()
		b.began = true
	}
	return b.w.Write(p)
}
