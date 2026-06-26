package mysql

import (
	"context"
	"log/slog"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// syncerSlogHandler adapts the *slog.Logger that go-mysql's BinlogSyncer expects
// onto our zap logger, so MySQL CDC logs land in the same structured stream as
// the rest of committed instead of going straight to stdout via slog's default
// handler.
//
// Level policy: the binlog syncer is chatty at info/debug (binlog rotation,
// position sync), so Debug and Info demote to zap Debug — suppressed at the
// default production (info) level, but recoverable by dropping zap to debug. Warn
// and Error keep their level. slog has no Fatal/Panic, so the old "a third-party
// log call must never take the node down" rule is now structural rather than
// policy.
type syncerSlogHandler struct {
	z *zap.Logger
}

// newSyncerLogger wraps z under a "binlog" name so MySQL CDC log lines are
// attributable in the unified stream.
func newSyncerLogger(z *zap.Logger) *slog.Logger {
	return slog.New(&syncerSlogHandler{z: z.Named("binlog")})
}

// zapLevel applies the level policy. slog levels are ordered integers, so >=
// comparisons also catch custom levels between the named ones.
func zapLevel(l slog.Level) zapcore.Level {
	switch {
	case l >= slog.LevelError:
		return zapcore.ErrorLevel
	case l >= slog.LevelWarn:
		return zapcore.WarnLevel
	default:
		// Info and Debug both demote to Debug (see level policy).
		return zapcore.DebugLevel
	}
}

func (h *syncerSlogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return h.z.Core().Enabled(zapLevel(level))
}

func (h *syncerSlogHandler) Handle(_ context.Context, r slog.Record) error {
	ce := h.z.Check(zapLevel(r.Level), r.Message)
	if ce == nil {
		return nil
	}
	fields := make([]zap.Field, 0, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		fields = append(fields, zap.Any(a.Key, a.Value.Any()))
		return true
	})
	ce.Write(fields...)
	return nil
}

func (h *syncerSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	fields := make([]zap.Field, 0, len(attrs))
	for _, a := range attrs {
		fields = append(fields, zap.Any(a.Key, a.Value.Any()))
	}
	return &syncerSlogHandler{z: h.z.With(fields...)}
}

func (h *syncerSlogHandler) WithGroup(name string) slog.Handler {
	// zap.Namespace nests every subsequent field under name, which is
	// slog's group contract.
	return &syncerSlogHandler{z: h.z.With(zap.Namespace(name))}
}
