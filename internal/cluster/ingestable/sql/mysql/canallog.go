package mysql

import (
	"context"
	"log/slog"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// canalSlogHandler adapts canal's expected logger (*slog.Logger since
// go-mysql v1.15) onto our zap logger, so MySQL CDC logs land in the
// same structured stream as the rest of committed instead of going
// straight to stdout via slog's default handler.
//
// Level policy (carried over from the previous loggers.Advanced
// adapter): canal is chatty at info/debug (binlog rotation, table
// parsing), so Debug and Info demote to zap Debug — suppressed at the
// default production (info) level, but recoverable by dropping zap to
// debug. Warn and Error keep their level. slog has no Fatal/Panic, so
// the old "a third-party log call must never take the node down" rule
// is now structural rather than policy.
type canalSlogHandler struct {
	z *zap.Logger
}

// newCanalLogger wraps z under a "canal" name so MySQL CDC log lines
// are attributable in the unified stream.
func newCanalLogger(z *zap.Logger) *slog.Logger {
	return slog.New(&canalSlogHandler{z: z.Named("canal")})
}

// zapLevel applies the level policy. slog levels are ordered integers,
// so >= comparisons also catch custom levels between the named ones.
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

func (h *canalSlogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return h.z.Core().Enabled(zapLevel(level))
}

func (h *canalSlogHandler) Handle(_ context.Context, r slog.Record) error {
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

func (h *canalSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	fields := make([]zap.Field, 0, len(attrs))
	for _, a := range attrs {
		fields = append(fields, zap.Any(a.Key, a.Value.Any()))
	}
	return &canalSlogHandler{z: h.z.With(fields...)}
}

func (h *canalSlogHandler) WithGroup(name string) slog.Handler {
	// zap.Namespace nests every subsequent field under name, which is
	// slog's group contract.
	return &canalSlogHandler{z: h.z.With(zap.Namespace(name))}
}
