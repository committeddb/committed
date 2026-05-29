package mysql

import (
	"fmt"
	"strings"

	"github.com/siddontang/go-log/loggers"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// canalLogger adapts canal's expected logger (siddontang/go-log's
// loggers.Advanced) onto our zap logger, so MySQL CDC logs land in the
// same structured stream as the rest of committed instead of being
// written straight to stdout by siddontang/go-log.
//
// Level policy: canal is chatty at info/debug (binlog rotation, table
// parsing). The previous code pinned siddontang's logger to LevelError
// to keep that quiet, so we preserve the intent by demoting canal's
// Debug/Info/Print to zap Debug — suppressed at the default production
// (info) level, but recoverable by dropping zap to debug. Warn and Error
// keep their level. Fatal and Panic map to Error rather than
// exiting/panicking the process: a third-party library's log call must
// never take the node down.
//
// Note: this only removes our *direct construction* of a stdout logger,
// not the module dependency. siddontang/go-log cannot leave go.mod —
// go-mysql/canal depends on it transitively and types Config.Logger as
// loggers.Advanced — and it stays a *direct* require because this file
// imports its loggers package to implement that interface.
type canalLogger struct {
	z *zap.Logger
}

// newCanalLogger wraps z under a "canal" name so MySQL CDC log lines are
// attributable in the unified stream.
func newCanalLogger(z *zap.Logger) *canalLogger {
	return &canalLogger{z: z.Named("canal")}
}

// Compile-time check that we satisfy the interface canal's Config.Logger
// expects; if go-mysql changes the contract this fails to build rather
// than silently dropping canal's logs.
var _ loggers.Advanced = (*canalLogger)(nil)

// at writes msg at lvl, skipping formatting work when the level is
// disabled.
func (l *canalLogger) at(lvl zapcore.Level, msg string) {
	if ce := l.z.Check(lvl, msg); ce != nil {
		ce.Write()
	}
}

// sprintln mirrors fmt.Sprintln but strips the trailing newline zap adds
// its own line framing.
func sprintln(args ...any) string {
	return strings.TrimSuffix(fmt.Sprintln(args...), "\n")
}

// Debug/Info/Print are demoted to zap Debug (see level policy above).
func (l *canalLogger) Debug(args ...any) {
	l.at(zapcore.DebugLevel, fmt.Sprint(args...))
}

func (l *canalLogger) Debugf(f string, args ...any) {
	l.at(zapcore.DebugLevel, fmt.Sprintf(f, args...))
}

func (l *canalLogger) Debugln(args ...any) {
	l.at(zapcore.DebugLevel, sprintln(args...))
}

func (l *canalLogger) Info(args ...any) {
	l.at(zapcore.DebugLevel, fmt.Sprint(args...))
}

func (l *canalLogger) Infof(f string, args ...any) {
	l.at(zapcore.DebugLevel, fmt.Sprintf(f, args...))
}

func (l *canalLogger) Infoln(args ...any) {
	l.at(zapcore.DebugLevel, sprintln(args...))
}

func (l *canalLogger) Print(args ...any) {
	l.at(zapcore.DebugLevel, fmt.Sprint(args...))
}

func (l *canalLogger) Printf(f string, args ...any) {
	l.at(zapcore.DebugLevel, fmt.Sprintf(f, args...))
}

func (l *canalLogger) Println(args ...any) {
	l.at(zapcore.DebugLevel, sprintln(args...))
}

func (l *canalLogger) Warn(args ...any) {
	l.at(zapcore.WarnLevel, fmt.Sprint(args...))
}

func (l *canalLogger) Warnf(f string, args ...any) {
	l.at(zapcore.WarnLevel, fmt.Sprintf(f, args...))
}

func (l *canalLogger) Warnln(args ...any) {
	l.at(zapcore.WarnLevel, sprintln(args...))
}

func (l *canalLogger) Error(args ...any) {
	l.at(zapcore.ErrorLevel, fmt.Sprint(args...))
}

func (l *canalLogger) Errorf(f string, args ...any) {
	l.at(zapcore.ErrorLevel, fmt.Sprintf(f, args...))
}

func (l *canalLogger) Errorln(args ...any) {
	l.at(zapcore.ErrorLevel, sprintln(args...))
}

// Fatal/Panic are logged at Error and deliberately do NOT exit or panic
// — a library log call must not bring the node down.
func (l *canalLogger) Fatal(args ...any) {
	l.at(zapcore.ErrorLevel, fmt.Sprint(args...))
}

func (l *canalLogger) Fatalf(f string, args ...any) {
	l.at(zapcore.ErrorLevel, fmt.Sprintf(f, args...))
}

func (l *canalLogger) Fatalln(args ...any) {
	l.at(zapcore.ErrorLevel, sprintln(args...))
}

func (l *canalLogger) Panic(args ...any) {
	l.at(zapcore.ErrorLevel, fmt.Sprint(args...))
}

func (l *canalLogger) Panicf(f string, args ...any) {
	l.at(zapcore.ErrorLevel, fmt.Sprintf(f, args...))
}

func (l *canalLogger) Panicln(args ...any) {
	l.at(zapcore.ErrorLevel, sprintln(args...))
}
