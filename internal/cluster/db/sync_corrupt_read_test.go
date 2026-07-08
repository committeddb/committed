package db

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/committeddb/committed/internal/cluster"
)

// panicOnFatal makes logger.Fatal panic instead of calling os.Exit, so a test
// can assert whether a fatal fired.
type panicOnFatal struct{}

func (panicOnFatal) OnWrite(*zapcore.CheckedEntry, []zapcore.Field) {
	panic("logger.Fatal called")
}

// TestFatalOnCorruptRead is the mid-log regression: a corrupt (checksum-failed)
// committed event on a sync read must fatal-exit the node, not be swallowed.
// Reader.Read never advances past a corrupt entry, so the old warn-and-retry
// re-read the same seq forever; the log is untrustworthy and the node must be
// rebuilt from a healthy replica. A transient read error must NOT fatal.
func TestFatalOnCorruptRead(t *testing.T) {
	d := &DB{logger: zap.New(zapcore.NewNopCore(), zap.WithFatalHook(panicOnFatal{}))}

	corrupt := fmt.Errorf("event log read seq 5: %w", cluster.ErrCorruptEntry)
	require.Panics(t, func() { d.fatalOnCorruptRead("s1", corrupt) },
		"a corrupt read must fatal-exit, not be swallowed")

	require.NotPanics(t, func() { d.fatalOnCorruptRead("s1", errors.New("transient read glitch")) },
		"a transient read error must not fatal")
}
