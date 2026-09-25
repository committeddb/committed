package wal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

type observedSealedCompressor struct{ calls chan struct{} }

func (c observedSealedCompressor) CompressNextSealed() (bool, error) {
	select {
	case c.calls <- struct{}{}:
	default:
	}
	return false, nil
}

// Embedding supplies the EventLog methods; binding must discover the optional
// capability without depending on a concrete backend type.
type compressingEventLog struct {
	eventlog.EventLog
	observedSealedCompressor
}

func TestEventBindingCompressionCapability(t *testing.T) {
	for name, open := range productionEntryTestBackends() {
		t.Run(name, func(t *testing.T) {
			binding, err := open(t.TempDir())
			require.NoError(t, err)
			defer func() { _ = binding.Close() }()
			if name == "tidwall" {
				// The raw tidwall fixture omits the production compression wrapper.
				require.Nil(t, binding.compressor)
			} else {
				require.NotNil(t, binding.compressor)
			}
		})
	}
	log := &compressingEventLog{observedSealedCompressor: observedSealedCompressor{calls: make(chan struct{}, 1)}}
	binding := bindEventLog(log)
	require.Same(t, log, binding.compressor)
}

func TestSealerRechecksOptionalCapabilityAndReleasesLayout(t *testing.T) {
	s := &Storage{
		eventLayout: layoutLock{logger: zap.NewNop()},
		eventLog:    &eventLogBinding{}, // No native handle and no compressor.
		logger:      zap.NewNop(),
		sealerIdle:  time.Millisecond,
		sealerStop:  make(chan struct{}),
		sealerDone:  make(chan struct{}),
	}
	// Hold eventMu until the worker owns the layout and is waiting to inspect
	// the binding. This guarantees the no-capability branch runs first.
	func() {
		s.eventMu.Lock()
		defer s.eventMu.Unlock()
		go s.sealerWorker()
		t.Cleanup(s.stopSealer)
		require.Eventually(t, func() bool {
			if s.eventLayout.lock.TryLock() {
				s.eventLayout.lock.Unlock()
				return false
			}
			return true
		}, 5*time.Second, time.Millisecond)
	}()
	require.Eventually(t, func() bool {
		if !s.eventLayout.lock.TryLock() {
			return false
		}
		s.eventLayout.lock.Unlock()
		return true
	}, 5*time.Second, time.Millisecond, "missing compressor must release the layout")

	// Repeated layout freezes must remain usable while the backend has no
	// background compression. A missing release would prevent future steps.
	for range 3 {
		release := s.FreezeEventLayout()
		release()
	}
	calls := make(chan struct{}, 1)
	s.eventMu.Lock()
	s.eventLog = &eventLogBinding{compressor: observedSealedCompressor{calls: calls}}
	s.eventMu.Unlock()
	select {
	case <-calls:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not acquire replacement capability")
	}

	// Shutdown must work after returning to a backend with no compressor.
	s.eventMu.Lock()
	s.eventLog = &eventLogBinding{}
	s.eventMu.Unlock()
	s.stopSealer()
}
