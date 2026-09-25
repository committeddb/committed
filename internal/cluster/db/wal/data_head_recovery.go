package wal

import (
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// recoverDataHead is the bounded fallback for directories without a
// persisted data head. Classification and failure reporting are application
// policy; reverse physical traversal belongs to the selected backend.
func (s *Storage) recoverDataHead() {
	if s.dataEventIndex.Load() != 0 {
		return
	}
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	const dataHeadBackscanCap = 4096
	scanned, err := s.eventLog.entries.ScanReverse(dataHeadBackscanCap, func(entry *pb.Entry) (bool, error) {
		if entry.GetType() != pb.EntryNormal || len(entry.Data) == 0 {
			return true, nil
		}
		typeID, ok, err := cluster.FirstEntityTypeID(entry.Data)
		if err != nil {
			return false, err
		}
		if ok && !cluster.IsInternal(typeID) {
			s.dataEventIndex.Store(entry.GetIndex())
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		s.logger.Warn("dataEventIndex backscan: read or decode failed; leaving head at 0",
			zap.Int("scanned", scanned), zap.Error(err))
	}
	if s.dataEventIndex.Load() == 0 && scanned >= dataHeadBackscanCap {
		s.logger.Warn("dataEventIndex backscan hit its cap without finding a data entry; the data head is 0 until the next data entry applies — syncable lag/caughtUp UNDER-REPORT until then (a fresh syncable may briefly read caughtUp over an empty replay range)",
			zap.Int("scanned", scanned))
	}
}
