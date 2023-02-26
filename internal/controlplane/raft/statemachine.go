package raft

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/lni/dragonboat/v4"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

type ExampleStateMachine struct {
	nh        *dragonboat.NodeHost
	ShardID   uint64
	ReplicaID uint64
	Count     uint64
}

func NewExampleStateMachine(nh *dragonboat.NodeHost) func(uint64, uint64) sm.IStateMachine {
	s := &ExampleStateMachine{nh: nh}
	return func(shardID uint64, replicaID uint64) sm.IStateMachine {
		s.ShardID = shardID
		s.ReplicaID = replicaID
		return s
	}
}

func (s *ExampleStateMachine) Update(e sm.Entry) (sm.Result, error) {
	// Can we store how many indexes we have printed and only print indexes we haven't printed?
	// Maybe store last printed index in the snapshot?
	// Does this sound like a strategy for a syncable? It should because that is where we are going.
	// This represents one shard (log). We can hook up an arbitrary number of syncables to a log
	// The snapshot should be a map[syncableID]index where index is the last written index
	// Should syncables work async so we don't block writes? Also so that failed syncs don't fail
	// the write?
	// If they are async we can use a mutex to protect the map and then let the map flush on
	// SaveSnapshot(). Each syncable should (must?) be idempotent so if an index get synced
	// twice there will be no ill consequences. Any side-effects or calculations should be
	// proposed to the raft so the syncable can remain idempotent
	s.Count++

	id, _, _, err := s.nh.GetLeaderID(s.ShardID)
	if err != nil {
		return sm.Result{}, err
	}
	leader := id == s.ReplicaID

	fmt.Printf("from ExampleStateMachine.Update(), msg: %s, count:%d, leader:%t\n",
		string(e.Cmd), s.Count, leader)
	return sm.Result{Value: uint64(len(e.Cmd))}, nil
}

func (s *ExampleStateMachine) Lookup(query interface{}) (interface{}, error) {
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, s.Count)
	return result, nil
}

func (s *ExampleStateMachine) SaveSnapshot(
	w io.Writer,
	fc sm.ISnapshotFileCollection,
	done <-chan struct{},
) error {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, s.Count)
	_, err := w.Write(data)
	return err
}

func (s *ExampleStateMachine) RecoverFromSnapshot(
	r io.Reader,
	files []sm.SnapshotFile,
	done <-chan struct{},
) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	v := binary.LittleEndian.Uint64(data)
	s.Count = v
	return nil
}

func (s *ExampleStateMachine) Close() error { return nil }
