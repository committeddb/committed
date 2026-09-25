package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"strings"

	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// The metadata handle is the stopped-node lock already held by RepairNode.
// Applied entries will not replay; a persisted snapshot may also have cut away
// Raft entries before its metadata installation finished. Retain both bounds.
func offlineEventRepairBound(base string, metadata *bolt.DB) (uint64, error) {
	if metadata == nil {
		return 0, fmt.Errorf("application metadata is missing")
	}
	var required uint64
	err := metadata.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(appliedIndexBucket)
		if bucket == nil {
			return ErrBucketMissing
		}
		value := bucket.Get(appliedIndexKey)
		if value == nil {
			return nil
		}
		if len(value) != 8 {
			return fmt.Errorf("invalid applied index length %d", len(value))
		}
		required = binary.BigEndian.Uint64(value)
		return nil
	})
	if err != nil {
		return 0, err
	}
	dir := datadir.StateLogDir(base)
	diagnosis, err := DiagnoseLog(dir)
	if err != nil {
		return 0, err
	}
	if diagnosis.Status != LogClean {
		return 0, fmt.Errorf("raft state log is %s", diagnosis.Status)
	}
	segments, _, err := listSegments(dir)
	if err != nil {
		return 0, err
	}
	if len(segments) == 0 {
		return 0, fmt.Errorf("raft state log is missing")
	}
	for _, segment := range segments {
		_, err := inspectStateSegment(segment, func(state State) error {
			if state.Type != Snapshot {
				return nil
			}
			snapshot := new(pb.Snapshot)
			if err := proto.Unmarshal(state.Data, snapshot); err != nil {
				return err
			}
			required = max(required, snapshot.GetMetadata().GetIndex())
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	return required, nil
}

func inspectStateSegment(segment segFile, visit func(State) error) (int, error) {
	data, err := os.ReadFile(segment.path)
	if err != nil {
		return 0, err
	}
	if strings.HasSuffix(segment.name, ".zst") {
		data, err = decodeZstd(data)
		if err != nil {
			return 0, err
		}
	}
	var invalid error
	count, incomplete := walkSegmentRecords(data, func(_, _, _ int, raw []byte) bool {
		payload, err := unframe(raw)
		if err != nil {
			invalid = err
			return false
		}
		var state State
		if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&state); err != nil {
			invalid = err
			return false
		}
		if state.Type != Snapshot && state.Type != HardState {
			invalid = fmt.Errorf("unknown Raft state record type")
			return false
		}
		invalid = visit(state)
		return invalid == nil
	})
	if invalid != nil {
		return count, invalid
	}
	if incomplete >= 0 {
		return count, fmt.Errorf("incomplete Raft state record")
	}
	return count, nil
}

func repairSegmentedNodeTail(base, dir string, metadata *bolt.DB, d *Diagnosis, commit bool) error {
	required, err := offlineEventRepairBound(base, metadata)
	if err != nil {
		message, _ := cluster.RedactedMessage(err)
		d.Detail = "segmented tail left unchanged: cannot establish durable event boundary: " + message
		return nil
	}
	_, err = segmentlog.RepairTail(context.Background(), dir, required, false)
	if err != nil {
		message, _ := cluster.RedactedMessage(err)
		d.Detail = "segmented tail repair failed: " + message
		return nil
	}
	if commit {
		if _, err := segmentlog.RepairTail(context.Background(), dir, required, true); err != nil {
			return err
		}
	}
	d.Status = LogTornTail
	d.Repaired = commit
	d.Detail = fmt.Sprintf("incomplete segmented tail group; verified prefix retains applied and snapshot progress through index %d", required)
	return nil
}
