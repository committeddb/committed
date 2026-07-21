package cluster

import (
	"encoding/binary"
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

// ErrReplayMigrationFailed wraps the error a migration retry's re-run
// returned: the migration chain still fails on the dead-lettered entity, so
// the record is left in place. The HTTP layer maps this to 502 and surfaces
// the cause.
var ErrReplayMigrationFailed = errors.New("cluster: migration retry failed")

// EntityKindSnapshot — the type-keyed twin of syncableDeadLetterType: keyed by
// its full identity (type id + raft index — typeMigrationDeadLetterKey) in both
// the upsert and the clearing delete, so the scrubber compacts it keep-latest-
// per-key like any other Snapshot built-in (see syncableDeadLetterType).
var typeMigrationDeadLetterType = registerSystemType(&Type{
	ID:         "9e9a9e5f-22f6-4963-ae77-a4a87d807496",
	Name:       "InternalTypeMigrationDeadLetter",
	Version:    1,
	EntityKind: EntityKindSnapshot,
}, AdmissionCoordination)

// TypeMigrationDeadLetter records that a type-migration program (jq) failed
// at runtime while upgrading the entity at raft Index. It is the type-keyed
// twin of SyncableDeadLetter: the syncable record says which syncable
// skipped the proposal, this one says which type's migration broke it — so
// an operator can enumerate the failures per type and retry them after
// fixing the program. Proposed by the sync worker alongside the syncable
// dead letter and applied deterministically on every node, so the record is
// durable and queryable cluster-wide.
//
// TimestampUnixNano and Message are stamped once by the proposer so apply
// writes identical bytes on every replica. Message is truncated by the
// proposer (see db.maxDeadLetterMessageBytes) to bound log growth.
type TypeMigrationDeadLetter struct {
	TypeID            string
	Index             uint64
	TimestampUnixNano int64
	// FromVersion -> ToVersion identify the failing chain step: the
	// ToVersion migration program is the one that errored.
	FromVersion int
	ToVersion   int
	Message     string
}

func (d *TypeMigrationDeadLetter) Marshal() ([]byte, error) {
	ld := &clusterpb.LogTypeMigrationDeadLetter{
		TypeID:            d.TypeID,
		Index:             d.Index,
		TimestampUnixNano: d.TimestampUnixNano,
		// Versions are bounded by the domain: monotonically assigned
		// starting at 1, they will never exceed int32.
		FromVersion: int32(d.FromVersion), //nolint:gosec // G115: bounded by domain
		ToVersion:   int32(d.ToVersion),   //nolint:gosec // G115: bounded by domain
		Message:     d.Message,
	}
	return proto.Marshal(ld)
}

func (d *TypeMigrationDeadLetter) Unmarshal(bs []byte) error {
	ld := &clusterpb.LogTypeMigrationDeadLetter{}
	if err := proto.Unmarshal(bs, ld); err != nil {
		return err
	}

	d.TypeID = ld.TypeID
	d.Index = ld.Index
	d.TimestampUnixNano = ld.TimestampUnixNano
	d.FromVersion = int(ld.FromVersion)
	d.ToVersion = int(ld.ToVersion)
	d.Message = ld.Message

	return nil
}

func IsTypeMigrationDeadLetter(id string) bool {
	return id == typeMigrationDeadLetterType.ID
}

// typeMigrationDeadLetterKey encodes a migration dead letter's full identity —
// type id followed by the 8-byte big-endian raft index — and is the event-log
// entity Key for BOTH the upsert and the clearing delete, so each record is a
// single symmetric, unique key (what lets the scrubber compact them keep-latest-
// per-key). DecodeTypeMigrationDeadLetterKey reverses it.
func typeMigrationDeadLetterKey(typeID string, index uint64) []byte {
	key := make([]byte, len(typeID)+8)
	copy(key, typeID)
	binary.BigEndian.PutUint64(key[len(typeID):], index)
	return key
}

// NewUpsertTypeMigrationDeadLetterEntity wraps a TypeMigrationDeadLetter as an
// upsert entity keyed by its full identity (type id + raft index —
// typeMigrationDeadLetterKey), matching the clearing delete's key. The apply
// handler still derives the bbolt key from the unmarshaled record body, so
// storage is unchanged.
func NewUpsertTypeMigrationDeadLetterEntity(d *TypeMigrationDeadLetter) (*Entity, error) {
	bs, err := d.Marshal()
	if err != nil {
		return nil, err
	}

	return NewUpsertEntity(typeMigrationDeadLetterType, typeMigrationDeadLetterKey(d.TypeID, d.Index), bs), nil
}

// NewDeleteTypeMigrationDeadLetterEntity clears the migration dead-letter record
// at a specific raft index (used by migration retry after the fixed program
// succeeds). It keys by the same id+index identity as the upsert (a delete
// carries the delete sentinel in the body, so the index must ride in the Key);
// DecodeTypeMigrationDeadLetterKey reverses it on the apply side.
func NewDeleteTypeMigrationDeadLetterEntity(typeID string, index uint64) *Entity {
	return NewDeleteEntity(typeMigrationDeadLetterType, typeMigrationDeadLetterKey(typeID, index))
}

// DecodeTypeMigrationDeadLetterKey reverses
// NewDeleteTypeMigrationDeadLetterEntity's composite Key into
// (typeID, index). ok is false if the key is too short to carry an
// 8-byte index.
func DecodeTypeMigrationDeadLetterKey(key []byte) (typeID string, index uint64, ok bool) {
	if len(key) < 8 {
		return "", 0, false
	}
	return string(key[:len(key)-8]), binary.BigEndian.Uint64(key[len(key)-8:]), true
}
