package db

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// Duplicate node ids (see cluster.Incarnation). A node id belongs to one data
// directory for as long as it is a member, so two incarnations for one id are
// two processes running as one node — each can grant a vote in the same term,
// and the cluster can elect two leaders. Three guards:
//
//   - The cluster records each member's incarnation (first value wins); a node
//     whose recorded incarnation is another directory's stops, at start, after
//     a snapshot install, or when it applies its own refused announcement.
//   - A node refuses a raft message whose incarnation differs from the one
//     recorded for its sender, so the duplicate's votes are never counted.
//     Before anything is recorded (a new cluster, or one still upgrading), a
//     node that sees two incarnations for one id refuses both, and keeps
//     refusing that id: it can't tell which is the original.
//   - A node refused as a duplicate stops, naming the reason.
//
// A copy of a data directory carries its incarnation, so this cannot tell two
// copies of one directory apart; the runbooks never copy a live directory.

// featureLevelIncarnation gates the incarnation announcement: every member
// must understand the entity first.
const featureLevelIncarnation = featureLevelClusterIdentity

// IncarnationStorage is the storage side of incarnations. Storage that has
// none (the in-memory test doubles) runs without one.
type IncarnationStorage interface {
	LocalIncarnation() (cluster.Incarnation, error)
	MemberIncarnation(id uint64) (cluster.Incarnation, bool)
}

// IncarnationChecker is what the peer transport asks of the node about
// incarnations. A TransportRaft that doesn't implement it runs without one.
type IncarnationChecker interface {
	// Incarnation returns this node's incarnation, if it has one.
	Incarnation() (cluster.Incarnation, bool)
	// AdmitIncarnation checks a raft message's sender incarnation; an error
	// wraps ErrDuplicateNode and the message must be refused.
	AdmitIncarnation(from uint64, inc cluster.Incarnation) error
	// RefusedAsDuplicate stops this node: peer refused its messages because
	// another process is running as this node id.
	RefusedAsDuplicate(peer uint64, why string)
}

// ErrDuplicateNode is wrapped by AdmitIncarnation's refusals.
var ErrDuplicateNode = errors.New("two processes are running as one node id")

// incarnationRefresh bounds how often a sender with no recorded incarnation
// is looked up again.
const incarnationRefresh = time.Second

// senderIncarnation is what a node knows about one sender's incarnation.
type senderIncarnation struct {
	first, other cluster.Incarnation // other is set once a second one is seen
	recorded     cluster.Incarnation // set once the cluster has recorded one
	checkedAt    time.Time
}

// incarnations is the identity's incarnation state.
type incarnations struct {
	store  IncarnationStorage // nil: storage without incarnations
	self   cluster.Incarnation
	nodeID uint64

	mu      sync.Mutex
	senders map[uint64]*senderIncarnation
}

// initIncarnation reads (or mints) this node's incarnation at startup, and
// refuses to start a node the cluster recorded under another one.
func (n *Raft) initIncarnation() {
	store, ok := n.storage.(IncarnationStorage)
	if !ok {
		return
	}
	self, err := store.LocalIncarnation()
	if err != nil {
		n.logger.Error("incarnation", zap.Error(err))
		panic("db: " + err.Error())
	}
	n.ident.inc = incarnations{store: store, self: self, nodeID: n.id, senders: map[uint64]*senderIncarnation{}}
	if err := n.ident.checkOwnIncarnation(); err != nil {
		n.logger.Error("incarnation", zap.Error(err))
		panic("db: " + err.Error())
	}
}

// checkOwnIncarnation refuses a node whose recorded incarnation is another
// data directory's.
func (i *identity) checkOwnIncarnation() error {
	if i.inc.store == nil {
		return nil
	}
	if rec, ok := i.inc.store.MemberIncarnation(i.inc.nodeID); ok && rec != i.inc.self {
		return fmt.Errorf("another process is running as node %d: the cluster recorded incarnation %s for it, and this data directory is incarnation %s. Stop this process and give it its own node id", i.inc.nodeID, rec, i.inc.self)
	}
	return nil
}

// Incarnation implements IncarnationChecker.
func (i *identity) Incarnation() (cluster.Incarnation, bool) {
	return i.inc.self, !i.inc.self.IsZero()
}

// AdmitIncarnation implements IncarnationChecker.
func (i *identity) AdmitIncarnation(from uint64, inc cluster.Incarnation) error {
	if i.inc.store == nil {
		return nil
	}
	i.inc.mu.Lock()
	defer i.inc.mu.Unlock()
	s := i.inc.senders[from]
	if s == nil {
		s = &senderIncarnation{first: inc}
		i.inc.senders[from] = s
	}
	// Looked up again at most once per incarnationRefresh, and always before
	// a second incarnation is taken for a duplicate: the record may have
	// landed since, and then only the newcomer is refused.
	if s.recorded.IsZero() && (time.Since(s.checkedAt) >= incarnationRefresh || (inc != s.first && s.other.IsZero())) {
		s.checkedAt = time.Now()
		if rec, ok := i.inc.store.MemberIncarnation(from); ok {
			s.recorded = rec
		}
	}
	if !s.recorded.IsZero() {
		if inc != s.recorded {
			return fmt.Errorf("%w: node %d is recorded as incarnation %s, and this message came from incarnation %s", ErrDuplicateNode, from, s.recorded, inc)
		}
		return nil
	}
	if inc != s.first && s.other.IsZero() {
		s.other = inc
		i.logger.Warn("two processes are running as one node id: refusing both until the cluster records one",
			zap.Uint64("node", from), zap.String("incarnation", s.first.String()), zap.String("other incarnation", inc.String()))
	}
	if !s.other.IsZero() {
		return fmt.Errorf("%w: node %d has sent messages as incarnations %s and %s", ErrDuplicateNode, from, s.first, s.other)
	}
	return nil
}

// RefusedAsDuplicate implements IncarnationChecker.
func (i *identity) RefusedAsDuplicate(peer uint64, why string) {
	i.logger.Fatal("another process is running as this node id, so the cluster refuses this one's messages. Stop one of them: a node id belongs to one data directory, and a rebuilt node joins under a new id (docs/operations/rebuild.md)",
		zap.Uint64("node", i.inc.nodeID), zap.String("incarnation", i.inc.self.String()),
		zap.Uint64("refused by", peer), zap.String("reason", why))
}

// announceIncarnation records this node's incarnation in the cluster once
// every member understands it. Applying it on this node stops the node if
// another incarnation was recorded first (wal's handleNodeIncarnation). Exits
// once the record exists; not joined by Close, exits on db.ctx.
func (db *DB) announceIncarnation() {
	self, ok := db.raft.ident.Incarnation()
	if !ok {
		return
	}
	store := db.raft.ident.inc.store
	entity, err := cluster.NewNodeIncarnationEntity(db.ID(), self)
	if err != nil {
		db.logger.Error("incarnation announce: build entity", zap.Error(err))
		return
	}
	proposal := &cluster.Proposal{Entities: []*cluster.Entity{entity}}

	interval := db.announceInterval
	if interval <= 0 {
		interval = defaultTickInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if _, recorded := store.MemberIncarnation(db.ID()); recorded {
			return
		}
		if db.selfIsMember() && db.raft.Leader() != 0 && db.featureEnabled(featureLevelIncarnation) {
			if err := db.Propose(db.ctx, proposal); err == nil {
				db.logger.Info("recorded this node's incarnation",
					zap.Uint64("node", db.ID()), zap.String("incarnation", self.String()))
				return
			} else if db.ctx.Err() != nil {
				return
			}
		}
		select {
		case <-ticker.C:
		case <-db.ctx.Done():
			return
		}
	}
}
