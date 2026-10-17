package httptransport

import (
	"errors"
	"net/http"
	"strconv"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

// Peer identity on the wire (see cluster.ClusterID and db/cluster_identity.go).
//
// Every peer request carries:
//   - clusterIDHeader: the constant "committed-raft" while the sender has no
//     cluster id — what older releases require — and the real id once it has
//     one, so an older node refuses it (412) rather than talk to a node of
//     another cluster;
//   - clusterUUIDHeader: the sender's cluster id, when it has one;
//   - targetNodeHeader: the node id the request is for, checked before the
//     token and before the body is read, so a request that reached the wrong
//     node (a reused address) is refused at once — a misaddressed snapshot is
//     never read.
const (
	clusterUUIDHeader = "X-Committed-Cluster-Uuid"
	targetNodeHeader  = "X-Committed-Target-Node"
	// senderURLHeader is the sender's advertised peer URL (self-report).
	senderURLHeader = "X-Committed-Sender-Url"
)

// identity is the node's cluster identity, nil when the node runs without one.
func (t *HttpTransport) identity() db.ClusterIdentifier {
	ci, _ := t.raft.(db.ClusterIdentifier)
	return ci
}

// setPeerHeaders stamps a peer request with protocol and identity headers.
func (t *HttpTransport) setPeerHeaders(h http.Header, to uint64) {
	h.Set(protocolHeader, protocolVersion)
	h.Set(clusterIDHeader, clusterID)
	if ci := t.identity(); ci != nil {
		if id, ok := ci.ClusterID(); ok {
			h.Set(clusterIDHeader, id.String())
			h.Set(clusterUUIDHeader, id.String())
		}
	}
	if to != 0 {
		h.Set(targetNodeHeader, strconv.FormatUint(to, 10))
	}
	if t.selfURL != "" && !wildcardHost(t.selfURL) {
		h.Set(senderURLHeader, t.selfURL)
	}
	if t.token != "" {
		h.Set("Authorization", "Bearer "+t.token)
	}
}

// admitPeer runs the identity checks shared by every peer endpoint, in order:
// protocol, cluster id, target node. It writes the refusal and returns false
// when the request must not proceed; the caller then checks the token and
// reads the body. adopt is true only for raft messages: a joiner with no id
// takes the one the first raft message addressed to it carries.
func (t *HttpTransport) admitPeer(w http.ResponseWriter, r *http.Request, adopt bool) bool {
	if r.Header.Get(protocolHeader) != protocolVersion {
		http.Error(w, "wrong protocol", http.StatusPreconditionFailed)
		return false
	}
	legacy := r.Header.Get(clusterIDHeader)
	sent := r.Header.Get(clusterUUIDHeader)
	ci := t.identity()
	var mine cluster.ClusterID
	have := false
	if ci != nil {
		mine, have = ci.ClusterID()
	}

	switch {
	case have:
		if sent == "" {
			t.refuseCluster(w, r, mine, "the peer sent no cluster id (an older release, or a node that has not joined this cluster)")
			return false
		}
		if sent != mine.String() || (legacy != clusterID && legacy != mine.String()) {
			t.refuseCluster(w, r, mine, "the peer belongs to cluster "+sent)
			return false
		}
	case sent != "":
		theirs, err := cluster.ParseClusterID(sent)
		if err != nil || (legacy != clusterID && legacy != sent) {
			http.Error(w, "malformed cluster id", http.StatusBadRequest)
			return false
		}
		if adopt && ci != nil {
			if err := ci.AdoptClusterID(theirs); err != nil && !errors.Is(err, db.ErrNotAdoptable) {
				why, _ := cluster.RedactedMessage(err)
				t.refuseCluster(w, r, mine, why)
				return false
			}
		}
	default:
		if legacy != clusterID {
			http.Error(w, "wrong cluster", http.StatusPreconditionFailed)
			return false
		}
	}

	if target := r.Header.Get(targetNodeHeader); target != "" {
		to, err := strconv.ParseUint(target, 10, 64)
		if err != nil {
			http.Error(w, "malformed target node", http.StatusBadRequest)
			return false
		}
		if to != t.id {
			t.logger.Warn("refused a peer request addressed to another node (an address now served by a different member)",
				zap.Uint64("this node", t.id), zap.Uint64("addressed to", to), zap.String("from", r.RemoteAddr))
			http.Error(w, "request addressed to node "+target+", this is node "+strconv.FormatUint(t.id, 10), http.StatusMisdirectedRequest)
			return false
		}
	}
	return true
}

func (t *HttpTransport) refuseCluster(w http.ResponseWriter, r *http.Request, mine cluster.ClusterID, why string) {
	t.logger.Warn("refused a peer request from another cluster",
		zap.String("this cluster", mine.String()), zap.String("reason", why),
		zap.String("from", r.RemoteAddr), zap.String("path", r.URL.Path))
	http.Error(w, "wrong cluster: this node belongs to cluster "+mine.String()+"; "+why, http.StatusPreconditionFailed)
}
