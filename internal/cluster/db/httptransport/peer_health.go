package httptransport

import (
	"net"
	"net/url"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

// Self-report: every peer request carries the sender's advertised URL
// (senderURLHeader). A node whose address for that peer has stopped
// delivering switches to the reported one — that is how a node that moved
// rejoins without the operator touching the nodes that didn't move: its
// election attempts reach them (its own addresses for them still work), they
// learn its new address, and the leader's next heartbeat reaches it. The
// leader then records the address cluster-wide (db's agreed-address loop).
//
// A self-report never replaces an address that is still delivering. A node
// that advertises an address its peers can't reach — a NAT or container
// address that works today because peers use another — therefore keeps the
// working one.

const (
	// unreachableWarnAfter is how long a peer may go without a successful
	// delivery, while messages are being attempted, before the transport warns.
	unreachableWarnAfter = 30 * time.Second
	// duplicateReportWindow: two different addresses reported for one node id
	// within it mean two processes claim that id.
	duplicateReportWindow = 10 * time.Second
)

// peerHealth tracks deliveries to a peer's current address. Times are unix
// nanos; zero is "never".
type peerHealth struct {
	lastSuccess atomic.Int64
	lastFailure atomic.Int64
	changedAt   atomic.Int64
	lastWarn    atomic.Int64
	source      atomic.Pointer[string]
}

func (h *peerHealth) changed(source string) {
	h.changedAt.Store(time.Now().UnixNano())
	h.lastSuccess.Store(0)
	h.lastFailure.Store(0)
	h.setSource(source)
}

func (h *peerHealth) setSource(source string) { h.source.Store(&source) }

func (h *peerHealth) succeeded() { h.lastSuccess.Store(time.Now().UnixNano()) }

// failing reports whether the most recent delivery attempt to the current
// address failed.
func (h *peerHealth) failing() bool { return h.lastFailure.Load() > h.lastSuccess.Load() }

// noteFailure records a failed delivery and warns, at most every
// unreachableWarnAfter, when the peer has gone that long without a success.
func (t *HttpTransport) noteFailure(pr *peer, err error) {
	now := time.Now().UnixNano()
	pr.health.lastFailure.Store(now)
	since := pr.health.lastSuccess.Load()
	if since == 0 {
		since = pr.health.changedAt.Load()
	}
	if now-since < int64(unreachableWarnAfter) || now-pr.health.lastWarn.Load() < int64(unreachableWarnAfter) {
		return
	}
	pr.health.lastWarn.Store(now)
	src := ""
	if p := pr.health.source.Load(); p != nil {
		src = *p
	}
	t.logger.Warn("raft peer unreachable: no successful delivery",
		zap.Uint64("peer", pr.id), zap.String("address", pr.baseURL()), zap.String("source", src),
		zap.Duration("for", time.Duration(now-since).Round(time.Second)), zap.Error(err))
}

// PeerAddress implements db.Transport.
func (t *HttpTransport) PeerAddress(id uint64) (db.PeerAddressView, bool) {
	t.mu.RLock()
	pr, ok := t.peers[id]
	t.mu.RUnlock()
	if !ok {
		return db.PeerAddressView{}, false
	}
	v := db.PeerAddressView{URL: pr.baseURL()}
	if p := pr.health.source.Load(); p != nil {
		v.Source = *p
	}
	if s := pr.health.lastSuccess.Load(); s != 0 {
		v.LastSuccess = time.Unix(0, s)
		v.Delivered = true
	}
	return v, true
}

// selfReport is one peer's last reported address.
type selfReport struct {
	url string
	at  time.Time
}

// observeSelfReport handles the advertised URL a peer's request carried. It
// runs only after the request passed every identity check (cluster id, target
// node, token or mTLS) and came from a sender with a cluster id.
func (t *HttpTransport) observeSelfReport(from uint64, raw string) {
	reported, err := cluster.CanonicalPeerURL(raw)
	if err != nil || wildcardHost(reported) {
		return
	}
	if mv, ok := t.raft.(db.MembershipView); !ok || !mv.IsMember(from) {
		return // a non-member can't register itself, or redirect anyone
	}
	now := time.Now()
	if prev, loaded := t.reports.Swap(from, &selfReport{url: reported, at: now}); loaded {
		if p, _ := prev.(*selfReport); p != nil && p.url != reported && now.Sub(p.at) < duplicateReportWindow {
			t.logger.Error("two addresses report as the same node id — two processes may be running as one node",
				zap.Uint64("node", from), zap.String("address", p.url), zap.String("other address", reported))
		}
	}

	t.mu.RLock()
	pr, exists := t.peers[from]
	t.mu.RUnlock()
	switch {
	case !exists:
		_ = t.SetPeerAddress(from, reported, db.PeerAddressFromSelfReport)
	case pr.baseURL() == reported:
	case pr.health.failing():
		_ = t.SetPeerAddress(from, reported, db.PeerAddressFromSelfReport)
	}
}

// wildcardHost reports whether a URL names an address no peer can dial.
func wildcardHost(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return true
	}
	ip := net.ParseIP(u.Hostname())
	return ip != nil && ip.IsUnspecified()
}
