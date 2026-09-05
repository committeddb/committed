package http

import "context"

// viewStub is the hand-rolled clusterView for the in-package middleware
// units — the legs a live single-node engine cannot produce: a follower's
// proxy hop (id != leader), a failed read-index, an unready or stalled
// node. Zero value = a node that knows nothing.
type viewStub struct {
	id, leader, applied uint64
	stalled             bool
	linearizeErr        error
	linearizeCalls      int
	memberAPIURLCalls   int
	apiURLs             map[uint64]string
}

func (v *viewStub) ID() uint64           { return v.id }
func (v *viewStub) Leader() uint64       { return v.leader }
func (v *viewStub) AppliedIndex() uint64 { return v.applied }
func (v *viewStub) ApplyStalled() bool   { return v.stalled }
func (v *viewStub) LinearizableRead(context.Context) error {
	v.linearizeCalls++
	return v.linearizeErr
}

func (v *viewStub) MemberAPIURL(id uint64) (string, bool) {
	v.memberAPIURLCalls++
	u, ok := v.apiURLs[id]
	return u, ok
}
