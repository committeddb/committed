package types

// Leader responds with whether this node is a leader
//counterfeiter:generate . Leader
type Leader interface {
	IsLeader() bool
}