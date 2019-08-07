package types

// Snapshotter is able to get and apply snapshots
type Snapshotter interface {
	GetSnapshot() ([]byte, error)
	ApplySnapshot([]byte) error
}
