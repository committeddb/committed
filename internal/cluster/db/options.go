package db

import "time"

// defaultTickInterval is the cadence at which Raft.Tick() is called when no
// override is supplied. It interacts with the (currently hard-coded)
// ElectionTick=10 and HeartbeatTick=1 settings: with a 100ms tick, leader
// election takes ~1s and heartbeats fire every 100ms.
const defaultTickInterval = 100 * time.Millisecond

// Option configures behaviour of New. Options exist primarily to let tests
// shorten timing-sensitive constants; production callers should pass none.
type Option func(*options)

type options struct {
	tickInterval time.Duration
}

func defaultOptions() options {
	return options{
		tickInterval: defaultTickInterval,
	}
}

// WithTickInterval overrides how often Raft.Tick() is called. Smaller
// intervals make leader election and heartbeats fire faster, which is useful
// in tests where waiting a full second for a single-node election to complete
// is unacceptable. Production callers should leave this at the default.
func WithTickInterval(d time.Duration) Option {
	return func(o *options) { o.tickInterval = d }
}
