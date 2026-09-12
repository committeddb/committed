package db

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// The JSON shape census (see cluster.TopicCensus): the ingest worker folds
// every SNAPSHOT-phase row's payload shape into a per-topic census and
// publishes it as replicated state, so any node's status endpoint can serve
// it and the type-drafting flow has its bootstrap input. Snapshot rows are
// exactly the SourceSeq==0 proposals — CDC proposals carry a positive source
// sequence — and they carry the refresh epoch as their Generation, which is
// what lets a resumed worker seed-and-continue at the same epoch while a
// fresh full snapshot (higher epoch) resets instead of double-counting.
// Default-on; `census = false` in the [ingestable] envelope opts out, and
// `censusValues = true` opts into bounded distinct-value tracking for enum
// drafting (values enter replicated state only on that opt-in — the PII
// posture). Dialect-agnostic by construction: the fold runs in the worker,
// on rendered payloads, for every SQL engine identically.

// censusPublishInterval throttles mid-snapshot census publishes: at most one
// consensus write per interval, plus a forced publish when the refresh
// boundary closes the pass.
const censusPublishInterval = 5 * time.Second

// censusRecorder is one worker's census state. A nil recorder (census
// disabled, or no config found) is a valid receiver: every method no-ops.
type censusRecorder struct {
	db     *DB
	id     string
	opts   cluster.CensusOptions
	epoch  uint64
	topics map[string]*cluster.TopicCensus

	dirty       bool
	lastPublish time.Time
}

// newCensusRecorder builds the worker's recorder: parses the census options
// from the ingestable's stored configuration and seeds the accumulator from
// the last published census so a mid-snapshot resume continues rather than
// restarting its counts. Returns nil when the census is disabled.
func (db *DB) newCensusRecorder(id string) *censusRecorder {
	opts, ok := db.censusOptionsFor(id)
	if !ok || opts.Disabled {
		return nil
	}
	c := &censusRecorder{
		db: db, id: id, opts: opts,
		topics: map[string]*cluster.TopicCensus{},
		// Seed the throttle so the FIRST publish also waits an interval: a
		// small snapshot then publishes exactly once, at its refresh
		// boundary, instead of spending a consensus write on its first row.
		lastPublish: time.Now(),
	}
	if prev, ok := db.storage.IngestableCensus(id); ok {
		c.epoch = prev.RefreshEpoch
		if prev.Topics != nil {
			c.topics = prev.Topics
		}
	}
	return c
}

// censusOptionsFor reads the [ingestable] envelope's census keys from the
// stored configuration. ok is false when no configuration exists for the id
// (a raced delete, or a test driving db.Ingest directly without seeding one).
func (db *DB) censusOptionsFor(id string) (cluster.CensusOptions, bool) {
	cfgs, err := db.storage.Ingestables()
	if err != nil {
		return cluster.CensusOptions{}, false
	}
	for _, cfg := range cfgs {
		if cfg.ID != id {
			continue
		}
		v, err := cluster.ParseConfigBytes(cfg.MimeType, cfg.Data)
		if err != nil {
			db.logger.Warn("census: cannot parse ingestable config; census disabled for this worker",
				zap.String("id", id), zap.Error(err))
			return cluster.CensusOptions{Disabled: true}, true
		}
		return cluster.ParseCensusOptions(v), true
	}
	return cluster.CensusOptions{}, false
}

// observe folds one outgoing proposal into the census. Snapshot rows
// (SourceSeq 0) fold; CDC proposals are ignored (the census is tied to the
// snapshot pass — the tripwire covers post-census drift); a refresh-boundary
// marker forces a publish (the pass just closed). Never disturbs ingest: any
// failure is logged and the row skipped.
func (c *censusRecorder) observe(ctx context.Context, p *cluster.Proposal) {
	if c == nil {
		return
	}
	if containsRefreshBoundary(p) {
		c.maybePublish(ctx, true)
		return
	}
	if p.SourceSeq != 0 {
		// A CDC proposal: nothing to fold (the census is tied to the
		// snapshot pass), but flush any census still pending from a pass
		// that ended without a refresh marker (a partial backfill).
		c.maybePublish(ctx, false)
		return
	}
	for _, e := range p.Entities {
		if e.Variant() != cluster.EntityVariantRow || len(e.Data) == 0 || e.Type == nil {
			continue
		}
		// A higher generation is a fresh full snapshot re-observing every
		// row: reset rather than double-count. (Entities of one proposal
		// share their epoch; pre-epoch entities carry 0 and fold together.)
		if e.Generation > c.epoch {
			c.epoch = e.Generation
			c.topics = map[string]*cluster.TopicCensus{}
		}
		tc := c.topics[e.Type.ID]
		if tc == nil {
			tc = &cluster.TopicCensus{}
			c.topics[e.Type.ID] = tc
		}
		if err := tc.Fold(e.Data, c.opts); err != nil {
			c.db.logger.Warn("census: skipping unfoldable payload",
				zap.String("id", c.id), zap.String("topic", e.Type.ID), zap.Error(err))
			continue
		}
		c.dirty = true
	}
	c.maybePublish(ctx, false)
}

// maybePublish proposes the census record — throttled, or immediately when
// forced. A propose failure keeps the census dirty and is retried on the
// next observation; it never affects the data path.
func (c *censusRecorder) maybePublish(ctx context.Context, force bool) {
	if c == nil || !c.dirty {
		return
	}
	if !force && time.Since(c.lastPublish) < censusPublishInterval {
		return
	}
	e, err := cluster.NewIngestableCensusEntity(&cluster.IngestableCensus{
		ID: c.id, RefreshEpoch: c.epoch, Topics: c.topics,
	})
	if err != nil {
		c.db.logger.Warn("census: build record failed", zap.String("id", c.id), zap.Error(err))
		return
	}
	if err := c.db.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{e}}); err != nil {
		c.db.logger.Warn("census: publish failed; will retry on the next observation",
			zap.String("id", c.id), zap.Error(err))
		return
	}
	c.dirty = false
	c.lastPublish = time.Now()
}
