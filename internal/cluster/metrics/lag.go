package metrics

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// LagProvider supplies the current per-config lag values for the observable lag
// gauges. The DB implements it. Both methods are read at metric collection
// (scrape) time, so they reflect the lag right now — which is exactly what makes
// a stalled syncable's lag visibly grow even though it has stopped
// checkpointing.
type LagProvider interface {
	// SyncableLags returns each configured syncable's raft-index lag
	// (head − checkpoint, clamped to ≥ 0), keyed by syncable id.
	SyncableLags() map[string]uint64
	// IngestableLags returns each configured ingestable's source lag, keyed by
	// ingestable id. An ingestable whose lag is unknown (mid-snapshot, or the
	// source was unreachable) has no entry and is reported as absent.
	IngestableLags() map[string]IngestLag
}

// IngestLag is one ingestable's source lag in the unit its dialect measures:
// bytes of source log (PostgreSQL, MySQL without GTIDs) or a count of source
// transactions (MySQL with GTIDs, SQL Server). Each unit has its own gauge, so
// an alert threshold never compares bytes with transactions.
type IngestLag struct {
	Value        uint64
	Transactions bool
}

// RegisterLagGauges registers three observable gauges that report sync and
// ingest lag, computed from p at collection time:
//
//   - committed.sync.lag{syncable_id}     — raft-index lag (head − checkpoint).
//   - committed.ingest.lag{ingestable_id} — source byte lag, for an ingestable
//     measured in bytes; absent when unknown.
//   - committed.ingest.lag.transactions{ingestable_id} — source transaction
//     lag, for an ingestable measured in transactions; absent when unknown.
//
// They are observable (async) rather than worker-pushed on purpose: a stuck
// syncable stops checkpointing, so a push-on-bump gauge would freeze at its last
// value and never trip a "falling behind" alert — while the real lag keeps
// growing as the log head advances. Reading p at each scrape makes the gauge
// track the true, current lag. The callbacks do only what
// GET /v1/{syncable,ingestable}/{id}/status does, at scrape cadence.
//
// meter must be non-nil; callers gate registration on metrics being enabled.
func RegisterLagGauges(meter metric.Meter, p LagProvider) error {
	if _, err := meter.Float64ObservableGauge("committed.sync.lag",
		metric.WithDescription("raft-index lag of a syncable behind the log head (head − checkpoint)"),
		metric.WithUnit("{index}"),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			for id, lag := range p.SyncableLags() {
				o.Observe(float64(lag), metric.WithAttributes(attribute.String("syncable_id", id)))
			}
			return nil
		}),
	); err != nil {
		return err
	}

	ingestGauge := func(name, description, unit string, transactions bool) error {
		_, err := meter.Float64ObservableGauge(name,
			metric.WithDescription(description),
			metric.WithUnit(unit),
			metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
				for id, lag := range p.IngestableLags() {
					if lag.Transactions == transactions {
						o.Observe(float64(lag.Value), metric.WithAttributes(attribute.String("ingestable_id", id)))
					}
				}
				return nil
			}),
		)
		return err
	}
	if err := ingestGauge("committed.ingest.lag",
		"bytes the source write head is ahead of what an ingestable has consumed", "By", false); err != nil {
		return err
	}
	return ingestGauge("committed.ingest.lag.transactions",
		"source transactions committed that an ingestable has not yet consumed", "{transaction}", true)
}
