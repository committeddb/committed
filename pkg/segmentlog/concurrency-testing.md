# Concurrency validation

The managed log serializes its operations with one mutex. A scan holds that lock
through its callbacks. Cancellation is cooperative: it cannot interrupt a blocked
callback, and waiting operations cannot proceed until the callback returns.

## Controlled reader lifetime tests

`TestScanLifetimeSerializesMutations` pauses a scan inside its first callback,
checks that the managed view remains locked, and starts a competing append,
rewrite, reclaim, or close. The competing operation must remain blocked until
the callback is released. The scan then completes, observes cancellation, or
returns a callback error. Tests check the delivered IDs and the mutation's result;
mutations other than close are also checked after reopening.

`TestEventLogProtectedReadConcurrentReleaseAndRewrite` exercises the application
adapter with both tidwall and segmented backends. It pauses decoding in a type
resolver, cancels the reader, and races multiple reader closes with a rewrite.
Protection remains active until the in-flight read exits. The canceled read must
return no Actual and leave its cursor unchanged. Closure must release protection
exactly once, and the rewrite must succeed either immediately after release or
on retry after a deferred result.

Channel gates establish the pause points. Lock probes check ownership at those
points; short observation windows check competing operations for premature
completion. Bounded completion waits report deadlocks rather than waiting forever.

## Mixed operations and shutdown

`TestConcurrentLogHistory` starts appends, scans, lookups, rewrites, and reclamation
together. Scans and lookups check ordering and payload identity. A final ordered
rewrite establishes a deterministic expected history, which is checked after
reclamation and reopening, together with the original append position.

`TestConcurrentClosePreservesAcknowledgedAppends` races two closes with reads and
appends. Operations may complete before closure or return `ErrClosed`. Reopening
must recover exactly the acknowledged append prefix and its append position.

These tests check selected interleavings and data invariants. They are not an
exhaustive scheduler exploration, a full linearizability proof, or a simulation of
process crashes. The race detector checks memory accesses on exercised paths;
the assertions also check logical behavior that the race detector cannot detect.

## Repeat under the race detector

```sh
go test -race ./pkg/segmentlog -run 'TestScanLifetimeSerializesMutations|TestConcurrentLogHistory|TestConcurrentClosePreservesAcknowledgedAppends' -count=20 -timeout=5m
go test -race ./internal/cluster/db/wal -run '^TestEventLogProtectedReadConcurrentReleaseAndRewrite$' -count=20 -timeout=5m
```
