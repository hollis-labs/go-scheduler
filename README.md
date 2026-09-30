# go-scheduler

`go-scheduler` is an application-neutral timed activation engine for Go. It
materializes due schedules into durable fires, claims each fire attempt with
compare-and-swap semantics, and dispatches opaque jobs to a `Runner`.

The library owns time, fire identity, generic retry state, and lifecycle
observation. Applications continue to own job types, workflow or loop/reflex
semantics, event names, activation policy, persistence schemas, and execution.

## Status

Pre-1.0. Version `v0.2.0` is a breaking `Store` migration from the
schedule-row-only v0.1 contract to durable schedule plus fire contracts. The
common constructor remains source-compatible as `New(store, runner)` and keeps
the v0.1 polling defaults. See [MIGRATION.md](MIGRATION.md) for the exact store
contract and downstream migration checklist, and pin `v0.2.0` in `go.mod`.

Documentation: [pkg.go.dev/github.com/hollis-labs/go-scheduler](https://pkg.go.dev/github.com/hollis-labs/go-scheduler).

## Install

```bash
go get github.com/hollis-labs/go-scheduler
```

## Usage

Implement `Store` over durable schedule and fire records, and implement
`Runner` over your queue or executor:

```go
engine := scheduler.New(store, runner)
engine.Start()
defer engine.Stop()
```

The call above uses the backward-compatible one-second cadence, due-batch
limit of 100, system clock, and no observer. Optional configuration is additive:

```go
engine := scheduler.New(
    store,
    runner,
    scheduler.WithClock(clock),
    scheduler.WithTickCadence(250*time.Millisecond),
    scheduler.WithDueBatchLimit(50),
    scheduler.WithClaimLease(10*time.Minute),
    scheduler.WithObserver(observer),
)
```

`TickNow(ctx)` runs one deterministic tick using the configured clock. A custom
clock implements `Now` and `NewTicker`; this makes both synchronous and
background-loop tests independent of wall time.

### Runnable example

This program needs `modernc.org/sqlite` (or any other SQLite driver) in your own
`go.mod`. It runs a one-time schedule through the reference store and prints the
dispatch:

```go
package main

import (
    "context"
    "database/sql"
    "fmt"
    "time"

    _ "modernc.org/sqlite"

    scheduler "github.com/hollis-labs/go-scheduler"
    "github.com/hollis-labs/go-scheduler/sqlstore"
)

type printRunner struct{}

func (printRunner) Enqueue(_ context.Context, job scheduler.Job) error {
    fmt.Println("dispatch", job.ScheduleID, job.Attempt)
    return nil
}

func main() {
    ctx := context.Background()
    db, _ := sql.Open("sqlite", "file:example.db?_pragma=busy_timeout(5000)")
    defer db.Close()
    _ = sqlstore.Migrate(ctx, db)
    store, _ := sqlstore.New(db)
    _ = store.CreateSchedule(ctx, scheduler.Schedule{
        ID: "hello", NextRun: time.Now().Add(-time.Second), Enabled: true, JobType: "print",
    })
    _ = scheduler.New(store, printRunner{}).TickNow(ctx)
}
```

## Stable Fire Identity

A schedule occurrence is identified by:

```go
fireID := scheduler.DeriveFireID(scheduleID, scheduledAt)
```

The derivation uses only the schedule ID and the scheduled fire time normalized
to UTC. Observed tick time and attempt number do not participate. Consequently:

- the same occurrence keeps one `Fire.ID` across ticks, processes, and retries;
- `Fire.ScheduledAt` records when the occurrence was due;
- `Fire.FiredAt` records when the current attempt was observed; and
- `Fire.Attempt` is one-based after a successful claim.

`Job.FireID` is the canonical dispatch identity. `Job.RunID` is deprecated but
is still populated as the exact same value for v0.1 consumers; it is not a
second identity. `Job.ScheduledAt`, `Job.FiredAt`, and `Job.Attempt` preserve the
same distinctions at the runner seam.

## Durable Store Contract

`Store` is intentionally record-shape-neutral. Its operations define required
atomic behavior without prescribing tables or fields:

- `ListDueSchedules` returns enabled schedules due for materialization.
- `CreateFire` atomically verifies the observed schedule `NextRun`, creates the
  derived fire if its ID has never existed, and advances the schedule. A
  terminal fire must never be recreated.
- `ListDueFires` returns persisted `pending` or `retrying` fires whose next
  attempt time is due, plus `claimed` fires whose claim lease expired.
- `ClaimFire` atomically compares status and attempt, increments the attempt,
  records observed `FiredAt` and `ClaimExpiresAt`, and changes the status to
  `claimed`. Recovering an expired claim preserves the attempt number.
- `TransitionFire` atomically compares claimed status, attempt, and `FiredAt`
  before recording `retrying`, `succeeded`, `skipped`, or `exhausted`.
- `DisableSchedule` disables a one-time schedule after its durable fire is
  materialized. The fire remains dispatchable.

Both materialization uniqueness and per-attempt claims are compare-and-swap
boundaries. Two engines may list the same due records, but only one can create a
given fire and only one can dispatch a given attempt.

## sqlstore: a reference SQLite Store

`github.com/hollis-labs/go-scheduler/sqlstore` implements `Store` over a small
schema of its own (`gosched_schedules`, `gosched_fires`). It is for new
adopters; applications with existing tables keep them and use `conformance`
below instead. You open the `*sql.DB` with any SQLite driver, apply the schema
with `Migrate` (or read the DDL from `Schema()`), and hand the store to the
engine:

```go
db, _ := sql.Open("sqlite", "file:sched.db?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
_ = sqlstore.Migrate(ctx, db)
store, _ := sqlstore.New(db)

_ = store.CreateSchedule(ctx, scheduler.Schedule{
    ID: "nightly", CronExpr: "0 3 * * *", NextRun: next, Enabled: true, JobType: "backup",
})
engine := scheduler.New(store, runner)
```

Each compare-and-swap is one SQL statement whose `WHERE` clause carries every
precondition, including `ExpectedFiredAt` on `ClaimFire` and `ClaimedAt` on
`TransitionFire`, so a stale owner loses inside the database rather than
depending on a lock held by the caller. `ClaimFire` uses `UPDATE ... RETURNING`
(SQLite 3.35 or newer). Timestamps are stored as fixed-width UTC text with
nanosecond precision.

## conformance: test any Store

`github.com/hollis-labs/go-scheduler/conformance` is a portable suite for the
`Store` contract, driven only through the `Store` interface, so it works
against your own schema and adapter:

```go
func TestMyStoreConforms(t *testing.T) {
    conformance.Run(t, func(t *testing.T) scheduler.Store {
        return newMyStoreOverTempDatabase(t) // must also implement conformance.Seeder
    })
}
```

The store must additionally implement `conformance.Seeder` (one method,
`CreateSchedule`), because `Store` itself never creates schedules. The suite
covers the `CreateFire` schedule-advance CAS, pending/retrying claims,
expired-claim recovery, the requirement that a stale `ExpectedFiredAt` is
rejected by the store, `ClaimedAt` fencing of a stale owner in
`TransitionFire`, `ListDueFires` selection, crash-before-complete recovery, and
racing claimers. Run it with `-race` and a repeat count.

## Restart Recovery

Every claim has a lease (`ClaimExpiresAt`). If a process exits after claiming a
fire but before persisting its result, a later engine lists that expired claim
as due and reclaims it using status, attempt, and prior `FiredAt` as CAS
preconditions. Reclaiming preserves `FireID` and `Attempt`; replacing `FiredAt`
fences a late transition from the old owner.

Recovery is at-least-once. A crash can happen after `Runner.Enqueue` accepts a
job but before the success transition commits, so runners should deduplicate on
`Job.FireID` and return an error wrapping `ErrDuplicateJob`. Set
`WithClaimLease` longer than the application's maximum expected `Enqueue`
latency. The default is five minutes.

Outcome transitions use a cancellation-detached context once `Enqueue`
returns, so cancellation of the tick cannot by itself strand a completed
attempt. Store calls must still implement their own finite database or network
timeouts.

## Retry And Exhaustion

Retry behavior is persisted with each `Fire`:

```go
retry := scheduler.RetryPolicy{
    MaxAttempts: 4, // includes the initial attempt
    Backoff: scheduler.BackoffPolicy{
        Strategy:     scheduler.BackoffExponential,
        InitialDelay: time.Second,
        MaxDelay:     30 * time.Second,
    },
}
```

Supported backoff strategies are `none`, `constant`, `linear`, and
`exponential`. `MaxAttempts == 0` is unbounded, preserving the v0.1 retry
default; configure a positive value when exhaustion is required. Once a fire is
`exhausted`, it is terminal and its stable ID prevents a later tick from
materializing the same occurrence again.

A normal enqueue error produces either `retrying` or `exhausted` according to
the persisted policy. A runner that recognizes an already-dispatched
`Job.FireID` returns an error wrapping `ErrDuplicateJob`; the engine records
that fire as terminal `skipped` without counting a worker error.

## Observation

An optional `Observer` receives application-neutral events for:

- `claim`
- `fire`
- `retry`
- `skip`
- `success`
- `exhaustion`
- `disable`
- `engine_error`

`ObserverEvent` supplies the current fire plus generic time, reason, operation,
retry time, and error context where relevant. It does not define application
event names or a persistence schema. Observer callbacks run synchronously, but
returned errors and panics are isolated: they increment
`Status.ObserverErrors` and do not alter claims, transitions, or dispatch
outcomes.

Ordering is deterministic for a claimed attempt: `claim`, `fire`, then either
`success`; `skip`; or `engine_error` followed by `retry`/`exhaustion`. A
one-time schedule's `disable` event occurs after materialization and before its
claim events. An expired-claim recovery emits another `claim`/`fire` pair for
the redelivery, with `Reason == "expired_claim_recovery"` on `claim`.

## API Overview

- `Engine` / `New(store, runner, ...Option)` — materializes and dispatches due
  fires. `Start` and `Stop` are idempotent; `TickNow` runs synchronously.
- `Schedule` — neutral cron/one-time descriptor with opaque job payload and
  `RetryPolicy`.
- `Fire` / `FireStatus` — durable identity, attempt, timing, retry, and terminal
  status contract.
- `FireCreation`, `FireClaim`, `FireTransition` — store CAS request contracts.
- `Job` / `Runner` — opaque dispatch seam.
- `Observer`, `ObserverFunc`, `ObserverEvent` — neutral lifecycle hooks.
- `Clock`, `Ticker`, and engine options — deterministic time, polling, batches,
  and claim-lease recovery.
- `Status` — dispatch, retry, skip, exhaustion, worker-error, and observer-error
  counters.
- `ValidateCron`, `NextRun`, and `DeriveFireID` — standalone helpers.

## v0.1 Store Migration

`New(store, runner)` still compiles after the store implements the new
interface. Existing v0.1 stores must replace:

- `ClaimAndUpdateScheduleRun`
- `SetScheduleNextRun`

with durable `CreateFire`, `ListDueFires`, `ClaimFire`, and `TransitionFire`
operations. `ListDueSchedules` and `DisableSchedule` remain. Applications must
persist fires so retries and exhaustion survive ticks and process restarts.

Existing runners may continue reading `Job.RunID` during migration, but should
switch deduplication to `Job.FireID`. Unlike v0.1, `ErrDuplicateJob` terminates
the fire as `skipped` instead of requeuing it indefinitely.

The remote `v0.1.1` tag contains an earlier draft of the breaking durable-fire
contract. It remains immutable but was never promoted as the supported GitHub
Release. Use `v0.2.0`, which adds restart-safe leased claims and stale-owner
fencing. See [MIGRATION.md](MIGRATION.md).

## Dependencies

The only direct external dependency of the engine is
[`github.com/robfig/cron/v3`](https://pkg.go.dev/github.com/robfig/cron/v3) for
standard cron parsing. `sqlstore` and `conformance` import only the standard
library and the root package; no SQL driver is linked into your build unless you
choose one. `modernc.org/sqlite` appears in `go.mod` for this module's own
tests only. The module imports no application packages.

## Testing

```bash
go test ./...
go test -race ./...
```

The engine tests use in-memory contract fakes and include a concurrent
two-engine CAS test proving that one fire attempt cannot dispatch twice. The
`sqlstore` tests run the `conformance` suite against a temporary SQLite database
and race two engines over one database file.

## Compatibility

This module is pre-1.0: minor releases may break the exported API, as v0.2.0
did for `Store`. Pin an exact version and read [CHANGELOG.md](CHANGELOG.md)
before upgrading. Additions such as `sqlstore` and `conformance` do not change
the engine or `Store` contract. It needs Go 1.26.6 or newer (the `go` line of
`go.mod`).

## Out of scope

- Cron parsing, next-run computation, retry and backoff, and lease duration.
  The engine owns these; `sqlstore` and `conformance` do not add policy.
- A required schema. Applications keep their own tables; `sqlstore` is a
  reference and `conformance` verifies any implementation.
- Job execution, queues and application event names. Those belong to your
  `Runner`.
- Databases other than SQLite in `sqlstore`.

## Development

```bash
gofmt -l .
go vet ./...
go test -race -count=1 ./...
```

CI (`.github/workflows/check.yml`) is the full gate.

## License

[MIT](LICENSE) © Hollis Labs.
