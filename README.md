# go-scheduler

`go-scheduler` is an application-neutral timed activation engine for Go. It
materializes due schedules into durable fires, claims each fire attempt with
compare-and-swap semantics, and dispatches opaque jobs to a `Runner`.

The library owns time, fire identity, generic retry state, and lifecycle
observation. Applications continue to own job types, workflow or loop/reflex
semantics, event names, activation policy, persistence schemas, and execution.

## Status

Pre-1.0. The current branch introduces a breaking `Store` migration from the
schedule-row-only v0.1 contract to durable schedule plus fire contracts. The
common constructor remains source-compatible as `New(store, runner)` and keeps
the v0.1 defaults. See [CHANGELOG.md](CHANGELOG.md) for the migration checklist
and pin a version in `go.mod`.

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
    scheduler.WithObserver(observer),
)
```

`TickNow(ctx)` runs one deterministic tick using the configured clock. A custom
clock implements `Now` and `NewTicker`; this makes both synchronous and
background-loop tests independent of wall time.

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
  attempt time is due.
- `ClaimFire` atomically compares status and attempt, increments the attempt,
  records observed `FiredAt`, and changes the status to `claimed`.
- `TransitionFire` atomically compares the claimed status and attempt before
  recording `retrying`, `succeeded`, `skipped`, or `exhausted`.
- `DisableSchedule` disables a one-time schedule after its durable fire is
  materialized. The fire remains dispatchable.

Both materialization uniqueness and per-attempt claims are compare-and-swap
boundaries. Two engines may list the same due records, but only one can create a
given fire and only one can dispatch a given attempt.

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
- `Clock`, `Ticker`, and engine options — deterministic time and polling.
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

## Dependencies

The only direct external dependency is
[`github.com/robfig/cron/v3`](https://pkg.go.dev/github.com/robfig/cron/v3) for
standard cron parsing. The module imports no application packages.

## Testing

```bash
go test ./...
go test -race ./...
```

The test suite uses in-memory contract fakes and includes a concurrent
two-engine CAS test proving that one fire attempt cannot dispatch twice.

## License

[MIT](LICENSE) © Hollis Labs.
